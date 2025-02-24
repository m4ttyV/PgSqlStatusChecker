using System.Diagnostics;
using System.IO;
using System.Text;
using Npgsql;
using Npgsql.Internal;

namespace ConsoleApp2
{
    internal class Program
    {
        public class NpgsqlDbHelper
        {
            private readonly string _connectionString;
            public NpgsqlDbHelper(string connectionString)
            {
                _connectionString = connectionString;
            }
            public NpgsqlConnection GetConnection()
            {
                NpgsqlConnection conn = new NpgsqlConnection(_connectionString);
                conn.Open();
                return conn;
            }
            public string GetColumnValues(NpgsqlConnection conn, string tableName, string columnName)
            {
                string values = "0";
                string query = $"SELECT MAX({columnName}::TIMESTAMP)  FROM {tableName}";
                using (NpgsqlCommand cmd = new NpgsqlCommand(query, conn))
                {
                    using (NpgsqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            values = reader[0]?.ToString();
                        }
                    }
                }
                return values;
            }
        }
        
        static void Main(string[] args)
        {
            string Hostname = "";
            string Port = "";
            string Username = "";
            string Password = "";
            string status = DateTime.UtcNow.ToString();
            try
            {
                Directory.CreateDirectory("./Output");
            }
            catch { }
            try {
                Directory.CreateDirectory("./Logs");
            }
            catch { }
            try
            {
                using (StreamReader reader = new StreamReader("./database_conf.cfg"))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        if (string.IsNullOrEmpty(line))
                            continue;
                        if (line.StartsWith('#'))
                            continue;
                        Hostname = line.Split(';')[0];
                        Port = line.Split(';')[1];
                        Username = line.Split(';')[2];
                        Password = line.Split(';')[3];
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"В ходе получения данных для подключения к базе данных возникла ошибка: {ex.Message}\n " +
                    $"Данные должны храниться в файле \"database_conf.cfg\" рядом с исполняемым файлом в формате \"hostname;port;username;password\"");
                Console.ReadKey();
            }
            string connectionString = $"Host={Hostname};Port={Port};Username={Username};Password={Password}";
            List<string> Output = new List<string>();
            List<string> Logs = new List<string>();
            NpgsqlDbHelper db = new NpgsqlDbHelper(connectionString);

            using (var conn = new NpgsqlConnection(connectionString))
            {
                conn.Open();
                using (var cmd =  new NpgsqlCommand("SELECT version();", conn))
                {
                    var version = cmd.ExecuteScalar();
                    Console.WriteLine($"Подключение к базе данных: Успех!\n" +
                        $" Версия PostgreSQL:{version}\n" +
                        $"Получаем данные...");
                }
                conn.Close();
            }

            string def_column = "m4400";
            string tableListFilePath = "./table_names.txt";
            List<string> tables = new List<string>();
            using (StreamReader reader = new StreamReader(tableListFilePath))
            {
                string line;
                Console.WriteLine("Данные будут извлечены из слудующих таблиц:");
                while ((line = reader.ReadLine()) != null)
                {
                    string columnName = "";
                    string tableName = "";
                    line = line.Trim();
                    if (string.IsNullOrEmpty(line))
                        continue;
                    string[] words = line.Split(';', StringSplitOptions.RemoveEmptyEntries);
                    if (words.Length > 0)
                    {
                        if (words.Length > 1)
                            columnName = words[1];
                        else
                            columnName = def_column;
                        tableName = words[0];
                        tables.Add($"{tableName};{columnName}");
                        Console.WriteLine(tableName);
                    }
                }
            }
            for (int i = 0; i < tables.Count; i++)
            {
                string tableName = tables[i].Split(';')[0];
                string columnName = tables[i].Split(';')[1];
                string outputRow = "";
                string logRow = "";
                //пишем логи
                try
                {
                    string dateRow = db.GetColumnValues(db.GetConnection(), tableName, columnName);
                    logRow = $"info;{DateTime.UtcNow.ToString()};{tableName};{dateRow}";
                    outputRow = dateRow;
                }
                catch (Exception ex)
                {
                    logRow = $"Error;{DateTime.UtcNow.ToString()};{tableName};{ex.Message}";
                    outputRow = ex.Message;
                    Console.ReadKey();
                }
                Logs.Add(logRow);
                Output.Add($"{tableName};{outputRow}");
            }

            //Запись выходных файлов
            try
            {
                foreach (var str in Output)
                {
                    if (!File.Exists("./Output/" + str.Split(';')[0] + ".txt"))
                    {
                        using (FileStream fs = File.Create("./Output/" + str.Split(';')[0] + ".txt"))
                        {
                            byte[] info = new UTF8Encoding(true).GetBytes(str.Split(';')[1]);
                            fs.Write(info, 0, info.Length);
                        }
                    }
                    else
                    {
                        using (StreamWriter writer = new StreamWriter("./Output/" + str.Split(';')[0] + ".txt"))
                        {
                            writer.WriteLine(str.Split(';')[1]);
                        }
                    }
                }

                if (!File.Exists("./Logs/Log-" + DateOnly.FromDateTime(DateTime.UtcNow) + ".txt"))
                {
                    using (FileStream fs = File.Create("./Logs/Log-" + DateOnly.FromDateTime(DateTime.UtcNow) + ".txt"))
                    {
                        foreach (var str in Logs)
                        {
                            byte[] info = new UTF8Encoding(true).GetBytes(str + "\t\n");
                            fs.Write(info, 0, info.Length);
                        }
                    }
                }
                else
                {
                    using (StreamWriter writer = new StreamWriter("./Logs/Log-" + DateOnly.FromDateTime(DateTime.UtcNow) + ".txt", true))
                    {
                        foreach (var str in Logs)
                        {
                            writer.WriteLine(str);
                        }
                    }
                }

                if (!File.Exists("./Status.txt"))
                {
                    using (FileStream fs = File.Create("./Status.txt"))
                    {
                        byte[] info = new UTF8Encoding(true).GetBytes(status);
                        fs.Write(info, 0, info.Length);
                    }
                }
                else
                {
                    using (StreamWriter writer = new StreamWriter("./Status.txt"))
                    {
                        writer.WriteLine(status);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                if (!File.Exists("./Status.txt"))
                    File.Create("./Status.txt");
                Thread.Sleep(15);
                using (StreamWriter writer = new StreamWriter("./Status.txt"))
                {
                    writer.WriteLine(status + "|" + ex.Message);
                }

                Console.ReadKey();
            }
            Console.WriteLine("Готово!");
        }
    }
}
