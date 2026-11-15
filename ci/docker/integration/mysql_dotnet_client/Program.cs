using System;
using MySql.Data.MySqlClient;
using CommandLine;

class Program
{
    [Option('h', "host", HelpText = "Host of ClickHouse server")]
    public string Host { get; set; }

    [Option('p', "port", HelpText = "Port of ClickHouse server")]
    public string Port { get; set; }

    [Option('u', "username", HelpText = "Username")]
    public string Username { get; set; }

    [Option('p', "password", HelpText = "Password")]
    public string Password { get; set; }
    static void Main(string[] args)
    {
        var parsed = Parser.Default.ParseArguments<Program>(args);
        var options = parsed.Value;
        var connectionString = $"Host={options.Host};Port={options.Port};Username={options.Username};Password={options.Password};Database=default;";

        try
        {
            using (var connection = new MySqlConnection(connectionString))
            {
                Console.WriteLine("Openning connection...");
                connection.Open();
                Console.WriteLine("Connection openned!");

                using (var cmd = new MySqlCommand("SELECT 1 as a;", connection))
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine(reader.GetValue(0));
                    }
                }

                string dropTableSql = @"
                    DROP TABLE IF EXISTS test_table;";

                using (var cmd = new MySqlCommand(dropTableSql, connection))
                {
                    cmd.ExecuteNonQuery();
                    Console.WriteLine("Table dropped");
                }

                string createTableSql = @"
                    CREATE TABLE IF NOT EXISTS test_table (
                        id UInt32
                    ) ENGINE = Memory";

                using (var cmd = new MySqlCommand(createTableSql, connection))
                {
                    cmd.ExecuteNonQuery();
                    Console.WriteLine("Table created or exists");
                }

                string insertSql = "INSERT INTO test_table (id) VALUES (1), (2)";

                using (var cmd = new MySqlCommand(insertSql, connection))
                {
                    int rows = cmd.ExecuteNonQuery();
                    Console.WriteLine($"{rows} rows inserted");
                }

                string selectSql = "SELECT id FROM test_table";

                using (var cmd = new MySqlCommand(selectSql, connection))
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        uint id = (uint)reader.GetInt32(0);
                        Console.WriteLine($"id={id}");
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("=== Exception ===");
            Console.WriteLine($"Type: {ex.GetType()}");
            Console.WriteLine($"Message: {ex.Message}");
            Console.WriteLine($"StackTrace: {ex.StackTrace}");

            if (ex.InnerException != null)
            {
                Console.WriteLine("--- Inner Exception ---");
                Console.WriteLine($"Type: {ex.InnerException.GetType()}");
                Console.WriteLine($"Message: {ex.InnerException.Message}");
                Console.WriteLine($"StackTrace: {ex.InnerException.StackTrace}");
            }
        }
    }
}
