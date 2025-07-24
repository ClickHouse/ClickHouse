using System;
using MySql.Data.MySqlClient;

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
    static void Main()
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

                string createTableSql = @"
                    CREATE TABLE IF NOT EXISTS test_table (
                        id UInt32,
                        name String
                    ) ENGINE = Memory";

                using (var cmd = new MySqlCommand(createTableSql, connection))
                {
                    cmd.ExecuteNonQuery();
                    Console.WriteLine("Table created or exists");
                }

                string insertSql = "INSERT INTO test_table (id, name) VALUES (1, 'Alice'), (2, 'Bob')";

                using (var cmd = new MySqlCommand(insertSql, connection))
                {
                    int rows = cmd.ExecuteNonQuery();
                    Console.WriteLine($"{rows} rows inserted");
                }

                string selectSql = "SELECT id, name FROM test_table";

                using (var cmd = new MySqlCommand(selectSql, connection))
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        uint id = (uint)reader.GetInt32(0);
                        string name = reader.GetString(1);
                        Console.WriteLine($"id={id}, name={name}");
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
