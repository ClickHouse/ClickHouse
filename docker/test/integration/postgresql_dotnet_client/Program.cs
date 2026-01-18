using System;
using Npgsql;
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
            using (var conn = new NpgsqlConnection(connectionString))
            {
                conn.Open();
                Console.WriteLine("Connection sucessful!");

                using var command = new NpgsqlCommand("SELECT 1", conn);
                var result = command.ExecuteScalar();

                Console.WriteLine($"Result: {result}");

                using (var selectCmd = new NpgsqlCommand("SELECT oid, typname FROM pg_type;", conn))
                using (var reader = selectCmd.ExecuteReader())
                {
                    Console.WriteLine("Content:");
                    while (reader.Read())
                    {
                        string id = reader.GetString(0);
                        string name = reader.GetString(1);
                        Console.WriteLine($"id = {id}, name = {name}");
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error: " + ex.Message);
        }
    }
}
