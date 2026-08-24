using System;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;

namespace clickhouse.test
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                using var connection = new ClickHouseConnection(GetConnectionString(args));

                await connection.ExecuteStatementAsync("CREATE DATABASE IF NOT EXISTS test");
                await connection.ExecuteStatementAsync("TRUNCATE TABLE IF EXISTS test.dotnet_test");
                await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.dotnet_test (`age` Int32, `name` String) Engine = Memory");

                using var command = connection.CreateCommand();
                command.AddParameter("name", "Linus Torvalds");
                command.AddParameter("age", 51);
                command.CommandText = "INSERT INTO test.dotnet_test VALUES({age:Int32}, {name:String})";
                await command.ExecuteNonQueryAsync();

                using var result1 = await connection.ExecuteReaderAsync("SELECT * FROM test.dotnet_test");
                while (result1.Read())
                {
                    var values = new object[result1.FieldCount];
                    result1.GetValues(values);

                    foreach (var row in values)
                    {
                        Console.WriteLine(row);
                    }
                }

                using var result2 = await connection.ExecuteReaderAsync(selectSql);
                while (result2.Read())
                {
                    var values = new object[result2.FieldCount];
                    result2.GetValues(values);

                    foreach (var row in values)
                    {
                        Console.WriteLine(row);
                    }
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e);
                Environment.ExitCode = 1;
            }
        }

        private static string GetConnectionString(string[] args)
        {
            var builder = new ClickHouseConnectionStringBuilder();
            int i = 0;
            while (i < args.Length)
            {
                switch (args[i])
                {
                    case "--host":
                        builder.Host = args[++i];
                        break;
                    case "--port":
                        builder.Port = UInt16.Parse(args[++i]);
                        break;
                    case "--user":
                        builder.Username = args[++i];
                        break;
                    case "--password":
                        builder.Password = args[++i];
                        break;
                    case "--database":
                        builder.Database = args[++i];
                        break;
                    default:
                        i++;
                        break;
                }
            }
            return builder.ToString();
        }

        private static string selectSql = @"SELECT NULL, toInt8(-8), toUInt8(8), toInt16(-16), toUInt16(16), toInt16(-32), toUInt16(32), toInt64(-64), toUInt64(64), toFloat32(32e6), toFloat32(-32e6), toFloat64(64e6), toFloat64(-64e6), 'TestString', toFixedString('ASD',3), toFixedString('ASD',5), toUUID('00000000-0000-0000-0000-000000000000'), toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toIPv4('1.2.3.4'), toIPv4('255.255.255.255'), CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'), CAST('a', 'Enum8(\'a\' = -1, \'b\' = 127)'), CAST('a', 'Enum16(\'a\' = -32768, \'b\' = 32767)'), array(1, 2, 3), array('a', 'b', 'c'), array(1, 2, NULL), toInt32OrNull('123'), toInt32OrNull(NULL), CAST(NULL AS Nullable(DateTime)), CAST(NULL AS LowCardinality(Nullable(String))), toLowCardinality('lowcardinality'), tuple(1, 'a', 8), tuple(123, tuple(5, 'a', 7)), toDateOrNull('1999-11-12'), toDateTime('1988-08-28 11:22:33'), toDateTime64('2043-03-01 18:34:04.4444444', 9), toDecimal32(123.45, 3), toDecimal32(-123.45, 3), toDecimal64(1.2345, 7), toDecimal64(-1.2345, 7), toDecimal128(12.34, 9), toDecimal128(-12.34, 9), toIPv6('2001:0db8:85a3:0000:0000:8a2e:0370:7334')";
    }
}
