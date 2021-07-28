#include <iostream>
#include <mysqlxx/mysqlxx.h>


int main(int, char **)
{
    try
    {
        mysqlxx::Connection connection("test", "127.0.0.1", "root", "qwerty", 3306);
        std::cerr << "Connected." << std::endl;

        {
            mysqlxx::Query query = connection.query();
            query << "SELECT 1 x, '2010-01-01 01:01:01' d";
            mysqlxx::UseQueryResult result = query.use();
            std::cerr << "use() called." << std::endl;

            while (mysqlxx::Row row = result.fetch())
            {
                std::cerr << "Fetched row." << std::endl;
                std::cerr << row[0] << ", " << row["x"] << std::endl;
                std::cerr << row[1] << ", " << row["d"]
                    << ", " << row[1].getDate()
                    << ", " << row[1].getDateTime()
                    << ", " << row[1].getDate()
                    << ", " << row[1].getDateTime()
                    << std::endl
                    << row[1].getDate() << ", " << row[1].getDateTime() << std::endl
                    << row[1].getDate() << ", " << row[1].getDateTime() << std::endl
                    << row[1].getDate() << ", " << row[1].getDateTime() << std::endl
                    << row[1].getDate() << ", " << row[1].getDateTime() << std::endl
                    ;

                time_t t1 = row[0];
                time_t t2 = row[1];
                std::cerr << t1 << ", " << LocalDateTime(t1) << std::endl;
                std::cerr << t2 << ", " << LocalDateTime(t2) << std::endl;
            }
        }

        {
            mysqlxx::UseQueryResult result = connection.query("SELECT 'abc\\\\def' x").use();
            mysqlxx::Row row = result.fetch();
            std::cerr << row << std::endl;
            std::cerr << row << std::endl;
        }

        {
            /// Копирование Query
            mysqlxx::Query query1 = connection.query("SELECT");
            mysqlxx::Query query2 = query1;
            query2 << " 1";

            std::cerr << query1.str() << ", " << query2.str() << std::endl;
        }

        {
            /// NULL
            mysqlxx::Null<int> x = mysqlxx::null;
            std::cerr << (x == mysqlxx::null ? "Ok" : "Fail") << std::endl;
            std::cerr << (x == 0 ? "Fail" : "Ok") << std::endl;
            std::cerr << (x.isNull() ? "Ok" : "Fail") << std::endl;
            x = 1;
            std::cerr << (x == mysqlxx::null ? "Fail" : "Ok") << std::endl;
            std::cerr << (x == 0 ? "Fail" : "Ok") << std::endl;
            std::cerr << (x == 1 ? "Ok" : "Fail") << std::endl;
            std::cerr << (x.isNull() ? "Fail" : "Ok") << std::endl;
        }
    }
    catch (const mysqlxx::Exception & e)
    {
        std::cerr << e.code() << ", " << e.message() << std::endl;
        throw;
    }

    return 0;
}
