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
            mysqlxx::Query query = connection.query();
            query << "SELECT 1234567890 abc, 12345.67890 def UNION ALL SELECT 9876543210, 98765.43210";
            mysqlxx::StoreQueryResult result = query.store();

            std::cerr << result.at(0)["abc"].getUInt() << ", " << result.at(0)["def"].getDouble() << std::endl
                << result.at(1)["abc"].getUInt() << ", " << result.at(1)["def"].getDouble() << std::endl;
        }

        {
            mysqlxx::UseQueryResult result = connection.query("SELECT 'abc\\\\def' x").use();
            mysqlxx::Row row = result.fetch();
            std::cerr << row << std::endl;
            std::cerr << row << std::endl;
        }

        {
            mysqlxx::Query query = connection.query("SEL");
            query << "ECT 1";

            std::cerr << query.store().at(0).at(0) << std::endl;
        }

        {
            /// Копирование Query
            mysqlxx::Query query = connection.query("SELECT 'Ok' x");
            using Queries = std::vector<mysqlxx::Query>;
            Queries queries;
            queries.push_back(query);

            for (auto & q : queries)
            {
                std::cerr << q.str() << std::endl;
                std::cerr << q.store().at(0) << std::endl;
            }
        }

        {
            /// Копирование Query
            mysqlxx::Query query1 = connection.query("SELECT");
            mysqlxx::Query query2 = query1;
            query2 << " 1";

            std::cerr << query1.str() << ", " << query2.str() << std::endl;
        }

        {
            /// Копирование Query
            using Queries = std::list<mysqlxx::Query>;
            Queries queries;
            queries.push_back(connection.query("SELECT"));
            mysqlxx::Query & qref = queries.back();
            qref << " 1";

            for (auto & query : queries)
            {
                std::cerr << query.str() << std::endl;
                std::cerr << query.store().at(0) << std::endl;
            }
        }

        {
            /// Транзакции
            connection.query("DROP TABLE IF EXISTS tmp").execute();
            connection.query("CREATE TABLE tmp (x INT, PRIMARY KEY (x)) ENGINE = InnoDB").execute();

            mysqlxx::Transaction trans(connection);
            connection.query("INSERT INTO tmp VALUES (1)").execute();

            std::cerr << connection.query("SELECT * FROM tmp").store().size() << std::endl;

            trans.rollback();

            std::cerr << connection.query("SELECT * FROM tmp").store().size() << std::endl;
        }

        {
            /// Транзакции
            connection.query("DROP TABLE IF EXISTS tmp").execute();
            connection.query("CREATE TABLE tmp (x INT, PRIMARY KEY (x)) ENGINE = InnoDB").execute();

            {
                mysqlxx::Transaction trans(connection);
                connection.query("INSERT INTO tmp VALUES (1)").execute();
                std::cerr << connection.query("SELECT * FROM tmp").store().size() << std::endl;
            }

            std::cerr << connection.query("SELECT * FROM tmp").store().size() << std::endl;
        }

        {
            /// Транзакции
            mysqlxx::Connection connection2("test", "127.0.0.1", "root", "qwerty", 3306);
            connection2.query("DROP TABLE IF EXISTS tmp").execute();
            connection2.query("CREATE TABLE tmp (x INT, PRIMARY KEY (x)) ENGINE = InnoDB").execute();

            mysqlxx::Transaction trans(connection2);
            connection2.query("INSERT INTO tmp VALUES (1)").execute();
            std::cerr << connection2.query("SELECT * FROM tmp").store().size() << std::endl;
        }
        std::cerr << connection.query("SELECT * FROM tmp").store().size() << std::endl;

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

        {
            /// Исключения при попытке достать значение не того типа
            try
            {
                connection.query("SELECT -1").store().at(0).at(0).getUInt();
                std::cerr << "Fail" << std::endl;
            }
            catch (const mysqlxx::Exception & e)
            {
                std::cerr << "Ok, " << e.message() << std::endl;
            }

            try
            {
                connection.query("SELECT 'xxx'").store().at(0).at(0).getInt();
                std::cerr << "Fail" << std::endl;
            }
            catch (const mysqlxx::Exception & e)
            {
                std::cerr << "Ok, " << e.message() << std::endl;
            }

            try
            {
                connection.query("SELECT NULL").store().at(0).at(0).getString();
                std::cerr << "Fail" << std::endl;
            }
            catch (const mysqlxx::Exception & e)
            {
                std::cerr << "Ok, " << e.message() << std::endl;
            }

            try
            {
                connection.query("SELECT 123").store().at(0).at(0).getDate();
                std::cerr << "Fail" << std::endl;
            }
            catch (const mysqlxx::Exception & e)
            {
                std::cerr << "Ok, " << e.message() << std::endl;
            }

            try
            {
                connection.query("SELECT '2011-01-01'").store().at(0).at(0).getDateTime();
                std::cerr << "Fail" << std::endl;
            }
            catch (const mysqlxx::Exception & e)
            {
                std::cerr << "Ok, " << e.message() << std::endl;
            }
        }
    }
    catch (const mysqlxx::Exception & e)
    {
        std::cerr << e.code() << ", " << e.message() << std::endl;
        throw;
    }

    return 0;
}
