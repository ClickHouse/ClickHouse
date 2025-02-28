#include <mysqlxx/mysqlxx.h>

#include <chrono>
#include <iostream>
#include <thread>


namespace
{
mysqlxx::Pool::Entry getWithFailover(mysqlxx::Pool & connections_pool)
{
    using namespace std::chrono;

    constexpr size_t max_tries = 3;

    mysqlxx::Pool::Entry worker_connection;

    for (size_t try_no = 1; try_no <= max_tries; ++try_no)
    {
        try
        {
            worker_connection = connections_pool.tryGet();

            if (!worker_connection.isNull())
            {
                return worker_connection;
            }
        }
        catch (const Poco::Exception & e)
        {
            if (e.displayText().find("mysqlxx::Pool is full") != std::string::npos)
            {
                std::cerr << e.displayText() << std::endl;
            }

            std::cerr << "Connection to " << connections_pool.getDescription() << " failed: " << e.displayText() << std::endl;
        }

        std::clog << "Connection to all replicas failed " << try_no << " times" << std::endl;
        std::this_thread::sleep_for(1s);
    }

    throw Poco::Exception("Connections to all replicas failed: " + connections_pool.getDescription());
}
}

int main(int, char **)
{
    using namespace std::chrono;

    const char * remote_mysql = "localhost";
    const std::string test_query = "SHOW DATABASES";

    mysqlxx::Pool mysql_conn_pool("", remote_mysql, "default", "10203040", 3306);

    size_t iteration = 0;
    while (++iteration)
    {
        std::clog << "Iteration: " << iteration << std::endl;
        try
        {
            std::clog << "Acquiring DB connection ...";
            mysqlxx::Pool::Entry worker = getWithFailover(mysql_conn_pool);
            std::clog << "ok" << std::endl;

            std::clog << "Preparing query (5s sleep) ...";
            std::this_thread::sleep_for(5s);
            mysqlxx::Query query = worker->query(test_query);
            std::clog << "ok" << std::endl;

            std::clog << "Querying result (5s sleep) ...";
            std::this_thread::sleep_for(5s);
            mysqlxx::UseQueryResult result = query.use();
            std::clog << "ok" << std::endl;

            std::clog << "Fetching result data (5s sleep) ...";
            std::this_thread::sleep_for(5s);
            size_t rows_count = 0;
            while (result.fetch())
                ++rows_count;
            std::clog << "ok" << std::endl;

            std::clog << "Read " << rows_count << " rows." << std::endl;
        }
        catch (const Poco::Exception & e)
        {
            std::cerr << "Iteration FAILED:\n" << e.displayText() << std::endl;
        }

        std::clog << "====================" << std::endl;
        std::this_thread::sleep_for(3s);
    }
}
