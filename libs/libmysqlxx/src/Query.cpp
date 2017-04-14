#if USE_MYSQL
#include <mysql/mysql.h>
#endif
#include <mysqlxx/Connection.h>
#include <mysqlxx/Query.h>


namespace mysqlxx
{

Query::Query(Connection * conn_, const std::string & query_string) : std::ostream(0), conn(conn_)
{
#if USE_MYSQL
    /// Важно в случае, если Query используется не из того же потока, что Connection.
    mysql_thread_init();
#endif

    init(&query_buf);

    if (!query_string.empty())
    {
        query_buf.str(query_string);
        seekp(0, std::ios::end);
    }

    imbue(std::locale::classic());
}

Query::Query(const Query & other) : std::ostream(0), conn(other.conn)
{
#if USE_MYSQL
    /// Важно в случае, если Query используется не из того же потока, что Connection.
    mysql_thread_init();
#endif

    init(&query_buf);
    imbue(std::locale::classic());

    *this << other.str();
}

Query & Query::operator= (const Query & other)
{
    conn = other.conn;

    seekp(0);
    clear();
    *this << other.str();

    return *this;
}

Query::~Query()
{
#if USE_MYSQL
    mysql_thread_end();
#endif
}

void Query::reset()
{
    seekp(0);
    clear();
    query_buf.str("");
}

void Query::executeImpl()
{
    std::string query_string = query_buf.str();
#if USE_MYSQL
    if (mysql_real_query(conn->getDriver(), query_string.data(), query_string.size()))
        throw BadQuery(errorMessage(conn->getDriver()), mysql_errno(conn->getDriver()));
#endif
}

UseQueryResult Query::use()
{
    executeImpl();
#if USE_MYSQL
    MYSQL_RES * res = mysql_use_result(conn->getDriver());
    if (!res)
        onError(conn->getDriver());

    return UseQueryResult(res, conn, this);
#else
    throw;
#endif
}

StoreQueryResult Query::store()
{
    executeImpl();
#if USE_MYSQL
    MYSQL_RES * res = mysql_store_result(conn->getDriver());
    if (!res)
        checkError(conn->getDriver());

    return StoreQueryResult(res, conn, this);
#else
    throw;
#endif
}

void Query::execute()
{
    executeImpl();
}

UInt64 Query::insertID()
{
#if USE_MYSQL
    return mysql_insert_id(conn->getDriver());
#else
    throw;
#endif
}

}
