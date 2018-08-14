#if __has_include(<mariadb/mysql.h>)
#include <mariadb/mysql.h>
#else
#include <mysql/mysql.h>
#endif

#include <mysqlxx/Connection.h>
#include <mysqlxx/UseQueryResult.h>


namespace mysqlxx
{

UseQueryResult::UseQueryResult(MYSQL_RES * res_, Connection * conn_, const Query * query_) : ResultBase(res_, conn_, query_)
{
}

Row UseQueryResult::fetch()
{
    MYSQL_ROW row = mysql_fetch_row(res);
    if (!row)
        checkError(conn->getDriver());

    return Row(row, this, mysql_fetch_lengths(res));
}

}
