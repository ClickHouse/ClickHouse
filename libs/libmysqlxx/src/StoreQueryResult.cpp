#if __has_include(<mariadb/mysql.h>)
#include <mariadb/mysql.h>
#else
#include <mysql/mysql.h>
#endif

#include <mysqlxx/Connection.h>
#include <mysqlxx/StoreQueryResult.h>


namespace mysqlxx
{

StoreQueryResult::StoreQueryResult(MYSQL_RES * res_, Connection * conn_, const Query * query_) : ResultBase(res_, conn_, query_)
{
    UInt64 rows = mysql_num_rows(res);
    UInt32 fields = getNumFields();
    reserve(rows);
    lengths.resize(rows * fields);

    for (UInt64 i = 0; MYSQL_ROW row = mysql_fetch_row(res); ++i)
    {
        MYSQL_LENGTHS lengths_for_row = mysql_fetch_lengths(res);
        memcpy(&lengths[i * fields], lengths_for_row, sizeof(lengths[0]) * fields);

        push_back(Row(row, this, &lengths[i * fields]));
    }
    checkError(conn->getDriver());
}

}
