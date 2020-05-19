#if __has_include(<mysql.h>)
#include <mysql.h>
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
    reserve(rows);
    lengths.resize(rows * num_fields);

    for (UInt64 i = 0; MYSQL_ROW row = mysql_fetch_row(res); ++i)
    {
        MYSQL_LENGTHS lengths_for_row = mysql_fetch_lengths(res);
        memcpy(&lengths[i * num_fields], lengths_for_row, sizeof(lengths[0]) * num_fields);

        push_back(Row(row, this, &lengths[i * num_fields]));
    }
    checkError(conn->getDriver());
}

}
