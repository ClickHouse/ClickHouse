#if USE_MYSQL
#include <mysql/mysql.h>
#endif
#include <mysqlxx/Connection.h>
#include <mysqlxx/ResultBase.h>


namespace mysqlxx
{

ResultBase::ResultBase(MYSQL_RES * res_, Connection * conn_, const Query * query_) : res(res_), conn(conn_), query(query_)
{
#if USE_MYSQL
    fields = mysql_fetch_fields(res);
    num_fields = mysql_num_fields(res);
#endif
}

ResultBase::~ResultBase()
{
#if USE_MYSQL
    mysql_free_result(res);
#endif
}

}
