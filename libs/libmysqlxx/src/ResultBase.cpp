#include <mysql/mysql.h>

#include <mysqlxx/Connection.h>
#include <mysqlxx/ResultBase.h>


namespace mysqlxx
{

ResultBase::ResultBase(MYSQL_RES * res_, Connection * conn_, const Query * query_) : res(res_), conn(conn_), query(query_)
{
    fields = mysql_fetch_fields(res);
    num_fields = mysql_num_fields(res);
}

ResultBase::~ResultBase()
{
    mysql_free_result(res);
}

}
