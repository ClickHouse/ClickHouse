#include <mysqlxx/Connection.h>
#include <mysqlxx/ResultBase.h>


namespace mysqlxx
{

ResultBase::ResultBase(MYSQL_RES * res_, Connection * conn_) : res(res_), conn(conn_)
{
	fields = mysql_fetch_fields(res);
	num_fields = mysql_num_fields(res);
}

}
