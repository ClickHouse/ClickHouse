#include <mysqlxx/Connection.h>
#include <mysqlxx/ResultBase.h>


namespace mysqlxx
{

void ResultBase::init()
{
	fields = mysql_fetch_fields(res);
	num_fields = mysql_num_fields(res);
}

ResultBase::ResultBase(MYSQL_RES * res_, Connection * conn_, const Query * query_) : res(res_), conn(conn_), query(query_)
{
	init();
}

ResultBase::ResultBase(const ResultBase & x)
{
	res = x.res;
	conn = x.conn;
	query = x.query;

	init();
}

ResultBase & ResultBase::operator= (const ResultBase & x)
{
	mysql_free_result(res);
	
	res = x.res;
	conn = x.conn;
	query = x.query;

	init();
	return *this;
}

}
