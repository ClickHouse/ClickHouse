#include <mysqlxx/Connection.h>
#include <mysqlxx/Query.h>


namespace mysqlxx
{

Query::Query(Connection & conn_, const std::string & query_string) : conn(conn_)
{
	query_stream << query_string;
}

Query::Query(const Query & other) : conn(other.conn)
{
	query_stream << other.query_stream.rdbuf();
}

void Query::reset()
{
	query_stream.str("");
}

void Query::execute()
{
	std::string query_string = query_stream.str();
	if (mysql_real_query(&conn.getDriver(), query_string.data(), query_string.size()))
		throw BadQuery(mysql_error(&conn.getDriver()), mysql_errno(&conn.getDriver()));
}

UseQueryResult Query::use()
{
	execute();
	MYSQL_RES * res = mysql_use_result(&conn.getDriver());
	if (!res)
		onError(conn.getDriver());

	return UseQueryResult(*res, conn);
}

StoreQueryResult Query::store()
{
	execute();
	MYSQL_RES * res = mysql_store_result(&conn.getDriver());
	if (!res)
		checkError(conn.getDriver());

	return StoreQueryResult(*res, conn);
}

}
