#include <mysqlxx/Connection.h>
#include <mysqlxx/StoreQueryResult.h>


namespace mysqlxx
{

StoreQueryResult::StoreQueryResult(MYSQL_RES * res_, Connection * conn_, const Query * query_) : ResultBase(res_, conn_, query_)
{
	reserve(mysql_num_rows(res));
	while (MYSQL_ROW row = mysql_fetch_row(res))
		push_back(Row(row, this));
	checkError(conn->getDriver());
}
	
}
