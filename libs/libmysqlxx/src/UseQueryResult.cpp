#include <mysqlxx/Connection.h>
#include <mysqlxx/UseQueryResult.h>


namespace mysqlxx
{

UseQueryResult::UseQueryResult(MYSQL_RES & res_, Connection & conn_) : ResultBase(res_, conn_)
{
}

Row UseQueryResult::fetch_row()
{
	MYSQL_ROW row = mysql_fetch_row(&res);
	if (!row)
		checkError(conn.getDriver());

	return Row(row, this);
}

}
