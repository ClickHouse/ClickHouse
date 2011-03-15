#ifndef MYSQLXX_USEQUERYRESULT_H
#define MYSQLXX_USEQUERYRESULT_H

#include <mysqlxx/ResultBase.h>
#include <mysqlxx/Row.h>


namespace mysqlxx
{

class Connection;
	
class UseQueryResult : public ResultBase
{
public:
	UseQueryResult(MYSQL_RES * res_, Connection * conn_);

	Row fetch();

	/// Для совместимости
	Row fetch_row() { return fetch(); }
};

}

#endif
