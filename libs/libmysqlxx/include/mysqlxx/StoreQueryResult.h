#ifndef MYSQLXX_STOREQUERYRESULT_H
#define MYSQLXX_STOREQUERYRESULT_H

#include <vector>

#include <mysqlxx/ResultBase.h>
#include <mysqlxx/Row.h>


namespace mysqlxx
{

class Connection;

class StoreQueryResult : public std::vector<Row>, public ResultBase
{
public:
	StoreQueryResult(MYSQL_RES * res_, Connection * conn_);

	size_t num_rows() const { return size(); }
};

}

#endif
