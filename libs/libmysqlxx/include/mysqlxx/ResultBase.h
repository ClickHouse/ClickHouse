#ifndef MYSQLXX_RESULTBASE_H
#define MYSQLXX_RESULTBASE_H

#include <boost/noncopyable.hpp>

#include <mysqlxx/Types.h>


namespace mysqlxx
{

class Connection;

class ResultBase
{
public:
	ResultBase(MYSQL_RES * res_, Connection * conn_);

	Connection * getConnection() 	{ return conn; }
	MYSQL_FIELDS getFields() 		{ return fields; }
	unsigned getNumFields() 		{ return num_fields; }
	MYSQL_RES * getRes()			{ return res; }

	virtual ~ResultBase()
	{
		mysql_free_result(res);
	}

protected:
	MYSQL_RES * res;
	Connection * conn;
	MYSQL_FIELDS fields;
	unsigned num_fields;
};

}

#endif
