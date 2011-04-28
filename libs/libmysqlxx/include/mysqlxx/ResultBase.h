#ifndef MYSQLXX_RESULTBASE_H
#define MYSQLXX_RESULTBASE_H

#include <boost/noncopyable.hpp>

#include <mysqlxx/Types.h>


namespace mysqlxx
{

class Connection;
class Query;


/** Базовый класс для UseQueryResult и StoreQueryResult.
  * Содержит общую часть реализации, 
  * Ссылается на Connection. Если уничтожить Connection, то пользоваться ResultBase и любым результатом нельзя.
  */
class ResultBase
{
public:
	ResultBase(MYSQL_RES * res_, Connection * conn_, const Query * query_);
	ResultBase(const ResultBase & x);
	ResultBase & operator= (const ResultBase & x);

	Connection * getConnection() 	{ return conn; }
	MYSQL_FIELDS getFields() 		{ return fields; }
	unsigned getNumFields() 		{ return num_fields; }
	MYSQL_RES * getRes()			{ return res; }
	const Query * getQuery() const	{ return query; }

	virtual ~ResultBase()
	{
		mysql_free_result(res);
	}

protected:
	MYSQL_RES * res;
	Connection * conn;
	const Query * query;
	MYSQL_FIELDS fields;
	unsigned num_fields;

private:
	void init();
};

}

#endif
