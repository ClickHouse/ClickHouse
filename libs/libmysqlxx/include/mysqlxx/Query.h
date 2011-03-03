#ifndef MYSQLXX_QUERY_H
#define MYSQLXX_QUERY_H

#include <sstream>

#include <mysqlxx/UseQueryResult.h>
#include <mysqlxx/StoreQueryResult.h>


namespace mysqlxx
{

class Query
{
public:
	Query(Connection & conn_, const std::string & query_string);
	Query(const Query & other);

	void reset();
	void execute();
	UseQueryResult use();
	StoreQueryResult store();

	template <typename T>
	Query & operator<< (T & x)
	{
		query_stream << x;
		return *this;
	}

private:
	Connection & conn;
	std::stringstream query_stream;
};


}

#endif
