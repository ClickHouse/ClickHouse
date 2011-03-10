#ifndef MYSQLXX_QUERY_H
#define MYSQLXX_QUERY_H

#include <ostream>

#include <mysqlxx/UseQueryResult.h>
#include <mysqlxx/StoreQueryResult.h>


namespace mysqlxx
{

class Query : public std::ostream
{
public:
	Query(Connection * conn_, const std::string & query_string);
	Query(const Query & other);
	Query & operator= (const Query & other);

	void reset();
	void execute();
	UseQueryResult use();
	StoreQueryResult store();

	UInt64 insertID();

	/// Для совместимости
	UInt64 insert_id() { return insertID(); }

	std::string str()
	{
		return query_buf.str();
	}

private:
	Connection * conn;
	std::stringbuf query_buf;
};


}

#endif
