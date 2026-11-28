#pragma once

#include <sstream>

#include <mysqlxx/UseQueryResult.h>


namespace mysqlxx
{


/** Query.
  * References Connection. If Connection is destroyed, Query becomes invalid and cannot be used.
  *
  * Usage example:
  *        mysqlxx::Query query = connection.query("SELECT 1 AS x, 2 AS y, 3 AS z LIMIT 1");
  *        mysqlxx::UseQueryResult result = query.use();
  *
  *        while (mysqlxx::Row row = result.fetch())
  *            std::cout << row["x"] << std::endl;
  *
  * Unlike the mysql++ library, the query can be copied.
  * (that is, the query can be placed in STL containers and nothing will happen to it)
  *
  * Attention! One query object can only be used from one thread.
  */
class Query
{
public:
    Query(Connection * conn_, const std::string & query_string);
    Query(const Query & other);
    Query & operator= (const Query & other);
    ~Query();

    /** Execute a query whose result is not meaningful (almost everything except SELECT). */
    void execute();

    /** Execute query with the ability to load rows to the client one by one.
      * That is, RAM is spent only on one row.
      */
    UseQueryResult use();

    /// Auto increment value after the last INSERT.
    UInt64 insertID();

    /// For compatibility, same as insertID().
    UInt64 insert_id() { return insertID(); }

    /// Get query text (for example, to output it to the log). See also operator<< below.
    std::string str() const
    {
        return query;
    }

private:
    Connection * conn;
    std::string query;

    void executeImpl();
};


/// Output query text to ostream.
inline std::ostream & operator<< (std::ostream & ostr, const Query & query)
{
    return ostr << query.str();
}


}
