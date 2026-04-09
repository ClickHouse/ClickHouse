#pragma once

#include <mysqlxx/ResultBase.h>
#include <mysqlxx/Row.h>


namespace mysqlxx
{

class Connection;


/** Query execution result designed for reading rows one by one.
  * Only one current row is stored in memory.
  * When reading the next row, the previous one becomes invalid.
  * You must read all rows from the result
  *  (call the fetch() function until it returns a value that converts to false),
  *  otherwise the next query will throw an exception with the text "Commands out of sync".
  * The object contains a reference to Connection.
  * If Connection is destroyed, the object becomes invalid and all result rows too.
  * If you execute the next query in the connection, the object and all rows also become invalid.
  * The object can only be used for the result of one query!
  * (Attempting to assign the result of the next query to the object is UB.)
  */
class UseQueryResult : public ResultBase
{
public:
    UseQueryResult(MYSQL_RES * res_, Connection * conn_, const Query * query_);

    Row fetch();

    /// For compatibility
    Row fetch_row() { return fetch(); } /// NOLINT
};

}
