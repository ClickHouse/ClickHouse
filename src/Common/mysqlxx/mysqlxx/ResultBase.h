#pragma once

#include <mysqlxx/Types.h>

namespace mysqlxx
{

class Connection;
class Query;


/** Base class for UseQueryResult.
  * Contains the common part of the implementation,
  * References Connection. If Connection is destroyed, then ResultBase and any result cannot be used.
  * The object can only be used for the result of one query!
  * (Attempting to assign the result of the next query to the object is UB.)
  */
class ResultBase
{
public:
    ResultBase(MYSQL_RES * res_, Connection * conn_, const Query * query_);

    ResultBase(const ResultBase &) = delete;
    ResultBase & operator=(const ResultBase &) = delete;
    ResultBase(ResultBase &&) = default;
    ResultBase & operator=(ResultBase &&) = default;

    Connection * getConnection() { return conn; }
    MYSQL_FIELDS getFields() { return fields; }
    unsigned getNumFields() const { return num_fields; }
    MYSQL_RES * getRes() { return res; }
    const Query * getQuery() const { return query; }

    std::string getFieldName(size_t n) const;

    virtual ~ResultBase();

protected:
    MYSQL_RES * res;
    Connection * conn;
    const Query * query;
    MYSQL_FIELDS fields;
    unsigned num_fields;
};

}
