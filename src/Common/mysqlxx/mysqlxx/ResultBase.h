#pragma once

#include <mysqlxx/Types.h>

namespace mysqlxx
{

class Connection;
class Query;


/** Базовый класс для UseQueryResult.
  * Содержит общую часть реализации,
  * Ссылается на Connection. Если уничтожить Connection, то пользоваться ResultBase и любым результатом нельзя.
  * Использовать объект можно только для результата одного запроса!
  * (При попытке присвоить объекту результат следующего запроса - UB.)
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
