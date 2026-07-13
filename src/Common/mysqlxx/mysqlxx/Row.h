#pragma once

#include <mysqlxx/Types.h>
#include <mysqlxx/Value.h>
#include <mysqlxx/ResultBase.h>
#include <mysqlxx/Exception.h>


namespace mysqlxx
{

class ResultBase;


/** Result row.
  * Unlike mysql++,
  *  this is a wrapper over MYSQL_ROW (char**), references ResultBase, does not own any data itself.
  * This means that if the result object or connection is destroyed,
  *  or the next query is executed, then Row becomes invalid.
  * When using UseQueryResult, only one result row is stored in memory,
  *  this means that after reading the next row, the previous one becomes invalid.
  */
class Row
{
private:
    /** @brief Pointer to bool data member, for use by safe bool conversion operator.
      * @see http://www.artima.com/cppsource/safebool.html
      * Taken from mysql++.
      */
    typedef MYSQL_ROW Row::*private_bool_type; /// NOLINT

public:
    /** For delayed initialization capability. */
    Row() /// NOLINT
    {
    }

    /** To create a Row, use the corresponding methods of UseQueryResult. */
    Row(MYSQL_ROW row_, ResultBase * res_, MYSQL_LENGTHS lengths_)
        : row(row_), res(res_), lengths(lengths_)
    {
    }

    /** Get value by index.
      * Here int is used instead of unsigned to avoid ambiguity with the same method that takes const char *.
      */
    Value operator[] (size_t n) const
    {
        if (unlikely(n >= res->getNumFields()))
            throw Exception("Index of column is out of range.");
        return Value(row[n], lengths[n], res);
    }

    /** Get value by column name. Less efficient. */
    Value operator[] (const char * name) const;

    Value operator[] (const std::string & name) const
    {
        return operator[](name.c_str());
    }

    /** Get value by index. */
    Value at(size_t n) const
    {
        return operator[](n);
    }

    /** Number of columns. */
    size_t size() const { return res->getNumFields(); }

    /** Is it empty? Such an object is used to indicate the end of the result
      * when using UseQueryResult. Or it means that the object is not initialized.
      * You can use bool conversion instead.
      */
    bool empty() const { return row == nullptr; }

    /** Conversion to bool.
      * (More precisely - to a type that converts to bool, and with which you can do almost nothing else.)
      */
    operator private_bool_type() const { return row == nullptr ? nullptr : &Row::row; } /// NOLINT

    enum enum_field_types getFieldType(size_t i);

private:
    MYSQL_ROW row{};
    ResultBase * res{};
    MYSQL_LENGTHS lengths{};
};

}
