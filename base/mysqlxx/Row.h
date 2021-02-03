#pragma once

#include <mysqlxx/Types.h>
#include <mysqlxx/Value.h>
#include <mysqlxx/ResultBase.h>
#include <mysqlxx/Exception.h>


namespace mysqlxx
{

class ResultBase;


/** Строка результата.
  * В отличие от mysql++,
  *  представляет собой обёртку над MYSQL_ROW (char**), ссылается на ResultBase, не владеет сам никакими данными.
  * Это значит, что если будет уничтожен объект результата или соединение,
  *  или будет задан следующий запрос, то Row станет некорректным.
  * При использовании UseQueryResult, в памяти хранится только одна строка результата,
  *  это значит, что после чтения следующей строки, предыдущая становится некорректной.
  */
class Row
{
private:
    /** @brief Pointer to bool data member, for use by safe bool conversion operator.
      * @see http://www.artima.com/cppsource/safebool.html
      * Взято из mysql++.
      */
    typedef MYSQL_ROW Row::*private_bool_type;

public:
    /** Для возможности отложенной инициализации. */
    Row()
    {
    }

    /** Для того, чтобы создать Row, используйте соответствующие методы UseQueryResult. */
    Row(MYSQL_ROW row_, ResultBase * res_, MYSQL_LENGTHS lengths_)
        : row(row_), res(res_), lengths(lengths_)
    {
    }

    /** Получить значение по индексу.
      * Здесь используется int, а не unsigned, чтобы не было неоднозначности с тем же методом, принимающим const char *.
      */
    Value operator[] (int n) const
    {
        if (unlikely(static_cast<size_t>(n) >= res->getNumFields()))
            throw Exception("Index of column is out of range.");
        return Value(row[n], lengths[n], res);
    }

    /** Get value by column name. Less efficient. */
    Value operator[] (const char * name) const;

    Value operator[] (const std::string & name) const
    {
        return operator[](name.c_str());
    }

    /** Получить значение по индексу. */
    Value at(size_t n) const
    {
        return operator[](n);
    }

    /** Количество столбцов. */
    size_t size() const { return res->getNumFields(); }

    /** Является ли пустым? Такой объект используется, чтобы обозначить конец результата
      * при использовании UseQueryResult. Или это значит, что объект не инициализирован.
      * Вы можете использовать вместо этого преобразование в bool.
      */
    bool empty() const { return row == nullptr; }

    /** Преобразование в bool.
      * (Точнее - в тип, который преобразуется в bool, и с которым больше почти ничего нельзя сделать.)
      */
    operator private_bool_type() const { return row == nullptr ? nullptr : &Row::row; }

private:
    MYSQL_ROW row{};
    ResultBase * res{};
    MYSQL_LENGTHS lengths{};
};

}
