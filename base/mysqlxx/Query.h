#pragma once

#include <sstream>

#include <mysqlxx/UseQueryResult.h>
#include <mysqlxx/StoreQueryResult.h>


namespace mysqlxx
{


/** Запрос.
  * Ссылается на Connection. Если уничтожить Connection, то Query станет некорректным и пользоваться им будет нельзя.
  *
  * Пример использования:
  *        mysqlxx::Query query = connection.query();
  *        query << "SELECT 1 AS x, 2 AS y, 3 AS z";
  *        query << " LIMIT 1";
  *        mysqlxx::UseQueryResult result = query.use();
  *
  *        while (mysqlxx::Row row = result.fetch())
  *            std::cout << row["x"] << std::endl;
  *
  * В отличие от библиотеки mysql++, запрос можно копировать.
  * (то есть, запрос можно класть в контейнеры STL и ничего с ним не будет)
  *
  * Внимание! Один объект запроса можно использовать только из одного потока.
  */
class Query
{
public:
    Query(Connection * conn_, const std::string & query_string = "");
    Query(const Query & other);
    Query & operator= (const Query & other);
    ~Query();

    /** Сбросить текст запроса. Это используется, если нужно написать новый запрос в том же объекте. */
    void reset();

    /** Выполнить запрос, результат которого не имеет значения (почти всё кроме SELECT). */
    void execute();

    /** Выполнить запрос, с возможностью загружать на клиента строки одна за другой.
      * То есть, оперативка расходуется только на одну строку.
      */
    UseQueryResult use();

    /** Выполнить запрос с загрузкой на клиента всех строк.
      * Требуется оперативка, чтобы вместить весь результат, зато к строкам можно обращаться в произвольном порядке.
      */
    StoreQueryResult store();

    /// Значение auto increment после последнего INSERT-а.
    UInt64 insertID();

    /// Для совместимости, то же, что insertID().
    UInt64 insert_id() { return insertID(); }

    /// Получить текст запроса (например, для вывода его в лог). См. ещё operator<< ниже.
    std::string str() const
    {
        return query_buf.str();
    }

    auto rdbuf() const
    {
        return query_buf.rdbuf();
    }

    template <typename T>
    inline Query & operator<< (T && x)
    {
        query_buf << std::forward<T>(x);
        return *this;
    }

private:
    Connection * conn;
    std::ostringstream query_buf;

    void executeImpl();
};


/// Вывести текст запроса в ostream.
inline std::ostream & operator<< (std::ostream & ostr, const Query & query)
{
    return ostr << query.rdbuf();
}


}
