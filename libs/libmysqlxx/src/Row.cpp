#if __has_include(<mariadb/mysql.h>)
#include <mariadb/mysql.h>
#else
#include <mysql/mysql.h>
#endif
#include <mysqlxx/Row.h>


namespace mysqlxx
{

Value Row::operator[] (const char * name) const
{
    unsigned n = res->getNumFields();
    MYSQL_FIELDS fields = res->getFields();

    for (unsigned i = 0; i < n; ++i)
        if (!strcmp(name, fields[i].name))
            return operator[](i);

    throw Exception(std::string("Unknown column ") + name);
}

}
