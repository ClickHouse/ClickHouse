#if __has_include(<mysql.h>)
#include <mysql.h>
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

std::string Row::getFieldName(size_t n) const
{
    if (res->getNumFields() <= n)
        throw Exception(std::string("Unknown column position ") + std::to_string(n));

    return res->getFields()[n].name;
}

}
