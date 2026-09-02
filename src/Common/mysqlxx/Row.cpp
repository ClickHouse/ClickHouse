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

enum enum_field_types Row::getFieldType(size_t i)
{
    if (i >= res->getNumFields())
        throw Exception(std::string("Array Index Overflow"));
    MYSQL_FIELDS fields = res->getFields();
    return fields[i].type;
}

}
