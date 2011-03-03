#ifndef MYSQLXX_TYPES_H
#define MYSQLXX_TYPES_H

#include <mysql/mysql.h>


namespace mysqlxx
{

typedef unsigned long long UInt64;
typedef long long Int64;
typedef unsigned UInt32;
typedef int Int32;

typedef unsigned long * MYSQL_LENGTHS;
typedef MYSQL_FIELD * MYSQL_FIELDS;

}

#endif
