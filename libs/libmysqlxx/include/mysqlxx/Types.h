#pragma once

#include <string>
#include <mysql/mysql.h>
#include <Poco/Types.h>

#include <mysqlxx/Date.h>
#include <mysqlxx/DateTime.h>


namespace mysqlxx
{

typedef Poco::UInt64 UInt64;
typedef Poco::Int64 Int64;
typedef Poco::UInt32 UInt32;
typedef Poco::Int32 Int32;

typedef unsigned long MYSQL_LENGTH;
typedef MYSQL_LENGTH * MYSQL_LENGTHS;
typedef MYSQL_FIELD * MYSQL_FIELDS;

}
