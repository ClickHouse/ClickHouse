#pragma once

#include <string>
#include <Poco/Types.h>

struct st_mysql;
using MYSQL = st_mysql;

struct st_mysql_res;
using MYSQL_RES = st_mysql_res;

using MYSQL_ROW = char**;

struct st_mysql_field;
using MYSQL_FIELD = st_mysql_field;



namespace mysqlxx
{

using UInt64 = Poco::UInt64;
using Int64 = Poco::Int64;
using UInt32 = Poco::UInt32;
using Int32 = Poco::Int32;

using MYSQL_LENGTH = unsigned long;
using MYSQL_LENGTHS = MYSQL_LENGTH *;
using MYSQL_FIELDS = MYSQL_FIELD *;

}
