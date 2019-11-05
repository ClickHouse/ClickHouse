#pragma once

#include <cstdint>
#include <string>

#ifndef _mysql_h

struct st_mysql;
using MYSQL = st_mysql;

struct st_mysql_res;
using MYSQL_RES = st_mysql_res;

using MYSQL_ROW = char**;

struct st_mysql_field;
using MYSQL_FIELD = st_mysql_field;

#endif

namespace mysqlxx
{

using UInt64 = uint64_t;
using Int64 = int64_t;
using UInt32 = uint32_t;
using Int32 = int32_t;

using MYSQL_LENGTH = unsigned long;
using MYSQL_LENGTHS = MYSQL_LENGTH *;
using MYSQL_FIELDS = MYSQL_FIELD *;

}
