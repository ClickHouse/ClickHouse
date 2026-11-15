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

enum struct enum_field_types { MYSQL_TYPE_DECIMAL, MYSQL_TYPE_TINY, /// NOLINT
                        MYSQL_TYPE_SHORT, MYSQL_TYPE_LONG,
                        MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE,
                        MYSQL_TYPE_NULL, MYSQL_TYPE_TIMESTAMP,
                        MYSQL_TYPE_LONGLONG, MYSQL_TYPE_INT24,
                        MYSQL_TYPE_DATE, MYSQL_TYPE_TIME,
                        MYSQL_TYPE_DATETIME, MYSQL_TYPE_YEAR,
                        MYSQL_TYPE_NEWDATE, MYSQL_TYPE_VARCHAR,
                        MYSQL_TYPE_BIT };

#endif

namespace mysqlxx
{

using UInt64 = uint64_t;
using Int64 = int64_t;
using UInt32 = uint32_t;
using Int32 = int32_t;

using MYSQL_LENGTH = unsigned long; /// NOLINT
using MYSQL_LENGTHS = MYSQL_LENGTH *;
using MYSQL_FIELDS = MYSQL_FIELD *;

}
