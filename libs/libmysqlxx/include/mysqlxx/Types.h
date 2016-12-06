#pragma once

#include <string>
#include <mysql.h>
#include <Poco/Types.h>

#include <common/LocalDate.h>
#include <common/LocalDateTime.h>


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
