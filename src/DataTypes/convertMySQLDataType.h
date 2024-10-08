#pragma once

#include <string>
#include <Core/MultiEnum.h>
#include <Parsers/IAST.h>
#include "IDataType.h"

namespace DB
{
enum class MySQLDataTypesSupport;

/// Convert MySQL type to ClickHouse data type.
DataTypePtr convertMySQLDataType(MultiEnum<MySQLDataTypesSupport> type_support, const std::string & mysql_data_type, bool is_nullable, bool is_unsigned, size_t length, size_t precision, size_t scale);

}
