#pragma once

#include <string>
#include <Core/MultiEnum.h>
#include <DataTypes/IDataType.h>

namespace DB
{
enum class MySQLDataTypesSupport : uint8_t;

/// Convert MySQL type to ClickHouse data type.
DataTypePtr convertMySQLDataType(MultiEnum<MySQLDataTypesSupport> type_support, const std::string & mysql_data_type, bool is_nullable, bool is_unsigned, size_t length, size_t precision, size_t scale);

}
