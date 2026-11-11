#pragma once

#include "config.h"

#include <string>
#include <Core/MultiEnum.h>
#include <DataTypes/IDataType.h>

#if USE_MYSQL
#include <mysqlxx/Types.h>
#endif

namespace DB
{
enum class MySQLDataTypesSupport : uint8_t;

/// Convert MySQL type to ClickHouse data type.
DataTypePtr convertMySQLDataType(MultiEnum<MySQLDataTypesSupport> type_support, const std::string & mysql_data_type, bool is_nullable, bool is_unsigned, size_t length, size_t precision, size_t scale);

#if USE_MYSQL
/// Convert MySQL type (expressed as the MySQL C API's field) to ClickHouse data type.
/// Used to infer the structure of a result of an arbitrary query passed to MySQL.
/// `use_nulls` corresponds to the `external_table_functions_use_nulls` setting: when false, nullable MySQL
/// fields are not wrapped in `Nullable`, matching the table-name path.
DataTypePtr convertMySQLDataType(MultiEnum<MySQLDataTypesSupport> type_support, MYSQL_FIELD & field, bool use_nulls);
#endif

}
