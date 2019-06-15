#pragma once

#include <Core/Types.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFixedString.h>

namespace DB
{

/// Convert data type to query. for example
/// DataTypeUInt8 -> ASTIdentifier(UInt8)
/// DataTypeNullable(DataTypeUInt8) -> ASTFunction(ASTIdentifier(UInt8))
ASTPtr dataTypeConvertToQuery(const DataTypePtr & data_type);

/// Convert MySQL type to ClickHouse data type.
DataTypePtr convertMySQLDataType(const String & mysql_data_type, bool is_nullable, bool is_unsigned, size_t length);

}
