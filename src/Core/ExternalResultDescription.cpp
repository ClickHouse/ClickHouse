#include "ExternalResultDescription.h"
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}

void ExternalResultDescription::init(const Block & sample_block_)
{
    sample_block = sample_block_;

    types.reserve(sample_block.columns());

    for (auto & elem : sample_block)
    {
        /// If default value for column was not provided, use default from data type.
        if (elem.column->empty())
            elem.column = elem.type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();

        types.emplace_back(getValueTypeWithNullable(elem.type));
    }
}

ExternalResultDescription::TypeWithNullable ExternalResultDescription::getValueTypeWithNullable(const DataTypePtr & input_type)
{
    bool is_nullable = input_type->isNullable();
    DataTypePtr type_not_nullable = removeNullable(input_type);
    const IDataType * type = type_not_nullable.get();

    WhichDataType which(type);

    if (which.isUInt8())
        return {ValueType::vtUInt8, is_nullable};
    else if (which.isUInt16())
        return {ValueType::vtUInt16, is_nullable};
    else if (which.isUInt32())
        return {ValueType::vtUInt32, is_nullable};
    else if (which.isUInt64())
        return {ValueType::vtUInt64, is_nullable};
    else if (which.isInt8())
        return {ValueType::vtInt8, is_nullable};
    else if (which.isInt16())
        return {ValueType::vtInt16, is_nullable};
    else if (which.isInt32())
        return {ValueType::vtInt32, is_nullable};
    else if (which.isInt64())
        return {ValueType::vtInt64, is_nullable};
    else if (which.isFloat32())
        return {ValueType::vtFloat32, is_nullable};
    else if (which.isFloat64())
        return {ValueType::vtFloat64, is_nullable};
    else if (which.isString())
        return {ValueType::vtString, is_nullable};
    else if (which.isDate())
        return {ValueType::vtDate, is_nullable};
    else if (which.isDate32())
        return {ValueType::vtDate32, is_nullable};
    else if (which.isDateTime())
        return {ValueType::vtDateTime, is_nullable};
    else if (which.isUUID())
        return {ValueType::vtUUID, is_nullable};
    else if (which.isEnum8())
        return {ValueType::vtEnum8, is_nullable};
    else if (which.isEnum16())
        return {ValueType::vtEnum16, is_nullable};
    else if (which.isDateTime64())
        return {ValueType::vtDateTime64, is_nullable};
    else if (which.isDecimal32())
        return {ValueType::vtDecimal32, is_nullable};
    else if (which.isDecimal64())
        return {ValueType::vtDecimal64, is_nullable};
    else if (which.isDecimal128())
        return {ValueType::vtDecimal128, is_nullable};
    else if (which.isDecimal256())
        return {ValueType::vtDecimal256, is_nullable};
    else if (which.isArray())
        return {ValueType::vtArray, is_nullable};
    else if (which.isFixedString())
        return {ValueType::vtFixedString, is_nullable};
    else if (which.isTuple())
        return {ValueType::vtTuple, is_nullable};
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unsupported type {}", type->getName());
}

}
