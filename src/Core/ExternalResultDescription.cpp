#include <Core/ExternalResultDescription.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeCustomGeo.h>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}

ExternalResultDescription::ExternalResultDescription(const Block & sample_block_)
{
    init(sample_block_);
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

        bool is_nullable = elem.type->isNullable();
        DataTypePtr type_not_nullable = removeNullable(elem.type);
        const IDataType * type = type_not_nullable.get();

        if (dynamic_cast<const DataTypePointName *>(type->getCustomName()))
        {
            types.emplace_back(ValueType::vtPoint, is_nullable);
            continue;
        }

        WhichDataType which(type);

        if (which.isUInt8())
            types.emplace_back(ValueType::vtUInt8, is_nullable);
        else if (which.isUInt16())
            types.emplace_back(ValueType::vtUInt16, is_nullable);
        else if (which.isUInt32())
            types.emplace_back(ValueType::vtUInt32, is_nullable);
        else if (which.isUInt64())
            types.emplace_back(ValueType::vtUInt64, is_nullable);
        else if (which.isInt8())
            types.emplace_back(ValueType::vtInt8, is_nullable);
        else if (which.isInt16())
            types.emplace_back(ValueType::vtInt16, is_nullable);
        else if (which.isInt32())
            types.emplace_back(ValueType::vtInt32, is_nullable);
        else if (which.isInt64())
            types.emplace_back(ValueType::vtInt64, is_nullable);
        else if (which.isFloat32())
            types.emplace_back(ValueType::vtFloat32, is_nullable);
        else if (which.isFloat64())
            types.emplace_back(ValueType::vtFloat64, is_nullable);
        else if (which.isString())
            types.emplace_back(ValueType::vtString, is_nullable);
        else if (which.isDate())
            types.emplace_back(ValueType::vtDate, is_nullable);
        else if (which.isDate32())
            types.emplace_back(ValueType::vtDate32, is_nullable);
        else if (which.isDateTime())
            types.emplace_back(ValueType::vtDateTime, is_nullable);
        else if (which.isUUID())
            types.emplace_back(ValueType::vtUUID, is_nullable);
        else if (which.isEnum8())
            types.emplace_back(ValueType::vtEnum8, is_nullable);
        else if (which.isEnum16())
            types.emplace_back(ValueType::vtEnum16, is_nullable);
        else if (which.isDateTime64())
            types.emplace_back(ValueType::vtDateTime64, is_nullable);
        else if (which.isTime())
            types.emplace_back(ValueType::vtTime, is_nullable);
        else if (which.isTime64())
            types.emplace_back(ValueType::vtTime64, is_nullable);
        else if (which.isDecimal32())
            types.emplace_back(ValueType::vtDecimal32, is_nullable);
        else if (which.isDecimal64())
            types.emplace_back(ValueType::vtDecimal64, is_nullable);
        else if (which.isDecimal128())
            types.emplace_back(ValueType::vtDecimal128, is_nullable);
        else if (which.isDecimal256())
            types.emplace_back(ValueType::vtDecimal256, is_nullable);
        else if (which.isArray())
            types.emplace_back(ValueType::vtArray, is_nullable);
        else if (which.isFixedString())
            types.emplace_back(ValueType::vtFixedString, is_nullable);
        else
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unsupported type {}", type->getName());
    }
}

}
