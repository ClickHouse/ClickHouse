#include "ExternalResultDescription.h"
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
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

        bool is_nullable = elem.type->isNullable();
        DataTypePtr type_not_nullable = removeNullable(elem.type);
        const IDataType * type = type_not_nullable.get();

        if (typeid_cast<const DataTypeUInt8 *>(type))
            types.emplace_back(ValueType::vtUInt8, is_nullable);
        else if (typeid_cast<const DataTypeUInt16 *>(type))
            types.emplace_back(ValueType::vtUInt16, is_nullable);
        else if (typeid_cast<const DataTypeUInt32 *>(type))
            types.emplace_back(ValueType::vtUInt32, is_nullable);
        else if (typeid_cast<const DataTypeUInt64 *>(type))
            types.emplace_back(ValueType::vtUInt64, is_nullable);
        else if (typeid_cast<const DataTypeInt8 *>(type))
            types.emplace_back(ValueType::vtInt8, is_nullable);
        else if (typeid_cast<const DataTypeInt16 *>(type))
            types.emplace_back(ValueType::vtInt16, is_nullable);
        else if (typeid_cast<const DataTypeInt32 *>(type))
            types.emplace_back(ValueType::vtInt32, is_nullable);
        else if (typeid_cast<const DataTypeInt64 *>(type))
            types.emplace_back(ValueType::vtInt64, is_nullable);
        else if (typeid_cast<const DataTypeFloat32 *>(type))
            types.emplace_back(ValueType::vtFloat32, is_nullable);
        else if (typeid_cast<const DataTypeFloat64 *>(type))
            types.emplace_back(ValueType::vtFloat64, is_nullable);
        else if (typeid_cast<const DataTypeString *>(type))
            types.emplace_back(ValueType::vtString, is_nullable);
        else if (typeid_cast<const DataTypeDate *>(type))
            types.emplace_back(ValueType::vtDate, is_nullable);
        else if (typeid_cast<const DataTypeDateTime *>(type))
            types.emplace_back(ValueType::vtDateTime, is_nullable);
        else if (typeid_cast<const DataTypeUUID *>(type))
            types.emplace_back(ValueType::vtUUID, is_nullable);
        else if (typeid_cast<const DataTypeEnum8 *>(type))
            types.emplace_back(ValueType::vtString, is_nullable);
        else if (typeid_cast<const DataTypeEnum16 *>(type))
            types.emplace_back(ValueType::vtString, is_nullable);
        else if (typeid_cast<const DataTypeDateTime64 *>(type))
            types.emplace_back(ValueType::vtDateTime64, is_nullable);
        else if (typeid_cast<const DataTypeDecimal<Decimal32> *>(type))
            types.emplace_back(ValueType::vtDecimal32, is_nullable);
        else if (typeid_cast<const DataTypeDecimal<Decimal64> *>(type))
            types.emplace_back(ValueType::vtDecimal64, is_nullable);
        else if (typeid_cast<const DataTypeDecimal<Decimal128> *>(type))
            types.emplace_back(ValueType::vtDecimal128, is_nullable);
        else if (typeid_cast<const DataTypeDecimal<Decimal256> *>(type))
            types.emplace_back(ValueType::vtDecimal256, is_nullable);
        else
            throw Exception{"Unsupported type " + type->getName(), ErrorCodes::UNKNOWN_TYPE};
    }
}

}
