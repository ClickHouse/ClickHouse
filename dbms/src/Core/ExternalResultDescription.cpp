#include "ExternalResultDescription.h"
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>


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
            types.emplace_back(ValueType::UInt8, is_nullable);
        else if (typeid_cast<const DataTypeUInt16 *>(type))
            types.emplace_back(ValueType::UInt16, is_nullable);
        else if (typeid_cast<const DataTypeUInt32 *>(type))
            types.emplace_back(ValueType::UInt32, is_nullable);
        else if (typeid_cast<const DataTypeUInt64 *>(type))
            types.emplace_back(ValueType::UInt64, is_nullable);
        else if (typeid_cast<const DataTypeInt8 *>(type))
            types.emplace_back(ValueType::Int8, is_nullable);
        else if (typeid_cast<const DataTypeInt16 *>(type))
            types.emplace_back(ValueType::Int16, is_nullable);
        else if (typeid_cast<const DataTypeInt32 *>(type))
            types.emplace_back(ValueType::Int32, is_nullable);
        else if (typeid_cast<const DataTypeInt64 *>(type))
            types.emplace_back(ValueType::Int64, is_nullable);
        else if (typeid_cast<const DataTypeFloat32 *>(type))
            types.emplace_back(ValueType::Float32, is_nullable);
        else if (typeid_cast<const DataTypeFloat64 *>(type))
            types.emplace_back(ValueType::Float64, is_nullable);
        else if (typeid_cast<const DataTypeString *>(type))
            types.emplace_back(ValueType::String, is_nullable);
        else if (typeid_cast<const DataTypeDate *>(type))
            types.emplace_back(ValueType::Date, is_nullable);
        else if (typeid_cast<const DataTypeDateTime *>(type))
            types.emplace_back(ValueType::DateTime, is_nullable);
        else if (typeid_cast<const DataTypeUUID *>(type))
            types.emplace_back(ValueType::UUID, is_nullable);
        else
            throw Exception{"Unsupported type " + type->getName(), ErrorCodes::UNKNOWN_TYPE};
    }
}

}
