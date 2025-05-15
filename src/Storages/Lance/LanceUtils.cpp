#include "LanceUtils.h"

#if USE_LANCE

#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_TYPE;
}

DataTypePtr lanceColumnTypeToDataType(lance::ColumnType type)
{
    switch (type)
    {
        case lance::ColumnType::Int8:
            return std::make_shared<DataTypeInt8>();
        case lance::ColumnType::UInt8:
            return std::make_shared<DataTypeUInt8>();
        case lance::ColumnType::Int16:
            return std::make_shared<DataTypeInt16>();
        case lance::ColumnType::UInt16:
            return std::make_shared<DataTypeUInt16>();
        case lance::ColumnType::Int32:
            return std::make_shared<DataTypeInt32>();
        case lance::ColumnType::UInt32:
            return std::make_shared<DataTypeUInt32>();
        case lance::ColumnType::Int64:
            return std::make_shared<DataTypeInt64>();
        case lance::ColumnType::UInt64:
            return std::make_shared<DataTypeUInt64>();
        case lance::ColumnType::Float32:
            return std::make_shared<DataTypeFloat32>();
        case lance::ColumnType::Float64:
            return std::make_shared<DataTypeFloat64>();
        case lance::ColumnType::String:
            return std::make_shared<DataTypeString>();
        default:
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown Lance type: {}", type);
    }
}

lance::ColumnType typeIndexToLanceColumnType(TypeIndex type)
{
    switch (type)
    {
        case TypeIndex::Int8:
            return lance::ColumnType::Int8;
        case TypeIndex::UInt8:
            return lance::ColumnType::UInt8;
        case TypeIndex::Int16:
            return lance::ColumnType::Int16;
        case TypeIndex::UInt16:
            return lance::ColumnType::UInt16;
        case TypeIndex::Int32:
            return lance::ColumnType::Int32;
        case TypeIndex::UInt32:
            return lance::ColumnType::UInt32;
        case TypeIndex::Int64:
            return lance::ColumnType::Int64;
        case TypeIndex::UInt64:
            return lance::ColumnType::UInt64;
        case TypeIndex::Float32:
            return lance::ColumnType::Float32;
        case TypeIndex::Float64:
            return lance::ColumnType::Float64;
        case TypeIndex::String:
            return lance::ColumnType::String;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported type for Lance engine: {}", type);
    }
}

lance::ColumnType dataTypePtrToLanceColumnType(DataTypePtr type)
{
    if (type->getColumnType() == TypeIndex::Nullable)
    {
        return dataTypePtrToLanceColumnType(assert_cast<const DataTypeNullable &>(*type).getNestedType());
    }
    return typeIndexToLanceColumnType(type->getColumnType());
}

} // namespace DB

#endif
