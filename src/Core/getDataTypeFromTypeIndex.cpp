#include "getDataTypeFromTypeIndex.h"
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeUUID.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

bool isSimpleDataType(TypeIndex type_index)
{
    DataTypePtr data_type;
    switch (type_index)
    {
        case TypeIndex::UInt8: [[fallthrough]];
        case TypeIndex::UInt16: [[fallthrough]];
        case TypeIndex::UInt32: [[fallthrough]];
        case TypeIndex::UInt64: [[fallthrough]];
        case TypeIndex::UInt128: [[fallthrough]];
        case TypeIndex::UInt256: [[fallthrough]];
        case TypeIndex::Int8: [[fallthrough]];
        case TypeIndex::Int16: [[fallthrough]];
        case TypeIndex::Int32: [[fallthrough]];
        case TypeIndex::Int64: [[fallthrough]];
        case TypeIndex::Int128: [[fallthrough]];
        case TypeIndex::Int256: [[fallthrough]];
        case TypeIndex::Float32: [[fallthrough]];
        case TypeIndex::Float64: [[fallthrough]];
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::Date32: [[fallthrough]];
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::UUID: [[fallthrough]];
        case TypeIndex::String:
            return true;
        default:
            return false;
    }
}

DataTypePtr getSimpleDataTypeFromTypeIndex(TypeIndex type_index)
{
    DataTypePtr data_type;
    switch (type_index)
    {
        case TypeIndex::UInt8:
            data_type = std::make_shared<DataTypeNumber<UInt8>>();
            break;
        case TypeIndex::UInt16:
            data_type = std::make_shared<DataTypeNumber<UInt16>>();
            break;
        case TypeIndex::UInt32:
            data_type = std::make_shared<DataTypeNumber<UInt32>>();
            break;
        case TypeIndex::UInt64:
            data_type = std::make_shared<DataTypeNumber<UInt64>>();
            break;
        case TypeIndex::UInt128:
            data_type = std::make_shared<DataTypeNumber<UInt128>>();
            break;
        case TypeIndex::UInt256:
            data_type = std::make_shared<DataTypeNumber<UInt256>>();
            break;
        case TypeIndex::Int8:
            data_type = std::make_shared<DataTypeNumber<Int8>>();
            break;
        case TypeIndex::Int16:
            data_type = std::make_shared<DataTypeNumber<Int16>>();
            break;
        case TypeIndex::Int32:
            data_type = std::make_shared<DataTypeNumber<Int32>>();
            break;
        case TypeIndex::Int64:
            data_type = std::make_shared<DataTypeNumber<Int64>>();
            break;
        case TypeIndex::Int128:
            data_type = std::make_shared<DataTypeNumber<Int128>>();
            break;
        case TypeIndex::Int256:
            data_type = std::make_shared<DataTypeNumber<Int256>>();
            break;
        case TypeIndex::Float32:
            data_type = std::make_shared<DataTypeFloat32>();
            break;
        case TypeIndex::Float64:
            data_type = std::make_shared<DataTypeFloat64>();
            break;
        case TypeIndex::Date:
            data_type = std::make_shared<DataTypeDate>();
            break;
        case TypeIndex::Date32:
            data_type = std::make_shared<DataTypeDate32>();
            break;
        case TypeIndex::DateTime:
            data_type = std::make_shared<DataTypeDateTime>();
            break;
        case TypeIndex::UUID:
            data_type = std::make_shared<DataTypeUUID>();
            break;
        case TypeIndex::String:
            data_type = std::make_shared<DataTypeString>();
            break;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported type index: {}", type_index);
    }
    return data_type;
}

}
