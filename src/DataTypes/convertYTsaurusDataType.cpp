#include "convertYTsaurusDataType.h"
#include <memory>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

/// https://ytsaurus.tech/docs/en/user-guide/storage/data-types
DataTypePtr convertYTsaurusDataType(const String & data_type, bool required)
{
    DataTypePtr data_type_ptr;
    if (data_type == "int64")
    {
        data_type_ptr = std::make_shared<DataTypeInt64>();
    }
    else if (data_type == "int32")
    {
        data_type_ptr = std::make_shared<DataTypeInt32>();
    }
    else if (data_type == "int16")
    {
        data_type_ptr = std::make_shared<DataTypeInt16>();
    }
    else if (data_type == "int8")
    {
        data_type_ptr = std::make_shared<DataTypeInt8>();
    }
    else if (data_type == "uint64")
    {
        data_type_ptr = std::make_shared<DataTypeUInt64>();
    }
    else if (data_type == "uint32")
    {
        data_type_ptr = std::make_shared<DataTypeUInt32>();
    }
    else if (data_type == "uint16")
    {
        data_type_ptr = std::make_shared<DataTypeUInt16>();
    }
    else if (data_type == "uint8")
    {
        data_type_ptr = std::make_shared<DataTypeUInt8>();
    }
    else if (data_type == "float")
    {
        data_type_ptr = std::make_shared<DataTypeFloat32>();
    }
    else if (data_type == "double")
    {
        data_type_ptr = std::make_shared<DataTypeFloat64>();
    }
    else if (data_type == "boolean")
    {
        data_type_ptr = std::make_shared<DataTypeUInt8>();
    }
    else if (data_type == "string")
    {
        data_type_ptr = std::make_shared<DataTypeString>();
    }
    else if (data_type == "utf8")
    {
        // ?
    }
    else if (data_type == "json")
    {
        // ?
    }
    else if (data_type == "uuid")
    {
        data_type_ptr = std::make_shared<DataTypeUUID>();
    }
    else if (data_type == "date32")
    {
        data_type_ptr = std::make_shared<DataTypeDate32>();
    }
    else if (data_type == "datetime64")
    {
        // data_type_ptr = std::make_shared<DataTypeDateTime64>(); - ?
    }
    else if (data_type == "timestamp64")
    {
        // data_type_ptr = std::make_shared<DataTypeTime64>(); - ?
    }
    else if (data_type == "interval64")
    {
        // data_type_ptr = std::make_shared<DataTypeInterval>(); - ?
    }
    else if (data_type == "any")
    {
        // ?
    }
    else if (data_type == "null")
    {
        // ?
    }
    else if (data_type == "void")
    {
        // ?
    }

    if (required)
    {
        return data_type_ptr;
    }

    auto nullable_data_type_ptr = std::make_shared<DataTypeNullable>(std::move(data_type_ptr));
    return nullable_data_type_ptr;
}
}
