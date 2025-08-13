#pragma once

#include <DataTypes/IDataType.h>

namespace llvm
{
    class Value;
}

namespace DB
{

/// LLVM value with its data type
struct ValueWithType
{
    llvm::Value * value = nullptr;
    DataTypePtr type;

    ValueWithType() = default;
    ValueWithType(llvm::Value * value_, DataTypePtr type_)
        : value(value_)
        , type(std::move(type_))
    {}
};

}
