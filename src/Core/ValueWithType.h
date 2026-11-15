#pragma once

namespace llvm
{
    class Value;
}

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

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
