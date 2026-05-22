#pragma once

#include "config.h"

#if USE_EMBEDDED_COMPILER
#    include <Common/Exception.h>
#    include <Core/ValueWithType.h>
#    include <DataTypes/IDataType.h>
#    include <llvm/IR/IRBuilder.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Returns true if type is signed, false otherwise
bool typeIsSigned(const IDataType & type);

/// Cast LLVM type to nullable LLVM type
llvm::Type * toNullableType(llvm::IRBuilderBase & builder, llvm::Type * type);

/// Returns true if type can be native LLVM type, false otherwise
bool canBeNativeType(const IDataType & type);

/// Returns true if type can be native LLVM type, false otherwise
bool canBeNativeType(const DataTypePtr & type);

template <typename Type>
static constexpr bool canBeNativeType()
{
    if constexpr (std::is_same_v<Type, Int8> || std::is_same_v<Type, UInt8>)
        return true;
    else if constexpr (std::is_same_v<Type, Int16> || std::is_same_v<Type, UInt16>)
        return true;
    else if constexpr (std::is_same_v<Type, Int32> || std::is_same_v<Type, UInt32>)
        return true;
    else if constexpr (std::is_same_v<Type, Int64> || std::is_same_v<Type, UInt64>)
        return true;
    else if constexpr (std::is_same_v<Type, Float32> || std::is_same_v<Type, Float64>)
        return true;

    return false;
}

/// Cast type to native LLVM type
llvm::Type * toNativeType(llvm::IRBuilderBase & builder, const IDataType & type);

/// Cast type to native LLVM type
llvm::Type * toNativeType(llvm::IRBuilderBase & builder, const DataTypePtr & type);

template <typename ToType>
static inline llvm::Type * toNativeType(llvm::IRBuilderBase & builder)
{
    if constexpr (std::is_same_v<ToType, Int8> || std::is_same_v<ToType, UInt8>)
        return builder.getInt8Ty();
    else if constexpr (std::is_same_v<ToType, Int16> || std::is_same_v<ToType, UInt16>)
        return builder.getInt16Ty();
    else if constexpr (std::is_same_v<ToType, Int32> || std::is_same_v<ToType, UInt32>)
        return builder.getInt32Ty();
    else if constexpr (std::is_same_v<ToType, Int64> || std::is_same_v<ToType, UInt64>)
        return builder.getInt64Ty();
    else if constexpr (std::is_same_v<ToType, Float32>)
        return builder.getFloatTy();
    else if constexpr (std::is_same_v<ToType, Float64>)
        return builder.getDoubleTy();

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid cast to native type");
}

template <typename ToType>
static inline DataTypePtr toNativeDataType()
{
    static_assert(std::is_same_v<ToType, Int8> || std::is_same_v<ToType, UInt8> ||
        std::is_same_v<ToType, Int16> || std::is_same_v<ToType, UInt16> ||
        std::is_same_v<ToType, Int32> || std::is_same_v<ToType, UInt32> ||
        std::is_same_v<ToType, Int64> || std::is_same_v<ToType, UInt64> ||
        std::is_same_v<ToType, Float32> || std::is_same_v<ToType, Float64>);
    return std::make_shared<DataTypeNumber<ToType>>();
}

/// Cast LLVM value with type to bool
llvm::Value * nativeBoolCast(llvm::IRBuilderBase & b, const DataTypePtr & from_type, llvm::Value * value);

/// Cast LLVM value with type to bool
llvm::Value * nativeBoolCast(llvm::IRBuilderBase & b, const ValueWithType & value_with_type);

/// Cast LLVM value with type to specified type
llvm::Value * nativeCast(llvm::IRBuilderBase & b, const DataTypePtr & from_type, llvm::Value * value, const DataTypePtr & to_type);

/// Cast LLVM value with type to specified type
llvm::Value * nativeCast(llvm::IRBuilderBase & b, const ValueWithType & value, const DataTypePtr & to_type);

template <typename FromType>
static inline llvm::Value * nativeCast(llvm::IRBuilderBase & b, llvm::Value * value, const DataTypePtr & to)
{
    auto native_data_type = toNativeDataType<FromType>();
    return nativeCast(b, native_data_type, value, to);
}

/// Get column value for specified index as LLVM constant
llvm::Constant * getColumnNativeValue(llvm::IRBuilderBase & builder, const DataTypePtr & column_type, const IColumn & column, size_t index);

}

#endif
