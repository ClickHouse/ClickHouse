#pragma once

#include "config.h"
#include <DataTypes/DataTypesDecimal.h>

#if USE_EMBEDDED_COMPILER
#    include <Common/Exception.h>
#    include <Core/ValueWithType.h>
#    include <DataTypes/IDataType.h>

namespace llvm
{
    class IRBuilderBase;
    class Type;
    class Value;
    class Constant;
}


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

/// LLVM supports up to 128-bit integers on x86_64 and AArch64
#define MAX_NATIVE_INT_SIZE 16

template <typename Type>
static constexpr bool canBeNativeType()
{
    if constexpr (std::is_same_v<Type, Float32> || std::is_same_v<Type, Float64>)
        return true;
    else if constexpr (is_integer<Type> && sizeof(Type) <= MAX_NATIVE_INT_SIZE)
        return true;
    else if constexpr (is_decimal<Type> && sizeof(Type) <= MAX_NATIVE_INT_SIZE)
        return true;
    else
        return false;
}

/// Cast type to native LLVM type
llvm::Type * toNativeType(llvm::IRBuilderBase & builder, const IDataType & type);

/// Cast type to native LLVM type
llvm::Type * toNativeType(llvm::IRBuilderBase & builder, const DataTypePtr & type);

/// Cast type to native LLVM type (template version, defined in Native.cpp)
template <typename ToType>
llvm::Type * toNativeType(llvm::IRBuilderBase & builder);

template <typename ToType>
static inline DataTypePtr toNativeDataType()
{
    if constexpr (std::is_same_v<ToType, Int8> || std::is_same_v<ToType, UInt8> ||
        std::is_same_v<ToType, Int16> || std::is_same_v<ToType, UInt16> ||
        std::is_same_v<ToType, Int32> || std::is_same_v<ToType, UInt32> ||
        std::is_same_v<ToType, Int64> || std::is_same_v<ToType, UInt64> ||
        std::is_same_v<ToType, Int128> || std::is_same_v<ToType, UInt128> ||
        std::is_same_v<ToType, Int256> || std::is_same_v<ToType, UInt256> ||
        std::is_same_v<ToType, Float32> || std::is_same_v<ToType, Float64>)
        return std::make_shared<DataTypeNumber<ToType>>();
    else if constexpr (std::is_same_v<ToType, DateTime64>)
        return std::make_shared<DataTypeDateTime64>(0);
    else if constexpr (std::is_same_v<ToType, Decimal32>)
        return createDecimalMaxPrecision<Decimal32>(0);
    else if constexpr (std::is_same_v<ToType, Decimal64>)
        return createDecimalMaxPrecision<Decimal64>(0);
    else if constexpr (std::is_same_v<ToType, Decimal128>)
        return createDecimalMaxPrecision<Decimal128>(0);
    else if constexpr (std::is_same_v<ToType, Decimal256>)
        return createDecimalMaxPrecision<Decimal256>(0);

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid cast to native data type");
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

/// Cast LLVM value with type to specified type with `Decimal` scale awareness.
/// Behaves identically to `nativeCast` for non-`Decimal` types but additionally
/// performs the `* 10^scale` / `/ 10^scale` adjustment when one side of the
/// conversion is a `Decimal` with a non-trivial scale.
///
/// Use this in non-aggregate function compilers (e.g. `if`, `multiIf`, ...)
/// where the analyzer may leave a non-`Decimal` literal branch unconverted and
/// the result type is `Decimal`. JIT-compiled aggregate functions like `avg`
/// MUST keep using plain `nativeCast` because they intentionally treat the
/// `Decimal` storage as a raw integer accumulator.
llvm::Value * nativeCastWithDecimalScale(llvm::IRBuilderBase & b, const DataTypePtr & from_type, llvm::Value * value, const DataTypePtr & to_type);
llvm::Value * nativeCastWithDecimalScale(llvm::IRBuilderBase & b, const ValueWithType & value, const DataTypePtr & to_type);

/// Get column value for specified index as LLVM constant
llvm::Constant * getColumnNativeValue(llvm::IRBuilderBase & builder, const DataTypePtr & column_type, const IColumn & column, size_t index);

/// Get value for specified field as LLVM constant
llvm::Constant * getNativeValue(llvm::IRBuilderBase & builder, const DataTypePtr & column_type, const Field & field);

}

#endif
