#pragma once

#include "config_core.h"

#if USE_EMBEDDED_COMPILER
#    include <Common/Exception.h>

#    include <DataTypes/IDataType.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <Columns/ColumnConst.h>
#    include <Columns/ColumnNullable.h>
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wunused-parameter"

#    include <llvm/IR/IRBuilder.h>

#    pragma GCC diagnostic pop


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

static inline bool typeIsSigned(const IDataType & type)
{
    WhichDataType data_type(type);
    return data_type.isNativeInt() || data_type.isFloat() || data_type.isEnum();
}

static inline llvm::Type * toNativeType(llvm::IRBuilderBase & builder, const IDataType & type)
{
    WhichDataType data_type(type);

    if (data_type.isNullable())
    {
        const auto & data_type_nullable = static_cast<const DataTypeNullable&>(type);
        auto * wrapped = toNativeType(builder, *data_type_nullable.getNestedType());
        auto * is_null_type = builder.getInt1Ty();
        return wrapped ? llvm::StructType::get(wrapped, is_null_type) : nullptr;
    }

    /// LLVM doesn't have unsigned types, it has unsigned instructions.
    if (data_type.isInt8() || data_type.isUInt8())
        return builder.getInt8Ty();
    else if (data_type.isInt16() || data_type.isUInt16() || data_type.isDate())
        return builder.getInt16Ty();
    else if (data_type.isInt32() || data_type.isUInt32() || data_type.isDate32() || data_type.isDateTime())
        return builder.getInt32Ty();
    else if (data_type.isInt64() || data_type.isUInt64())
        return builder.getInt64Ty();
    else if (data_type.isFloat32())
        return builder.getFloatTy();
    else if (data_type.isFloat64())
        return builder.getDoubleTy();
    else if (data_type.isEnum8())
        return builder.getInt8Ty();
    else if (data_type.isEnum16())
        return builder.getInt16Ty();

    return nullptr;
}

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

    return nullptr;
}

template <typename Type>
static inline bool canBeNativeType()
{
    if constexpr (std::is_same_v<Type, Int8> || std::is_same_v<Type, UInt8>)
        return true;
    else if constexpr (std::is_same_v<Type, Int16> || std::is_same_v<Type, UInt16>)
        return true;
    else if constexpr (std::is_same_v<Type, Int32> || std::is_same_v<Type, UInt32>)
        return true;
    else if constexpr (std::is_same_v<Type, Int64> || std::is_same_v<Type, UInt64>)
        return true;
    else if constexpr (std::is_same_v<Type, Float32>)
        return true;
    else if constexpr (std::is_same_v<Type, Float64>)
        return true;

    return false;
}

static inline bool canBeNativeType(const IDataType & type)
{
    WhichDataType data_type(type);

    if (data_type.isNullable())
    {
        const auto & data_type_nullable = static_cast<const DataTypeNullable&>(type);
        return canBeNativeType(*data_type_nullable.getNestedType());
    }

    return data_type.isNativeInt() || data_type.isNativeUInt() || data_type.isFloat() || data_type.isDate()
        || data_type.isDate32() || data_type.isDateTime() || data_type.isEnum();
}

static inline llvm::Type * toNativeType(llvm::IRBuilderBase & builder, const DataTypePtr & type)
{
    return toNativeType(builder, *type);
}

static inline llvm::Value * nativeBoolCast(llvm::IRBuilder<> & b, const DataTypePtr & from_type, llvm::Value * value)
{
    if (from_type->isNullable())
    {
        auto * inner = nativeBoolCast(b, removeNullable(from_type), b.CreateExtractValue(value, {0}));
        return b.CreateAnd(b.CreateNot(b.CreateExtractValue(value, {1})), inner);
    }
    auto * zero = llvm::Constant::getNullValue(value->getType());

    if (value->getType()->isIntegerTy())
        return b.CreateICmpNE(value, zero);
    if (value->getType()->isFloatingPointTy())
        return b.CreateFCmpONE(value, zero); /// QNaN is false

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot cast non-number {} to bool", from_type->getName());
}

static inline llvm::Value * nativeCast(llvm::IRBuilder<> & b, const DataTypePtr & from, llvm::Value * value, llvm::Type * to_type)
{
    auto * from_type = value->getType();

    if (from_type == to_type)
        return value;
    else if (from_type->isIntegerTy() && to_type->isFloatingPointTy())
        return typeIsSigned(*from) ? b.CreateSIToFP(value, to_type) : b.CreateUIToFP(value, to_type);
    else if (from_type->isFloatingPointTy() && to_type->isIntegerTy())
        return typeIsSigned(*from) ? b.CreateFPToSI(value, to_type) : b.CreateFPToUI(value, to_type);
    else if (from_type->isIntegerTy() && to_type->isIntegerTy())
        return b.CreateIntCast(value, to_type, typeIsSigned(*from));
    else if (from_type->isFloatingPointTy() && to_type->isFloatingPointTy())
        return b.CreateFPCast(value, to_type);

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot cast {} to requested type", from->getName());
}

template <typename FromType>
static inline llvm::Value * nativeCast(llvm::IRBuilder<> & b, llvm::Value * value, llvm::Type * to_type)
{
    auto * from_type = value->getType();

    static constexpr bool from_type_is_signed = std::numeric_limits<FromType>::is_signed;

    if (from_type == to_type)
        return value;
    else if (from_type->isIntegerTy() && to_type->isFloatingPointTy())
        return from_type_is_signed ? b.CreateSIToFP(value, to_type) : b.CreateUIToFP(value, to_type);
    else if (from_type->isFloatingPointTy() && to_type->isIntegerTy())
        return from_type_is_signed ? b.CreateFPToSI(value, to_type) : b.CreateFPToUI(value, to_type);
    else if (from_type->isIntegerTy() && to_type->isIntegerTy())
        return b.CreateIntCast(value, to_type, from_type_is_signed);
    else if (from_type->isFloatingPointTy() && to_type->isFloatingPointTy())
        return b.CreateFPCast(value, to_type);

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot cast {} to requested type", TypeName<FromType>);
}

static inline llvm::Value * nativeCast(llvm::IRBuilder<> & b, const DataTypePtr & from, llvm::Value * value, const DataTypePtr & to)
{
    auto * n_to = toNativeType(b, to);

    if (value->getType() == n_to)
    {
        return value;
    }
    else if (from->isNullable() && to->isNullable())
    {
        auto * inner = nativeCast(b, removeNullable(from), b.CreateExtractValue(value, {0}), to);
        return b.CreateInsertValue(inner, b.CreateExtractValue(value, {1}), {1});
    }
    else if (from->isNullable())
    {
        return nativeCast(b, removeNullable(from), b.CreateExtractValue(value, {0}), to);
    }
    else if (to->isNullable())
    {
        auto * inner = nativeCast(b, from, value, removeNullable(to));
        return b.CreateInsertValue(llvm::Constant::getNullValue(n_to), inner, {0});
    }

    return nativeCast(b, from, value, n_to);
}

static inline std::pair<llvm::Value *, llvm::Value *> nativeCastToCommon(llvm::IRBuilder<> & b, const DataTypePtr & lhs_type, llvm::Value * lhs, const DataTypePtr & rhs_type, llvm::Value * rhs) /// NOLINT
{
    llvm::Type * common;

    bool lhs_is_signed = typeIsSigned(*lhs_type);
    bool rhs_is_signed = typeIsSigned(*rhs_type);

    if (lhs->getType()->isIntegerTy() && rhs->getType()->isIntegerTy())
    {
        /// if one integer has a sign bit, make sure the other does as well. llvm generates optimal code
        /// (e.g. uses overflow flag on x86) for (word size + 1)-bit integer operations.

        size_t lhs_bit_width = lhs->getType()->getIntegerBitWidth() + (!lhs_is_signed && rhs_is_signed);
        size_t rhs_bit_width = rhs->getType()->getIntegerBitWidth() + (!rhs_is_signed && lhs_is_signed);

        size_t max_bit_width = std::max(lhs_bit_width, rhs_bit_width);
        common = b.getIntNTy(max_bit_width);
    }
    else
    {
        /// TODO: Check
        /// (double, float) or (double, int_N where N <= double's mantissa width) -> double
        common = b.getDoubleTy();
    }

    auto * cast_lhs_to_common = nativeCast(b, lhs_type, lhs, common);
    auto * cast_rhs_to_common = nativeCast(b, rhs_type, rhs, common);

    return std::make_pair(cast_lhs_to_common, cast_rhs_to_common);
}

static inline llvm::Constant * getColumnNativeValue(llvm::IRBuilderBase & builder, const DataTypePtr & column_type, const IColumn & column, size_t index)
{
    if (const auto * constant = typeid_cast<const ColumnConst *>(&column))
    {
        return getColumnNativeValue(builder, column_type, constant->getDataColumn(), 0);
    }

    WhichDataType column_data_type(column_type);

    auto * type = toNativeType(builder, column_type);

    if (!type || column.size() <= index)
        return nullptr;

    if (column_data_type.isNullable())
    {
        const auto & nullable_data_type = assert_cast<const DataTypeNullable &>(*column_type);
        const auto & nullable_column = assert_cast<const ColumnNullable &>(column);

        auto * value = getColumnNativeValue(builder, nullable_data_type.getNestedType(), nullable_column.getNestedColumn(), index);
        auto * is_null = llvm::ConstantInt::get(type->getContainedType(1), nullable_column.isNullAt(index));

        return value ? llvm::ConstantStruct::get(static_cast<llvm::StructType *>(type), value, is_null) : nullptr;
    }
    else if (column_data_type.isFloat32())
    {
        return llvm::ConstantFP::get(type, assert_cast<const ColumnVector<Float32> &>(column).getElement(index));
    }
    else if (column_data_type.isFloat64())
    {
        return llvm::ConstantFP::get(type, assert_cast<const ColumnVector<Float64> &>(column).getElement(index));
    }
    else if (column_data_type.isNativeUInt() || column_data_type.isDate() || column_data_type.isDateTime())
    {
        return llvm::ConstantInt::get(type, column.getUInt(index));
    }
    else if (column_data_type.isNativeInt() || column_data_type.isEnum() || column_data_type.isDate32())
    {
        return llvm::ConstantInt::get(type, column.getInt(index));
    }

    return nullptr;
}

}

#endif
