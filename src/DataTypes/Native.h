#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_EMBEDDED_COMPILER
#    include <Common/Exception.h>

#    include <DataTypes/IDataType.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeFixedString.h>
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
    return data_type.isNativeInt() || data_type.isFloat();
}

static inline llvm::Type * toNativeType(llvm::IRBuilderBase & builder, const IDataType & type)
{
    WhichDataType data_type(type);

    if (data_type.isNullable())
    {
        const auto & data_type_nullable = static_cast<const DataTypeNullable&>(type);
        auto * wrapped = toNativeType(builder, *data_type_nullable.getNestedType());
        return wrapped ? llvm::StructType::get(wrapped, /* is null = */ builder.getInt1Ty()) : nullptr;
    }

    /// LLVM doesn't have unsigned types, it has unsigned instructions.
    if (data_type.isInt8() || data_type.isUInt8())
        return builder.getInt8Ty();
    else if (data_type.isInt16() || data_type.isUInt16() || data_type.isDate())
        return builder.getInt16Ty();
    else if (data_type.isInt32() || data_type.isUInt32() || data_type.isDateTime())
        return builder.getInt32Ty();
    else if (data_type.isInt64() || data_type.isUInt64())
        return builder.getInt64Ty();
    else if (data_type.isFloat32())
        return builder.getFloatTy();
    else if (data_type.isFloat64())
        return builder.getDoubleTy();
    else if (data_type.isFixedString())
    {
        const auto & data_type_fixed_string = static_cast<const DataTypeFixedString &>(type);
        return llvm::VectorType::get(builder.getInt8Ty(), data_type_fixed_string.getN());
    }

    return nullptr;
}

static inline bool canBeNativeType(const IDataType & type)
{
    WhichDataType data_type(type);

    if (data_type.isNullable())
    {
        const auto & data_type_nullable = static_cast<const DataTypeNullable&>(type);
        return canBeNativeType(*data_type_nullable.getNestedType());
    }

    return data_type.isNativeInt() || data_type.isNativeUInt() || data_type.isFloat() || data_type.isFixedString() || data_type.isDate();
}

static inline llvm::Type * toNativeType(llvm::IRBuilderBase & builder, const DataTypePtr & type)
{
    return toNativeType(builder, *type);
}

static inline llvm::Value * nativeBoolCast(llvm::IRBuilder<> & b, const DataTypePtr & from, llvm::Value * value)
{
    if (from->isNullable())
    {
        auto * inner = nativeBoolCast(b, removeNullable(from), b.CreateExtractValue(value, {0}));
        return b.CreateAnd(b.CreateNot(b.CreateExtractValue(value, {1})), inner);
    }
    auto * zero = llvm::Constant::getNullValue(value->getType());
    if (value->getType()->isIntegerTy())
        return b.CreateICmpNE(value, zero);
    if (value->getType()->isFloatingPointTy())
        return b.CreateFCmpONE(value, zero); /// QNaN is false

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot cast non-number {} to bool", from->getName());
}

static inline llvm::Value * nativeCast(llvm::IRBuilder<> & b, const DataTypePtr & from, llvm::Value * value, llvm::Type * to)
{
    auto * n_from = value->getType();

    if (n_from == to)
        return value;
    else if (n_from->isIntegerTy() && to->isFloatingPointTy())
        return typeIsSigned(*from) ? b.CreateSIToFP(value, to) : b.CreateUIToFP(value, to);
    else if (n_from->isFloatingPointTy() && to->isIntegerTy())
        return typeIsSigned(*from) ? b.CreateFPToSI(value, to) : b.CreateFPToUI(value, to);
    else if (n_from->isIntegerTy() && to->isIntegerTy())
        return b.CreateIntCast(value, to, typeIsSigned(*from));
    else if (n_from->isFloatingPointTy() && to->isFloatingPointTy())
        return b.CreateFPCast(value, to);

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot cast {} to requested type", from->getName());
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

}

#endif
