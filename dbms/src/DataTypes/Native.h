#pragma once

#include <Common/config.h>

#if USE_EMBEDDED_COMPILER

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

#include <llvm/IR/IRBuilder.h>

#pragma GCC diagnostic pop


namespace DB
{

template <typename... Ts>
static inline bool typeIsEither(const IDataType & type)
{
    return (typeid_cast<const Ts *>(&type) || ...);
}

static inline bool typeIsSigned(const IDataType & type)
{
    return typeIsEither<
        DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
        DataTypeFloat32, DataTypeFloat64,
        DataTypeDate, DataTypeDateTime, DataTypeInterval
    >(type);
}

static inline llvm::Type * toNativeType(llvm::IRBuilderBase & builder, const IDataType & type)
{
    if (auto * nullable = typeid_cast<const DataTypeNullable *>(&type))
    {
        auto * wrapped = toNativeType(builder, *nullable->getNestedType());
        return wrapped ? llvm::StructType::get(wrapped, /* is null = */ builder.getInt1Ty()) : nullptr;
    }
    /// LLVM doesn't have unsigned types, it has unsigned instructions.
    if (typeIsEither<DataTypeInt8, DataTypeUInt8>(type))
        return builder.getInt8Ty();
    if (typeIsEither<DataTypeInt16, DataTypeUInt16, DataTypeDate>(type))
        return builder.getInt16Ty();
    if (typeIsEither<DataTypeInt32, DataTypeUInt32, DataTypeDateTime>(type))
        return builder.getInt32Ty();
    if (typeIsEither<DataTypeInt64, DataTypeUInt64, DataTypeInterval>(type))
        return builder.getInt64Ty();
    if (typeIsEither<DataTypeUUID>(type))
        return builder.getInt128Ty();
    if (typeIsEither<DataTypeFloat32>(type))
        return builder.getFloatTy();
    if (typeIsEither<DataTypeFloat64>(type))
        return builder.getDoubleTy();
    if (auto * fixed_string = typeid_cast<const DataTypeFixedString *>(&type))
        return llvm::VectorType::get(builder.getInt8Ty(), fixed_string->getN());
    return nullptr;
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
    throw Exception("Cannot cast non-number " + from->getName() + " to bool", ErrorCodes::NOT_IMPLEMENTED);
}

static inline llvm::Value * nativeCast(llvm::IRBuilder<> & b, const DataTypePtr & from, llvm::Value * value, llvm::Type * to)
{
    auto * n_from = value->getType();
    if (n_from == to)
        return value;
    if (n_from->isIntegerTy() && to->isFloatingPointTy())
        return typeIsSigned(*from) ? b.CreateSIToFP(value, to) : b.CreateUIToFP(value, to);
    if (n_from->isFloatingPointTy() && to->isIntegerTy())
        return typeIsSigned(*from) ? b.CreateFPToSI(value, to) : b.CreateFPToUI(value, to);
    if (n_from->isIntegerTy() && to->isIntegerTy())
        return b.CreateIntCast(value, to, typeIsSigned(*from));
    if (n_from->isFloatingPointTy() && to->isFloatingPointTy())
        return b.CreateFPCast(value, to);
    throw Exception("Cannot cast " + from->getName() + " to requested type", ErrorCodes::NOT_IMPLEMENTED);
}

static inline llvm::Value * nativeCast(llvm::IRBuilder<> & b, const DataTypePtr & from, llvm::Value * value, const DataTypePtr & to)
{
    auto * n_to = toNativeType(b, to);
    if (value->getType() == n_to)
        return value;
    if (from->isNullable() && to->isNullable())
    {
        auto * inner = nativeCast(b, removeNullable(from), b.CreateExtractValue(value, {0}), to);
        return b.CreateInsertValue(inner, b.CreateExtractValue(value, {1}), {1});
    }
    if (from->isNullable())
        return nativeCast(b, removeNullable(from), b.CreateExtractValue(value, {0}), to);
    if (to->isNullable())
    {
        auto * inner = nativeCast(b, from, value, removeNullable(to));
        return b.CreateInsertValue(llvm::Constant::getNullValue(n_to), inner, {0});
    }
    return nativeCast(b, from, value, n_to);
}

}

#endif
