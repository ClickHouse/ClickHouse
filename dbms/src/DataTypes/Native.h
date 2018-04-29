#pragma once

#include <Common/config.h>

#if USE_EMBEDDED_COMPILER

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>

#include <llvm/IR/IRBuilder.h>

namespace DB
{

template <typename... Ts>
static inline bool typeIsEither(const IDataType & type)
{
    return (typeid_cast<const Ts *>(&type) || ...);
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

static inline llvm::Constant * getNativeValue(llvm::Type * type, const IColumn * column, size_t i)
{
    if (!column || !type)
        return nullptr;
    if (auto * constant = typeid_cast<const ColumnConst *>(column))
        return getNativeValue(type, &constant->getDataColumn(), 0);
    if (auto * nullable = typeid_cast<const ColumnNullable *>(column))
    {
        auto * value = getNativeValue(type->getContainedType(0), &nullable->getNestedColumn(), i);
        auto * is_null = llvm::ConstantInt::get(type->getContainedType(1), nullable->isNullAt(i));
        return value ? llvm::ConstantStruct::get(static_cast<llvm::StructType *>(type), value, is_null) : nullptr;
    }
    if (type->isFloatTy())
        return llvm::ConstantFP::get(type, static_cast<const ColumnVector<Float32> *>(column)->getElement(i));
    if (type->isDoubleTy())
        return llvm::ConstantFP::get(type, static_cast<const ColumnVector<Float64> *>(column)->getElement(i));
    if (type->isIntegerTy())
        return llvm::ConstantInt::get(type, column->getUInt(i));
    /// TODO: if (type->isVectorTy())
    return nullptr;
}

}

#endif
