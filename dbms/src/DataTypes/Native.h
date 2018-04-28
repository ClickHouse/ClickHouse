#pragma once

#include <Common/config.h>

#if USE_EMBEDDED_COMPILER

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

#include <llvm/IR/IRBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

static llvm::Type * toNativeType(llvm::IRBuilderBase & builder, const DataTypePtr & type)
{
    if (auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        auto * wrapped = toNativeType(builder, nullable->getNestedType());
        return wrapped ? llvm::StructType::get(wrapped, /* is null = */ builder.getInt1Ty()) : nullptr;
    }
    /// LLVM doesn't have unsigned types, it has unsigned instructions.
    if (typeid_cast<const DataTypeInt8 *>(type.get()) || typeid_cast<const DataTypeUInt8 *>(type.get()))
        return builder.getInt8Ty();
    if (typeid_cast<const DataTypeInt16 *>(type.get()) || typeid_cast<const DataTypeUInt16 *>(type.get()))
        return builder.getInt16Ty();
    if (typeid_cast<const DataTypeInt32 *>(type.get()) || typeid_cast<const DataTypeUInt32 *>(type.get()))
        return builder.getInt32Ty();
    if (typeid_cast<const DataTypeInt64 *>(type.get()) || typeid_cast<const DataTypeUInt64 *>(type.get()))
        return builder.getInt64Ty();
    if (typeid_cast<const DataTypeFloat32 *>(type.get()))
        return builder.getFloatTy();
    if (typeid_cast<const DataTypeFloat64 *>(type.get()))
        return builder.getDoubleTy();
    return nullptr;
}

static llvm::Constant * getDefaultNativeValue(llvm::Type * type)
{
    if (type->isIntegerTy())
        return llvm::ConstantInt::get(type, 0);
    if (type->isFloatTy() || type->isDoubleTy())
        return llvm::ConstantFP::get(type, 0.0);
    /// else nullable
    auto * value = getDefaultNativeValue(type->getContainedType(0));
    auto * is_null = llvm::ConstantInt::get(type->getContainedType(1), 1);
    return llvm::ConstantStruct::get(static_cast<llvm::StructType *>(type), value, is_null);
}

}

#endif
