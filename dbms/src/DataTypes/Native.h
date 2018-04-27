#pragma once

#include <Common/config.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

namespace llvm
{
    class IRBuilderBase;
    class Type;
}

#if USE_EMBEDDED_COMPILER
#include <llvm/IR/IRBuilder.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

static llvm::Type * toNativeType([[maybe_unused]] llvm::IRBuilderBase & builder, [[maybe_unused]] const DataTypePtr & type)
{
#if USE_EMBEDDED_COMPILER
    if (auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        auto * wrapped = toNativeType(builder, nullable->getNestedType());
        return wrapped ? llvm::PointerType::get(wrapped, 0) : nullptr;
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
#else
    throw Exception("JIT-compilation is disabled", ErrorCodes::NOT_IMPLEMENTED);
#endif
}

}
