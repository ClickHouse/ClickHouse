#include <DataTypes/Native.h>

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/DataTypeNullable.h>
#    include <Columns/ColumnConst.h>
#    include <Columns/ColumnNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

bool typeIsSigned(const IDataType & type)
{
    WhichDataType data_type(type);
    return data_type.isNativeInt() || data_type.isFloat() || data_type.isEnum() || data_type.isDate32();
}

llvm::Type * toNullableType(llvm::IRBuilderBase & builder, llvm::Type * type)
{
    auto * is_null_type = builder.getInt1Ty();
    return llvm::StructType::get(type, is_null_type);
}

bool canBeNativeType(const IDataType & type)
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

bool canBeNativeType(const DataTypePtr & type)
{
    return canBeNativeType(*type);
}

llvm::Type * toNativeType(llvm::IRBuilderBase & builder, const IDataType & type)
{
    WhichDataType data_type(type);

    if (data_type.isNullable())
    {
        const auto & data_type_nullable = static_cast<const DataTypeNullable&>(type);
        auto * nested_type = toNativeType(builder, *data_type_nullable.getNestedType());
        return toNullableType(builder, nested_type);
    }

    /// LLVM doesn't have unsigned types, it has unsigned instructions.
    if (data_type.isInt8() || data_type.isUInt8())
        return builder.getInt8Ty();
    if (data_type.isInt16() || data_type.isUInt16() || data_type.isDate())
        return builder.getInt16Ty();
    if (data_type.isInt32() || data_type.isUInt32() || data_type.isDate32() || data_type.isDateTime())
        return builder.getInt32Ty();
    if (data_type.isInt64() || data_type.isUInt64())
        return builder.getInt64Ty();
    if (data_type.isFloat32())
        return builder.getFloatTy();
    if (data_type.isFloat64())
        return builder.getDoubleTy();
    if (data_type.isEnum8())
        return builder.getInt8Ty();
    if (data_type.isEnum16())
        return builder.getInt16Ty();

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid cast to native type");
}

llvm::Type * toNativeType(llvm::IRBuilderBase & builder, const DataTypePtr & type)
{
    return toNativeType(builder, *type);
}

llvm::Value * nativeBoolCast(llvm::IRBuilderBase & b, const DataTypePtr & from_type, llvm::Value * value)
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
        return b.CreateFCmpUNE(value, zero);

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot cast non-number {} to bool", from_type->getName());
}

llvm::Value * nativeBoolCast(llvm::IRBuilderBase & b, const ValueWithType & value_with_type)
{
    return nativeBoolCast(b, value_with_type.type, value_with_type.value);
}

llvm::Value * nativeCast(llvm::IRBuilderBase & b, const DataTypePtr & from_type, llvm::Value * value, const DataTypePtr & to_type)
{
    if (from_type->equals(*to_type))
    {
        return value;
    }
    if (from_type->isNullable() && to_type->isNullable())
    {
        auto * inner = nativeCast(b, removeNullable(from_type), b.CreateExtractValue(value, {0}), to_type);
        return b.CreateInsertValue(inner, b.CreateExtractValue(value, {1}), {1});
    }
    if (from_type->isNullable())
    {
        return nativeCast(b, removeNullable(from_type), b.CreateExtractValue(value, {0}), to_type);
    }
    if (to_type->isNullable())
    {
        auto * to_native_type = toNativeType(b, to_type);
        auto * inner = nativeCast(b, from_type, value, removeNullable(to_type));
        return b.CreateInsertValue(llvm::Constant::getNullValue(to_native_type), inner, {0});
    }
    auto * from_native_type = toNativeType(b, from_type);
    auto * to_native_type = toNativeType(b, to_type);

    if (from_native_type == to_native_type)
        return value;
    if (from_native_type->isIntegerTy() && to_native_type->isFloatingPointTy())
        return typeIsSigned(*from_type) ? b.CreateSIToFP(value, to_native_type) : b.CreateUIToFP(value, to_native_type);
    if (from_native_type->isFloatingPointTy() && to_native_type->isIntegerTy())
        return typeIsSigned(*to_type) ? b.CreateFPToSI(value, to_native_type) : b.CreateFPToUI(value, to_native_type);
    if (from_native_type->isIntegerTy() && from_native_type->isIntegerTy())
        return b.CreateIntCast(value, to_native_type, typeIsSigned(*from_type));
    if (to_native_type->isFloatingPointTy() && to_native_type->isFloatingPointTy())
        return b.CreateFPCast(value, to_native_type);

    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Invalid cast to native value from type {} to type {}",
        from_type->getName(),
        to_type->getName());
}

llvm::Value * nativeCast(llvm::IRBuilderBase & b, const ValueWithType & value, const DataTypePtr & to_type)
{
    return nativeCast(b, value.type, value.value, to_type);
}

llvm::Constant * getColumnNativeValue(llvm::IRBuilderBase & builder, const DataTypePtr & column_type, const IColumn & column, size_t index)
{
    if (const auto * constant = typeid_cast<const ColumnConst *>(&column))
        return getColumnNativeValue(builder, column_type, constant->getDataColumn(), 0);

    auto * type = toNativeType(builder, column_type);

    WhichDataType column_data_type(column_type);
    if (column_data_type.isNullable())
    {
        const auto & nullable_data_type = assert_cast<const DataTypeNullable &>(*column_type);
        const auto & nullable_column = assert_cast<const ColumnNullable &>(column);

        auto * value = getColumnNativeValue(builder, nullable_data_type.getNestedType(), nullable_column.getNestedColumn(), index);
        auto * is_null = llvm::ConstantInt::get(type->getContainedType(1), nullable_column.isNullAt(index));

        return llvm::ConstantStruct::get(static_cast<llvm::StructType *>(type), value, is_null);
    }
    if (column_data_type.isFloat32())
    {
        return llvm::ConstantFP::get(type, assert_cast<const ColumnVector<Float32> &>(column).getElement(index));
    }
    if (column_data_type.isFloat64())
    {
        return llvm::ConstantFP::get(type, assert_cast<const ColumnVector<Float64> &>(column).getElement(index));
    }
    if (column_data_type.isNativeUInt() || column_data_type.isDate() || column_data_type.isDateTime())
    {
        return llvm::ConstantInt::get(type, column.getUInt(index));
    }
    if (column_data_type.isNativeInt() || column_data_type.isEnum() || column_data_type.isDate32())
    {
        return llvm::ConstantInt::get(type, column.getInt(index));
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Cannot get native value for column with type {}",
        column_type->getName());
}

}

#endif
