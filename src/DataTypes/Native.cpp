#include <DataTypes/Native.h>
#include <Columns/ColumnDecimal.h>

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/DataTypeDateTime64.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeTime64.h>
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
    return data_type.isInt() || data_type.isFloat() || data_type.isEnum() || data_type.isDate32() || data_type.isDecimal()
        || data_type.isDateTime64();
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

    return (data_type.isInt() || data_type.isUInt()
            || data_type.isNativeFloat()
            || data_type.isDate() || data_type.isDate32()
            || data_type.isDateTime() || data_type.isDateTime64()
            || data_type.isTime() || data_type.isTime64()
            || data_type.isEnum() || data_type.isDecimal())
        && type.getSizeOfValueInMemory() <= MAX_NATIVE_INT_SIZE;
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
    if (data_type.isInt32() || data_type.isUInt32() || data_type.isDate32() || data_type.isDateTime() || data_type.isDecimal32() || data_type.isTime())
        return builder.getInt32Ty();
    if (data_type.isInt64() || data_type.isUInt64() || data_type.isDecimal64() || data_type.isDateTime64() || data_type.isTime64())
        return builder.getInt64Ty();
    if (data_type.isInt128() || data_type.isUInt128() || data_type.isDecimal128())
        return builder.getInt128Ty();
    if (data_type.isInt256() || data_type.isUInt256() || data_type.isDecimal256())
        return builder.getIntNTy(256);
    if (data_type.isFloat32())
        return builder.getFloatTy();
    if (data_type.isFloat64())
        return builder.getDoubleTy();
    if (data_type.isEnum8())
        return builder.getInt8Ty();
    if (data_type.isEnum16())
        return builder.getInt16Ty();
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid cast from {} to native type", type.getName());
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

    /// Handle scale conversion for DateTime/DateTime64/Time/Time64 types.
    /// When converting between types with different scales (e.g., DateTime with implicit
    /// scale 0 to DateTime64 with scale 3), we need to multiply/divide by the appropriate
    /// power of 10, not just cast the integer value.
    /// Note: Decimal types are NOT handled here because JIT-compiled aggregate functions
    /// (e.g., avg) already manage Decimal scale conversion themselves.
    {
        auto get_effective_scale = [](const DataTypePtr & type) -> std::optional<UInt32>
        {
            WhichDataType which(type);
            if (which.isDateTime() || which.isTime())
                return 0u;
            if (which.isDateTime64())
                return typeid_cast<const DataTypeDateTime64 *>(type.get())->getScale();
            if (which.isTime64())
                return typeid_cast<const DataTypeTime64 *>(type.get())->getScale();
            return std::nullopt;
        };

        auto from_scale = get_effective_scale(from_type);
        auto to_scale = get_effective_scale(to_type);

        if (from_scale && to_scale && *from_scale != *to_scale)
        {
            /// First widen/narrow the integer type if needed.
            if (from_native_type != to_native_type)
                value = b.CreateIntCast(value, to_native_type, typeIsSigned(*from_type));

            UInt32 scale_diff = (*to_scale > *from_scale) ? (*to_scale - *from_scale) : (*from_scale - *to_scale);
            unsigned bit_width = to_native_type->getIntegerBitWidth();
            llvm::APInt scale_factor(bit_width, 1);
            for (UInt32 i = 0; i < scale_diff; ++i)
                scale_factor *= 10;
            auto * scale_constant = llvm::ConstantInt::get(b.getContext(), scale_factor);

            if (*to_scale > *from_scale)
                value = b.CreateMul(value, scale_constant);
            else
                value = b.CreateSDiv(value, scale_constant);

            return value;
        }
    }

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

    auto get_numeric_constant = [&type]<typename T>(const IColumn & column_, size_t index_) -> llvm::Constant *
    {
        const auto & column_vector_decimal = assert_cast<const ColumnVectorOrDecimal<T> &>(column_);
        const auto & element = column_vector_decimal.getElement(index_);

        if constexpr (std::is_floating_point_v<T>)
        {
            return llvm::ConstantFP::get(type, element);
        }
        else if constexpr (is_integer<T>)
        {
            if constexpr (std::is_integral_v<T>)
                return llvm::ConstantInt::get(type, static_cast<uint64_t>(element), is_signed_v<T>);
            else
            {
                llvm::APInt value(type->getIntegerBitWidth(), element.items);
                return llvm::ConstantInt::get(type, value);
            }
        }
        else if constexpr (is_decimal<T>)
        {
            if constexpr (!is_over_big_decimal<T>)
                return llvm::ConstantInt::get(type, static_cast<uint64_t>(element.value), true);
            else
            {
                llvm::APInt value(type->getIntegerBitWidth(), element.value.items);
                return llvm::ConstantInt::get(type, value);
            }
        }
    };


    #define GET_NUMERIC_CONSTANT(TYPE, DTYPE) \
        if (column_data_type.is##TYPE()) \
        { \
            return get_numeric_constant.operator()<DTYPE>(column, index); \
        }

    GET_NUMERIC_CONSTANT(Float32, Float32)
    GET_NUMERIC_CONSTANT(Float64, Float64)
    GET_NUMERIC_CONSTANT(Int8, Int8)
    GET_NUMERIC_CONSTANT(Int16, Int16)
    GET_NUMERIC_CONSTANT(Int32, Int32)
    GET_NUMERIC_CONSTANT(Time, Int32)
    GET_NUMERIC_CONSTANT(Time64, Time64)
    GET_NUMERIC_CONSTANT(Int64, Int64)
    GET_NUMERIC_CONSTANT(UInt8, UInt8)
    GET_NUMERIC_CONSTANT(UInt16, UInt16)
    GET_NUMERIC_CONSTANT(UInt32, UInt32)
    GET_NUMERIC_CONSTANT(UInt64, UInt64)
    GET_NUMERIC_CONSTANT(Enum8, Int8)
    GET_NUMERIC_CONSTANT(Enum16, Int16)
    GET_NUMERIC_CONSTANT(Date, UInt16)
    GET_NUMERIC_CONSTANT(Date32, Int32)
    GET_NUMERIC_CONSTANT(DateTime, UInt32)
    GET_NUMERIC_CONSTANT(DateTime64, DateTime64)
    GET_NUMERIC_CONSTANT(Int128, Int128)
    GET_NUMERIC_CONSTANT(Int256, Int256)
    GET_NUMERIC_CONSTANT(UInt128, UInt128)
    GET_NUMERIC_CONSTANT(UInt256, UInt256)
    GET_NUMERIC_CONSTANT(Decimal32, Decimal32)
    GET_NUMERIC_CONSTANT(Decimal64, Decimal64)
    GET_NUMERIC_CONSTANT(Decimal128, Decimal128)
    GET_NUMERIC_CONSTANT(Decimal256, Decimal256)
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get native value for column with type {}", column_type->getName());

#undef GET_NUMERIC_CONSTANT
}

llvm::Constant * getNativeValue(llvm::IRBuilderBase & builder, const DataTypePtr & column_type, const Field & field)
{
    ColumnPtr column = column_type->createColumnConst(1, field);
    return getColumnNativeValue(builder, column_type, *column, 0);
}
}

#endif
