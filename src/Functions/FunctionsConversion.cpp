#include <Functions/FunctionsConversion.h>

#if USE_EMBEDDED_COMPILER
#    include "DataTypes/Native.h"
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

namespace detail
{

UInt32 extractToDecimalScale(const ColumnWithTypeAndName & named_column)
{
    const auto * arg_type = named_column.type.get();
    bool ok = checkAndGetDataType<DataTypeUInt64>(arg_type)
        || checkAndGetDataType<DataTypeUInt32>(arg_type)
        || checkAndGetDataType<DataTypeUInt16>(arg_type)
        || checkAndGetDataType<DataTypeUInt8>(arg_type);
    if (!ok)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of toDecimal() scale {}", named_column.type->getName());

    Field field;
    named_column.column->get(0, field);
    return static_cast<UInt32>(field.safeGet<UInt32>());
}

ColumnUInt8::MutablePtr copyNullMap(ColumnPtr col)
{
    ColumnUInt8::MutablePtr null_map = nullptr;
    if (const auto * col_nullable = checkAndGetColumn<ColumnNullable>(col.get()))
    {
        null_map = ColumnUInt8::create();
        null_map->insertRangeFrom(col_nullable->getNullMapColumn(), 0, col_nullable->size());
    }
    return null_map;
}

}

FunctionBasePtr createFunctionBaseCast(
    ContextPtr context,
    const char * name,
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & return_type,
    std::optional<CastDiagnostic> diagnostic,
    CastType cast_type,
    FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior)
{
    DataTypes data_types(arguments.size());

    for (size_t i = 0; i < arguments.size(); ++i)
        data_types[i] = arguments[i].type;

    detail::FunctionCast::MonotonicityForRange monotonicity;

    if (isEnum(arguments.front().type)
        && castTypeToEither<DataTypeEnum8, DataTypeEnum16>(return_type.get(), [&](auto & type)
        {
            monotonicity = detail::FunctionTo<std::decay_t<decltype(type)>>::Type::Monotonic::get;
            return true;
        }))
    {
    }
    else if (castTypeToEither<
        DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64, DataTypeUInt128, DataTypeUInt256,
        DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64, DataTypeInt128, DataTypeInt256,
        DataTypeFloat32, DataTypeFloat64,
        DataTypeDate, DataTypeDate32, DataTypeDateTime, DataTypeDateTime64, DataTypeTime, DataTypeTime64,
        DataTypeString>(recursiveRemoveLowCardinality(return_type).get(), [&](auto & type)
        {
            monotonicity = detail::FunctionTo<std::decay_t<decltype(type)>>::Type::Monotonic::get;
            return true;
        }))
    {
    }

    return std::make_unique<detail::FunctionCast>(
        context, name, std::move(monotonicity), data_types, return_type, diagnostic, cast_type, date_time_overflow_behavior);
}


#if USE_EMBEDDED_COMPILER

namespace detail
{

bool castType(const IDataType * type, auto && f)
{
    using Types = TypeList<
        DataTypeUInt8,
        DataTypeUInt16,
        DataTypeUInt32,
        DataTypeUInt64,
        DataTypeUInt128,
        DataTypeUInt256,
        DataTypeInt8,
        DataTypeInt16,
        DataTypeInt32,
        DataTypeInt64,
        DataTypeInt128,
        DataTypeInt256,
        DataTypeFloat32,
        DataTypeFloat64,
        DataTypeDecimal32,
        DataTypeDecimal64,
        DataTypeDecimal128,
        DataTypeDecimal256,
        DataTypeDate,
        DataTypeDateTime,
        DataTypeFixedString,
        DataTypeString,
        DataTypeInterval>;
    return castTypeToEither(Types{}, type, std::forward<decltype(f)>(f));
}

template <typename F>
bool castBothTypes(const IDataType * left, const IDataType * right, F && f)
{
    return castType(left, [&](const auto & left_) { return castType(right, [&](const auto & right_) { return f(left_, right_); }); });
}

bool convertIsCompilableImpl(const DataTypes & types, const DataTypePtr & result_type)
{
    if (types.empty())
        return false;

    if (!canBeNativeType(types[0]) || !canBeNativeType(result_type))
        return false;

    return castBothTypes(
        types[0].get(),
        result_type.get(),
        [](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;

            if constexpr (IsDataTypeDecimalOrNumber<LeftDataType> && IsDataTypeDecimalOrNumber<RightDataType>)
            {
                if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeNumber<RightDataType>)
                    return true;
                else if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeDecimal<RightDataType>)
                    return true;
                else if constexpr (IsDataTypeDecimal<LeftDataType> && IsDataTypeNumber<RightDataType>)
                    return true;
            }
            return false;
        });
}

llvm::Value * convertCompileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & result_type)
{
    llvm::Value * result = nullptr;
    castBothTypes(
        arguments[0].type.get(),
        result_type.get(),
        [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            if constexpr (IsDataTypeDecimalOrNumber<LeftDataType> && IsDataTypeDecimalOrNumber<RightDataType>)
            {
                using LeftFieldType = typename LeftDataType::FieldType;
                using RightFieldType = typename RightDataType::FieldType;

                if (isBool(right.getPtr()))
                {
                    /// nativeBoolCast returns i1, but Bool (UInt8) needs i8
                    auto * bool_value = nativeBoolCast(builder, arguments[0]);
                    result = builder.CreateZExt(bool_value, builder.getInt8Ty());
                    return true;
                }

                if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeNumber<RightDataType>)
                {
                    result = nativeCast(builder, arguments[0], right.getPtr());
                    return true;
                }
                else if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeDecimal<RightDataType>)
                {
                    auto scale = right.getScale();
                    auto multiplier = DecimalUtils::scaleMultiplier<NativeType<RightFieldType>>(scale);
                    if constexpr (std::is_floating_point_v<LeftFieldType>)
                    {
                        /// left type is float and right type is decimal
                        auto * from_type = arguments[0].value->getType();
                        auto * from_value = arguments[0].value;
                        result = builder.CreateFMul(from_value, llvm::ConstantFP::get(from_type, static_cast<LeftFieldType>(multiplier)));
                        result = nativeCast(builder, left.getPtr(), result, right.getPtr());
                    }
                    else
                    {
                        /// left type is integer and right type is decimal
                        auto * from_value = nativeCast(builder, arguments[0], right.getPtr());
                        auto * multiplier_value = getNativeValue(builder, right.getPtr(), RightFieldType(multiplier));
                        result = builder.CreateMul(from_value, multiplier_value);
                    }
                    return true;
                }
                else if constexpr (IsDataTypeDecimal<LeftDataType> && IsDataTypeNumber<RightDataType>)
                {
                    auto scale = left.getScale();
                    auto divider = DecimalUtils::scaleMultiplier<NativeType<LeftFieldType>>(scale);
                    if constexpr (std::is_floating_point_v<RightFieldType>)
                    {
                        DataTypePtr double_type = std::make_shared<DataTypeFloat64>();
                        auto * from_value = nativeCast(builder, arguments[0], double_type);
                        auto * d = llvm::ConstantFP::get(builder.getDoubleTy(), static_cast<double>(divider));
                        result = nativeCast(builder, double_type, builder.CreateFDiv(from_value, d), right.getPtr());
                    }
                    else
                    {
                        llvm::Value * d = nullptr;
                        auto * from_type = arguments[0].value->getType();
                        if constexpr (std::is_integral_v<NativeType<LeftFieldType>>)
                            d = llvm::ConstantInt::get(from_type, static_cast<uint64_t>(divider), true);
                        else
                        {
                            llvm::APInt v(from_type->getIntegerBitWidth(), divider.items);
                            d = llvm::ConstantInt::get(from_type, v);
                        }

                        auto * from_value = arguments[0].value;
                        auto * whole_part = builder.CreateSDiv(from_value, d);
                        result = nativeCast(builder, left.getPtr(), whole_part, right.getPtr());
                    }
                    return true;
                }
            }

            return false;
        });

    return result;
}


bool FunctionCast::isCompilable() const
{
    if (getName() != "CAST" || argument_types.size() != 2)
        return false;

    const auto & input_type = argument_types[0];
    const auto & result_type = getResultType();
    auto denull_input_type = removeNullable(input_type);
    auto denull_result_type = removeNullable(result_type);
    if (!canBeNativeType(denull_input_type) || !canBeNativeType(denull_result_type))
        return false;

    return castBothTypes(denull_input_type.get(), denull_result_type.get(), [](const auto & left, const auto & right)
    {
        using LeftDataType = std::decay_t<decltype(left)>;
        using RightDataType = std::decay_t<decltype(right)>;
        if constexpr (IsDataTypeDecimalOrNumber<LeftDataType> && IsDataTypeDecimalOrNumber<RightDataType>)
        {
            if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeNumber<RightDataType>)
                return true;
            else if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeDecimal<RightDataType>)
                return true;
            else if constexpr (IsDataTypeDecimal<LeftDataType> && IsDataTypeNumber<RightDataType>)
                return true;
        }
        return false;
    });
}

llvm::Value * FunctionCast::compile(llvm::IRBuilderBase & builder, const ValuesWithType & arguments) const
{
    const auto & input_type = arguments[0].type;
    const auto & result_type = getResultType();

    llvm::Value * result_value = nullptr;
    llvm::Value * input_value = arguments[0].value;
    llvm::Value * input_isnull = nullptr;
    if (input_type->isNullable())
    {
        input_isnull = builder.CreateExtractValue(input_value, {1});
        input_value = builder.CreateExtractValue(input_value, {0});
    }

    auto denull_input_type = removeNullable(input_type);
    auto denull_result_type = removeNullable(result_type);

    castBothTypes(
        denull_input_type.get(),
        denull_result_type.get(),
        [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;

            if constexpr (IsDataTypeDecimalOrNumber<LeftDataType> && IsDataTypeDecimalOrNumber<RightDataType>)
            {
                using LeftFieldType = typename LeftDataType::FieldType;
                using RightFieldType = typename RightDataType::FieldType;

                if (isBool(right.getPtr()))
                {
                    /// nativeBoolCast returns i1, but Bool (UInt8) needs i8
                    auto * bool_value = nativeBoolCast(builder, left.getPtr(), input_value);
                    result_value = builder.CreateZExt(bool_value, builder.getInt8Ty());
                    return true;
                }

                if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeNumber<RightDataType>)
                {
                    result_value = nativeCast(builder, left.getPtr(), input_value, right.getPtr());
                    return true;
                }
                else if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeDecimal<RightDataType>)
                {
                    auto scale = right.getScale();
                    auto multiplier = DecimalUtils::scaleMultiplier<NativeType<RightFieldType>>(scale);
                    if constexpr (std::is_floating_point_v<LeftFieldType>)
                    {
                        /// left type is float and right type is decimal
                        auto * from_type = toNativeType(builder, left);
                        result_value = builder.CreateFMul(input_value, llvm::ConstantFP::get(from_type, static_cast<LeftFieldType>(multiplier)));
                        result_value = nativeCast(builder, left.getPtr(), result_value, right.getPtr());
                    }
                    else
                    {
                        /// left type is integer and right type is decimal
                        auto * from_value = nativeCast(builder, left.getPtr(), input_value, right.getPtr());
                        auto * multiplier_value = getNativeValue(builder, right.getPtr(), RightFieldType(multiplier));
                        result_value = builder.CreateMul(from_value, multiplier_value);
                    }
                    return true;
                }
                else if constexpr (IsDataTypeDecimal<LeftDataType> && IsDataTypeNumber<RightDataType>)
                {
                    auto scale = left.getScale();
                    auto divider = DecimalUtils::scaleMultiplier<NativeType<LeftFieldType>>(scale);
                    if constexpr (std::is_floating_point_v<RightFieldType>)
                    {
                        DataTypePtr double_type = std::make_shared<DataTypeFloat64>();
                        auto * from_value = nativeCast(builder, left.getPtr(), input_value, double_type);
                        auto * d = llvm::ConstantFP::get(builder.getDoubleTy(), static_cast<double>(divider));
                        result_value = nativeCast(builder, double_type, builder.CreateFDiv(from_value, d), right.getPtr());
                    }
                    else
                    {
                        llvm::Value * d = nullptr;
                        auto * from_type = toNativeType(builder, left.getPtr());
                        if constexpr (std::is_integral_v<NativeType<LeftFieldType>>)
                            d = llvm::ConstantInt::get(from_type, static_cast<uint64_t>(divider), true);
                        else
                        {
                            llvm::APInt v(from_type->getIntegerBitWidth(), divider.items);
                            d = llvm::ConstantInt::get(from_type, v);
                        }

                        auto * from_value = input_value;
                        auto * whole_part = builder.CreateSDiv(from_value, d);
                        result_value = nativeCast(builder, left.getPtr(), whole_part, right.getPtr());
                    }
                    return true;
                }
            }
            return false;
        });

        if (!result_value)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot compile CAST function for types {} -> {}",
                input_type->getName(),
                result_type->getName());

        if (result_type->isNullable())
        {
            llvm::Value * result_isnull = input_isnull ? input_isnull : llvm::ConstantInt::get(builder.getInt1Ty(), 0);
            auto * nullable_structure_type = toNativeType(builder, result_type);
            llvm::Value * nullable_structure_value = llvm::Constant::getNullValue(nullable_structure_type);
            nullable_structure_value = builder.CreateInsertValue(nullable_structure_value, result_value, {0});
            return builder.CreateInsertValue(nullable_structure_value, result_isnull, {1});
        }
        else
        {
            return result_value;
        }
}

}
#endif

}
