#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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

    auto monotonicity_result_type = recursiveRemoveLowCardinality(return_type);

    /// Monotonicity for CAST is determined by conversion to the nested target type.
    /// Nullable only wraps the conversion result and does not change the order of successfully converted values.
    /// We remove Nullable here so CAST(..., Nullable(T)) can reuse T monotonicity metadata in optimizeReadInOrder
    /// and KeyCondition function-chain analysis. This is not a problem because both of them still validate monotonicity
    /// on actual argument types/ranges using getMonotonicityForRange.
    /// We do not do this for accurateCastOrNull because failed conversions can produce NULL from non-NULL input values.
    /// For example, ordered Float64 values (1.0, 1.1, 1.2, 1.25, 1.3, 1.5) become
    /// (1.0, NULL, NULL, 1.25, NULL, 1.5) with accurateCastOrNull(..., 'Float32').
    /// This can violate monotonicity assumptions used by optimizeReadInOrder/KeyCondition
    /// and can produce incorrect ORDER BY results.
    if (cast_type != CastType::accurateOrNull)
        monotonicity_result_type = removeNullable(monotonicity_result_type);

    if (isEnum(arguments.front().type)
        && castTypeToEither<DataTypeEnum8, DataTypeEnum16>(monotonicity_result_type.get(), [&](auto & type)
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
        DataTypeString>(monotonicity_result_type.get(), [&](auto & type)
        {
            monotonicity = detail::FunctionTo<std::decay_t<decltype(type)>>::Type::Monotonic::get;
            return true;
        }))
    {
    }

    return std::make_unique<detail::FunctionCast>(
        context, name, std::move(monotonicity), data_types, return_type, diagnostic, cast_type, date_time_overflow_behavior);
}

}
