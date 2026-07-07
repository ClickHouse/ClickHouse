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
        DataTypeDate, DataTypeDate32, DataTypeDateTime, DataTypeDateTime64,
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

}
