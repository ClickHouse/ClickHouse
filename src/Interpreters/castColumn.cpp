#include <Interpreters/castColumn.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/NullableUtils.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Common/assert_cast.h>


namespace DB
{

static ColumnPtr castColumn(CastType cast_type, const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr)
{
    if (arg.type->equals(*type) && cast_type != CastType::accurateOrNull)
    {
        /// `equals` ignores the state version of `AggregateFunction` types, e.g. a version `0` state
        /// received from an older shard and consumed under a `AggregateFunction(1, uniq, UInt64)` header.
        /// The column must take the version of the target type, which it uses on a later round trip
        /// of its states through an arena.
        if (!haveSameAggregateStateVersions(*arg.type, *type))
            return relabelAggregateStateVersions(arg.column, type);
        return arg.column;
    }

    const auto from_name = arg.type->getName();
    const auto to_name = type->getName();
    ColumnsWithTypeAndName arguments
    {
        arg,
        {
            DataTypeString().createColumnConst(arg.column->size(), to_name),
            std::make_shared<DataTypeString>(),
            ""
        }
    };
    auto get_cast_func = [from = arg, to = type, cast_type]
    {
        return createInternalCast(from, to, cast_type, {}, nullptr);
    };

    FunctionBasePtr func_cast = cache ? cache->getOrSet(cast_type, from_name, to_name, std::move(get_cast_func)) : get_cast_func();

    if (cast_type == CastType::accurateOrNull)
        return func_cast->execute(arguments, makeNullable(type), arg.column->size(), /* dry_run = */ false);
    return func_cast->execute(arguments, type, arg.column->size(), /* dry_run = */ false);
}

ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache)
{
    return castColumn(CastType::nonAccurate, arg, type, cache);
}

ColumnPtr castColumnAccurate(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache)
{
    return castColumn(CastType::accurate, arg, type, cache);
}

ColumnPtr castColumnAccurateOrNull(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache)
{
    return castColumn(CastType::accurateOrNull, arg, type, cache);
}

ColumnPtr castColumnAccurateSkipNulls(
    const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache)
{
    chassert(!type->isNullable());

    const ColumnPtr null_map_column = getOuterNullMap(*arg.column);
    if (!null_map_column)
        return castColumnAccurate(arg, type, cache);

    /// A `Nullable` column converts through its nested column. Any other column keeps its NULLs in its own
    /// representation, e.g. the discriminators of a `Variant`, and converts as it is once the NULL rows are
    /// filtered out.
    ColumnWithTypeAndName values = arg;
    if (const auto * column_nullable = checkAndGetColumn<ColumnNullable>(arg.column.get()))
        values = {column_nullable->getNestedColumnPtr(), removeNullable(arg.type), arg.name};

    if (values.type->equals(*type))
        return values.column;

    const NullMap & null_map = assert_cast<const ColumnUInt8 &>(*null_map_column).getData();
    const size_t rows = arg.column->size();
    const size_t not_null_rows = rows - countBytesInFilter(null_map);
    if (not_null_rows == 0)
        return type->createColumn()->cloneResized(rows);

    if (not_null_rows == rows)
        return castColumnAccurate(values, type, cache);

    IColumn::Filter not_null(rows);
    for (size_t i = 0; i < rows; ++i)
        not_null[i] = !null_map[i];

    auto result = IColumn::mutate(castColumnAccurate(
        {values.column->filter(not_null, not_null_rows), values.type, arg.name}, type, cache));
    result->expand(not_null, false);
    return result;
}

ColumnPtr castColumnAccurateOrNullSkipNulls(
    const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache)
{
    chassert(!type->isNullable());

    const ColumnPtr null_map_column = getOuterNullMap(*arg.column);
    if (!null_map_column)
        return castColumnAccurateOrNull(arg, type, cache);

    /// A column of NULLs has no value to convert, and its cast could fail on the types alone, e.g. for
    /// tuples of different sizes.
    const NullMap & null_map = assert_cast<const ColumnUInt8 &>(*null_map_column).getData();
    const size_t rows = arg.column->size();
    if (countBytesInFilter(null_map) == rows)
        return makeNullable(type->createColumn()->cloneResized(rows));

    /// `accurateCastOrNull` reports a value that does not convert as NULL instead of throwing, so the whole
    /// column converts, and only the NULLs of its non-NULL rows are kept.
    auto result = IColumn::mutate(castColumnAccurateOrNull(arg, type, cache));
    auto & result_null_map = assert_cast<ColumnNullable &>(*result).getNullMapData();
    for (size_t i = 0; i < rows; ++i)
        result_null_map[i] &= !null_map[i];
    return result;
}

}
