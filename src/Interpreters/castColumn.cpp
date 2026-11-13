#include <Interpreters/castColumn.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>


namespace DB
{

static ColumnPtr castColumn(
    CastType cast_type,
    const ColumnWithTypeAndName & arg,
    const DataTypePtr & type,
    InternalCastFunctionCache * cache,
    bool fixed_string_to_string_strip_trailing_zeros = false)
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
    auto get_cast_func = [from = arg, to = type, cast_type, fixed_string_to_string_strip_trailing_zeros]
    {
        return createInternalCast(from, to, cast_type, {}, nullptr, fixed_string_to_string_strip_trailing_zeros);
    };

    FunctionBasePtr func_cast = cache
        ? cache->getOrSet(cast_type, from_name, to_name, fixed_string_to_string_strip_trailing_zeros, std::move(get_cast_func))
        : get_cast_func();

    if (cast_type == CastType::accurateOrNull)
        return func_cast->execute(arguments, makeNullable(type), arg.column->size(), /* dry_run = */ false);
    return func_cast->execute(arguments, type, arg.column->size(), /* dry_run = */ false);
}

ColumnPtr castColumn(
    const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache, bool fixed_string_to_string_strip_trailing_zeros)
{
    return castColumn(CastType::nonAccurate, arg, type, cache, fixed_string_to_string_strip_trailing_zeros);
}

ColumnPtr castColumnAccurate(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache)
{
    return castColumn(CastType::accurate, arg, type, cache);
}

ColumnPtr castColumnAccurateOrNull(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache)
{
    return castColumn(CastType::accurateOrNull, arg, type, cache);
}

}
