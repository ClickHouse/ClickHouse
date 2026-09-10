#include <Interpreters/castColumn.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
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
        return arg.column;

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
    const auto & column = assert_cast<const ColumnNullable &>(*arg.column);
    const auto & nested_type = assert_cast<const DataTypeNullable &>(*arg.type).getNestedType();
    chassert(!type->isNullable());

    const ColumnPtr & nested_column = column.getNestedColumnPtr();
    if (nested_type->equals(*type))
        return nested_column;

    const size_t rows = column.size();
    const NullMap & null_map = column.getNullMapData();
    const size_t not_null_rows = rows - countBytesInFilter(null_map);
    if (not_null_rows == 0)
        return type->createColumn()->cloneResized(rows);

    if (not_null_rows == rows)
        return castColumnAccurate({nested_column, nested_type, arg.name}, type, cache);

    IColumn::Filter not_null(rows);
    for (size_t i = 0; i < rows; ++i)
        not_null[i] = !null_map[i];

    auto result = IColumn::mutate(castColumnAccurate(
        {nested_column->filter(not_null, not_null_rows), nested_type, arg.name}, type, cache));
    result->expand(not_null, false);
    return result;
}

}
