#include <Interpreters/castColumn.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/NullableUtils.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/DecimalFunctions.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <limits>


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

ColumnPtr getDateTime64CastLossMap(const ColumnWithTypeAndName & source, const DataTypePtr & target_type)
{
    if (source.type->equals(*target_type))
        return {};

    const auto from_type = removeLowCardinality(source.type);
    const auto to_type = removeLowCardinalityAndNullable(target_type);
    const auto column = source.column->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();

    if (from_type->isNullable())
    {
        const auto & nullable = assert_cast<const ColumnNullable &>(*column);
        /// An all-NULL source has no timestamp to check and needs no structural conversion.
        if (countBytesInFilter(nullable.getNullMapData()) == nullable.size())
            return {};

        auto loss_map = getDateTime64CastLossMap(
            {nullable.getNestedColumnPtr(), removeNullable(from_type), source.name}, to_type);
        if (!loss_map)
            return {};

        auto result = IColumn::mutate(std::move(loss_map));
        auto & values = assert_cast<ColumnUInt8 &>(*result).getData();
        const auto & nulls = nullable.getNullMapData();
        for (size_t row = 0; row < values.size(); ++row)
            values[row] &= !nulls[row];
        return result;
    }

    if (const auto * from_map = typeid_cast<const DataTypeMap *>(from_type.get()))
        return getDateTime64CastLossMap(
            {assert_cast<const ColumnMap &>(*column).getNestedColumnPtr(), from_map->getNestedTypeWithUnnamedTuple(), source.name},
            to_type);

    if (const auto * to_map = typeid_cast<const DataTypeMap *>(to_type.get()))
    {
        if (const auto * from_tuple = typeid_cast<const DataTypeTuple *>(from_type.get()))
        {
            /// A tuple-to-map cast converts the two arrays positionally into keys and values.
            chassert(from_tuple->getElements().size() == 2);
            const auto & elements = assert_cast<const ColumnTuple &>(*column).getColumns();
            ColumnPtr result;
            const auto & target_elements = to_map->getKeyValueTypes();
            for (size_t i = 0; i < 2; ++i)
                result = mergeNullMaps(std::move(result), getDateTime64CastLossMap(
                    {elements[i], from_tuple->getElements()[i], source.name}, std::make_shared<DataTypeArray>(target_elements[i])));
            return result;
        }
        return getDateTime64CastLossMap({column, from_type, source.name}, to_map->getNestedTypeWithUnnamedTuple());
    }

    if (const auto * from_tuple = typeid_cast<const DataTypeTuple *>(from_type.get()))
    {
        if (const auto * to_tuple = typeid_cast<const DataTypeTuple *>(to_type.get()))
        {
            const auto positions = getTupleCastElementPositions(*from_tuple, *to_tuple);
            const auto & elements = assert_cast<const ColumnTuple &>(*column).getColumns();
            ColumnPtr result;
            for (size_t i = 0; i < positions.size(); ++i)
            {
                if (positions[i])
                {
                    const size_t from_index = *positions[i];
                    result = mergeNullMaps(std::move(result), getDateTime64CastLossMap(
                        {elements[from_index], from_tuple->getElements()[from_index], source.name}, to_tuple->getElements()[i]));
                }
            }
            return result;
        }
    }

    if (const auto * from_array = typeid_cast<const DataTypeArray *>(from_type.get()))
    {
        if (const auto * to_array = typeid_cast<const DataTypeArray *>(to_type.get()))
        {
            const auto & array = assert_cast<const ColumnArray &>(*column);
            auto element_loss = getDateTime64CastLossMap(
                {array.getDataPtr(), from_array->getNestedType(), source.name}, to_array->getNestedType());
            if (!element_loss)
                return {};

            const auto & values = assert_cast<const ColumnUInt8 &>(*element_loss).getData();
            const auto & offsets = array.getOffsets();
            auto result = ColumnUInt8::create(array.size(), UInt8(0));
            auto & rows = result->getData();
            for (size_t row = 0; row < rows.size(); ++row)
                for (size_t element = offsets[row - 1]; element < offsets[row]; ++element)
                    rows[row] |= values[element];
            return result;
        }
    }

    const auto * datetime = typeid_cast<const DataTypeDateTime64 *>(from_type.get());
    if (!datetime)
        return {};

    UInt32 target_scale = 0;
    const WhichDataType target(to_type);
    if (target.isDateTime64() || target.isTime64() || target.isDecimal())
        target_scale = getDecimalScale(*to_type);
    else if (!target.isDateOrDate32() && !target.isDateTime() && !target.isTime() && !target.isInteger())
        return {};

    const UInt32 source_scale = datetime->getScale();
    if (target_scale >= source_scale && !target.isDateTime())
        return {};

    const Int64 divisor = DecimalUtils::scaleMultiplier<Int64>(source_scale > target_scale ? source_scale - target_scale : 0);
    const Int64 source_multiplier = datetime->getScaleMultiplier().value;
    const auto & values = assert_cast<const ColumnDecimal<DateTime64> &>(*column).getData();
    auto result = ColumnUInt8::create(values.size(), UInt8(0));
    auto & loss_map = result->getData();
    for (size_t row = 0; row < values.size(); ++row)
    {
        const Int64 value = values[row];
        const Int64 seconds = value / source_multiplier;
        loss_map[row] = value % divisor != 0
            || (target.isDateTime() && (value < 0 || seconds > std::numeric_limits<UInt32>::max()));
    }
    return result;
}

}
