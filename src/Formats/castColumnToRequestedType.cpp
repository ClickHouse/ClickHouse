#include <Formats/castColumnToRequestedType.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Utils.h>
#include <Formats/insertNullAsDefaultIfNeeded.h>
#include <Interpreters/castColumn.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
}

namespace
{

void castImpl(
    ColumnWithTypeAndName & column,
    const DataTypePtr & requested_type,
    const RequestedTypeCastSettings & settings,
    size_t column_i,
    BlockMissingValues * block_missing_values);

ColumnPtr castRowsNotInNullMap(
    const ColumnWithTypeAndName & column,
    const DataTypePtr & requested_type,
    const NullMap & null_map,
    const RequestedTypeCastSettings & settings)
{
    IColumn::Filter visible_rows(null_map.size());
    for (size_t i = 0; i < null_map.size(); ++i)
        visible_rows[i] = !null_map[i];

    ColumnWithTypeAndName visible{column.column->filter(visible_rows, -1), column.type, column.name};
    castImpl(visible, requested_type, settings, 0, nullptr);

    auto converted = IColumn::mutate(visible.column->convertToFullColumnIfConst());
    const size_t default_index = converted->size();
    requested_type->insertDefaultInto(*converted);
    auto indexes = ColumnUInt64::create(null_map.size());
    auto & index_data = indexes->getData();
    size_t visible_index = 0;
    for (size_t i = 0; i < null_map.size(); ++i)
        index_data[i] = null_map[i] ? default_index : visible_index++;
    return converted->index(*indexes, 0);
}

/// Whether the tuples differ at most in `Nullable` elements of `from` that `to` requests without `Nullable`.
bool differsOnlyInNullableElements(const IDataType & from, const IDataType & to)
{
    const auto * from_tuple = typeid_cast<const DataTypeTuple *>(&from);
    const auto * to_tuple = typeid_cast<const DataTypeTuple *>(&to);
    if (!from_tuple || !to_tuple || from_tuple->getElementNames() != to_tuple->getElementNames())
        return false;
    const auto & from_elements = from_tuple->getElements();
    const auto & to_elements = to_tuple->getElements();
    for (size_t i = 0; i < from_elements.size(); ++i)
        if (!from_elements[i]->equals(*to_elements[i]) && !removeNullable(from_elements[i])->equals(*to_elements[i]))
            return false;
    return true;
}

/// Prepares the elements of named tuples that are requested, recursively; the cast that follows matches them by name.
/// A Nullable tuple of another type is converted with its NULL rows excluded, before applying the requested
/// nullability. Top-level NULL defaults also populate `block_missing_values` for column `DEFAULT` expressions.
ColumnWithTypeAndName prepareNullableTuplesForCast(
    ColumnWithTypeAndName from,
    const DataTypePtr & to,
    const RequestedTypeCastSettings & settings,
    size_t column_i = 0,
    BlockMissingValues * block_missing_values = nullptr)
{
    if (from.type->equals(*to))
        return from;

    if (const auto * from_null = typeid_cast<const DataTypeNullable *>(from.type.get()))
    {
        const auto & from_null_column = assert_cast<const ColumnNullable &>(*from.column);
        ColumnWithTypeAndName nested{from_null_column.getNestedColumnPtr(), from_null->getNestedType(), from.name};
        const DataTypePtr to_nested = removeNullable(to);
        if (isTuple(nested.type) && isTuple(to_nested))
        {
            /// Null structs contain undefined child values. Convert only visible rows, then restore the
            /// null rows in the destination type so text parsers never see substitute empty strings.
            const auto & null_map = from_null_column.getNullMapData();
            const bool has_nulls = !memoryIsZero(null_map.data(), 0, null_map.size());
            const bool strip_nullable = !to->isNullable() && (settings.null_as_default || settings.null_tuple_as_default);
            /// Removing the `Nullable` of elements for `null_as_default` converts no value of a NULL row.
            const bool same_values = nested.type->equals(*to_nested)
                || (settings.null_as_default && differsOnlyInNullableElements(*nested.type, *to_nested));
            if (has_nulls && !same_values)
            {
                nested.column = castRowsNotInNullMap(nested, to_nested, null_map, settings);
                nested.type = to_nested;
            }
            else if (has_nulls && strip_nullable)
                nested.column = from_null_column.getNestedColumnWithDefaultOnNull();
            else
                nested = prepareNullableTuplesForCast(nested, to_nested, settings);

            if (strip_nullable)
            {
                if (has_nulls && settings.null_as_default && block_missing_values)
                    block_missing_values->setBitsFromNullMap(column_i, null_map);
                return nested;
            }
        }
        else
            nested = prepareNullableTuplesForCast(nested, to_nested, settings);
        from.column = ColumnNullable::create(nested.column, from_null_column.getNullMapColumnPtr());
        from.type = std::make_shared<DataTypeNullable>(nested.type);
        return from;
    }
    if (const auto * to_null = typeid_cast<const DataTypeNullable *>(to.get()))
        return prepareNullableTuplesForCast(from, to_null->getNestedType(), settings);

    if (const auto * to_arr = typeid_cast<const DataTypeArray *>(to.get()))
    {
        const auto * from_arr = typeid_cast<const DataTypeArray *>(from.type.get());
        if (!from_arr)
            return from;
        const auto & from_arr_column = assert_cast<const ColumnArray &>(*from.column);
        auto prepared = prepareNullableTuplesForCast(
            {from_arr_column.getDataPtr(), from_arr->getNestedType(), from.name}, to_arr->getNestedType(), settings);
        from.column = ColumnArray::create(prepared.column, from_arr_column.getOffsetsPtr());
        from.type = std::make_shared<DataTypeArray>(prepared.type);
        return from;
    }
    if (const auto * to_map = typeid_cast<const DataTypeMap *>(to.get()))
    {
        const auto * from_map = typeid_cast<const DataTypeMap *>(from.type.get());
        if (!from_map)
            return from;
        const auto & from_map_column = assert_cast<const ColumnMap &>(*from.column);
        const auto & from_entries = from_map_column.getNestedData();
        auto prepared_key = prepareNullableTuplesForCast(
            {from_entries.getColumnPtr(0), from_map->getKeyType(), from.name}, to_map->getKeyType(), settings);
        auto prepared_value = prepareNullableTuplesForCast(
            {from_entries.getColumnPtr(1), from_map->getValueType(), from.name}, to_map->getValueType(), settings);
        from.column = ColumnMap::create(ColumnArray::create(
            ColumnTuple::create(Columns{prepared_key.column, prepared_value.column}), from_map_column.getNestedColumn().getOffsetsPtr()));
        from.type = std::make_shared<DataTypeMap>(prepared_key.type, prepared_value.type);
        return from;
    }
    if (const auto * to_tuple = typeid_cast<const DataTypeTuple *>(to.get()))
    {
        const auto * from_tuple = typeid_cast<const DataTypeTuple *>(from.type.get());
        if (!from_tuple)
            return from;

        const auto & tuple_column = assert_cast<const ColumnTuple &>(*from.column);
        const auto & source_types = from_tuple->getElements();
        const auto & target_types = to_tuple->getElements();
        const bool match_by_name = from_tuple->hasExplicitNames() && to_tuple->hasExplicitNames();
        if (!match_by_name && source_types.size() != target_types.size())
            return from;

        /// The target element that each source element is converted to.
        VectorWithMemoryTracking<std::optional<size_t>> target_positions(source_types.size());
        if (match_by_name)
        {
            auto source_positions = from_tuple->matchElementsByName(*to_tuple, settings.case_insensitive);
            for (size_t target_i = 0; target_i < source_positions.size(); ++target_i)
                if (source_positions[target_i])
                    target_positions[*source_positions[target_i]] = target_i;
        }
        else
        {
            for (size_t i = 0; i < source_types.size(); ++i)
                target_positions[i] = i;
        }

        Columns columns;
        DataTypes types;
        columns.reserve(source_types.size());
        types.reserve(source_types.size());
        for (size_t i = 0; i < source_types.size(); ++i)
        {
            if (!target_positions[i])
            {
                columns.push_back(tuple_column.getColumnPtr(i));
                types.push_back(source_types[i]);
                continue;
            }
            auto prepared = prepareNullableTuplesForCast(
                {tuple_column.getColumnPtr(i), source_types[i], from.name}, target_types[*target_positions[i]], settings);
            columns.push_back(std::move(prepared.column));
            types.push_back(std::move(prepared.type));
        }

        /// An empty tuple has no element columns to carry its row count.
        from.column = columns.empty() ? ColumnTuple::create(tuple_column.size()) : ColumnTuple::create(std::move(columns));
        if (match_by_name)
            from.type = std::make_shared<DataTypeTuple>(std::move(types), from_tuple->getElementNames());
        else if (to_tuple->hasExplicitNames())
            from.type = std::make_shared<DataTypeTuple>(std::move(types), to_tuple->getElementNames());
        else
            from.type = std::make_shared<DataTypeTuple>(std::move(types));
        return from;
    }
    return from;
}

void castImpl(
    ColumnWithTypeAndName & column,
    const DataTypePtr & requested_type,
    const RequestedTypeCastSettings & settings,
    size_t column_i,
    BlockMissingValues * block_missing_values)
{
    column = prepareNullableTuplesForCast(column, requested_type, settings, column_i, block_missing_values);
    const auto matching = settings.case_insensitive ? TupleElementsMatching::ByNameCaseInsensitive : TupleElementsMatching::ByName;
    if (settings.null_as_default)
        insertNullAsDefaultIfNeeded(
            column, {requested_type->createColumn(), requested_type, column.name}, column_i, block_missing_values, matching);
    column.column = castColumnMatchingTupleElementsByName(column, requested_type, settings.case_insensitive);
    column.type = requested_type;
}

void addConversionMessage(Exception & e, const String & column_name, const DataTypePtr & read_type, const DataTypePtr & requested_type)
{
    e.addMessage(fmt::format(
        "while converting column {} from type {} to type {}",
        backQuote(column_name), read_type->getName(), requested_type->getName()));
}

}

void castColumnToRequestedType(
    ColumnWithTypeAndName & column,
    const DataTypePtr & requested_type,
    const RequestedTypeCastSettings & settings,
    size_t column_i,
    BlockMissingValues * block_missing_values)
{
    if (column.type->equals(*requested_type))
        return;

    const DataTypePtr read_type = column.type;
    try
    {
        castImpl(column, requested_type, settings, column_i, block_missing_values);
    }
    catch (Exception & e)
    {
        addConversionMessage(e, column.name, read_type, requested_type);
        throw;
    }
}

void castColumnToRequestedType(
    ColumnWithTypeAndName & column,
    const DataTypePtr & requested_type,
    const NullMap & null_map,
    const RequestedTypeCastSettings & settings)
{
    if (column.type->equals(*requested_type))
        return;

    const DataTypePtr read_type = column.type;
    try
    {
        column.column = castRowsNotInNullMap(column, requested_type, null_map, settings);
        column.type = requested_type;
    }
    catch (Exception & e)
    {
        addConversionMessage(e, column.name, read_type, requested_type);
        throw;
    }
}

ColumnPtr createMissingColumn(
    const ColumnWithTypeAndName & header_column,
    size_t num_rows,
    bool allow_missing_columns,
    size_t column_i,
    BlockMissingValues * block_missing_values)
{
    if (!allow_missing_columns)
        throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", header_column.name);

    if (block_missing_values)
        block_missing_values->setBits(column_i, num_rows);
    return createColumnWithDefaults(*header_column.type, num_rows);
}

void matchTupleElementsByName(ColumnWithTypeAndName & column, const DataTypePtr & requested_type)
{
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(removeNullable(column.type).get());
    const auto * requested_tuple_type = typeid_cast<const DataTypeTuple *>(removeNullable(requested_type).get());
    if (!tuple_type || !requested_tuple_type || !tuple_type->hasExplicitNames() || !requested_tuple_type->hasExplicitNames())
        return;

    DataTypes element_types;
    element_types.reserve(requested_tuple_type->getElements().size());
    for (size_t i = 0; i < requested_tuple_type->getElements().size(); ++i)
    {
        auto pos = tuple_type->tryGetPositionByName(requested_tuple_type->getNameByPosition(i + 1));
        element_types.push_back(pos ? tuple_type->getElement(*pos) : requested_tuple_type->getElement(i));
    }

    DataTypePtr target_type = std::make_shared<DataTypeTuple>(element_types, requested_tuple_type->getElementNames());
    if (column.type->isNullable())
        target_type = std::make_shared<DataTypeNullable>(target_type);

    column.column = castColumnMatchingTupleElementsByName(column, target_type);
    column.type = target_type;
}

}
