#include <Formats/castColumnToRequestedType.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>
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

/// Prepares `from` for the cast to `to`: the value of a NULL row is never converted, the row gets the default of `to`.
ColumnWithTypeAndName prepareForCast(
    ColumnWithTypeAndName from,
    const DataTypePtr & to,
    const RequestedTypeCastSettings & settings,
    size_t column_i = 0,
    BlockMissingValues * block_missing_values = nullptr)
{
    if (from.type->equals(*to))
        return from;

    const bool to_can_be_null = isNullableOrLowCardinalityNullable(to);
    if (const auto * from_lc = typeid_cast<const DataTypeLowCardinality *>(from.type.get());
        from_lc && from_lc->getDictionaryType()->isNullable() && !to_can_be_null && settings.null_as_default)
    {
        const auto & lc_column = assert_cast<const ColumnLowCardinality &>(*from.column);
        DataTypePtr dictionary_type = removeNullable(from_lc->getDictionaryType());
        if (dictionary_type->equals(*removeLowCardinality(to)))
        {
            if (block_missing_values)
                for (size_t i = 0; i < lc_column.size(); ++i)
                    if (lc_column.isNullAt(i))
                        block_missing_values->setBit(column_i, i);
            from.column = lc_column.cloneWithDefaultOnNull();
            from.type = std::make_shared<DataTypeLowCardinality>(std::move(dictionary_type));
            return from;
        }
        if (!lc_column.containsNull())
            return from;
        from.column = from.column->convertToFullColumnIfLowCardinality();
        from.type = from_lc->getDictionaryType();
    }

    if (const auto * from_null = typeid_cast<const DataTypeNullable *>(from.type.get()))
    {
        const auto & from_null_column = assert_cast<const ColumnNullable &>(*from.column);
        ColumnWithTypeAndName nested{from_null_column.getNestedColumnPtr(), from_null->getNestedType(), from.name};
        const DataTypePtr to_nested = removeNullable(to);
        const auto & null_map = from_null_column.getNullMapData();
        const bool has_nulls = !memoryIsZero(null_map.data(), 0, null_map.size());
        const bool strip_nullable = !to_can_be_null
            && (settings.null_as_default || (settings.null_tuple_as_default && isTuple(nested.type) && isTuple(to)));
        if (has_nulls && strip_nullable && settings.null_as_default && block_missing_values)
            block_missing_values->setBitsFromNullMap(column_i, null_map);

        /// The column zero under a NULL row is the default of `to` only for an equal type with a trivial default.
        /// `CAST` itself keeps the NULL rows of every type but a tuple.
        const bool convert_visible_rows = has_nulls
            && (strip_nullable ? !nested.type->equals(*to_nested) || !to_nested->isDefaultInsertTrivial()
                               : settings.allow_types_conversion && to_can_be_null && isTuple(nested.type)
                                   && !nested.type->equals(*to_nested));
        if (convert_visible_rows)
            return {castRowsNotInNullMap(nested, to, null_map, settings), to, from.name};

        if (strip_nullable)
        {
            if (has_nulls)
                nested.column = from_null_column.getNestedColumnWithDefaultOnNull();
            return prepareForCast(nested, to, settings);
        }

        nested = prepareForCast(nested, to_nested, settings);
        from.column = ColumnNullable::create(nested.column, from_null_column.getNullMapColumnPtr());
        from.type = std::make_shared<DataTypeNullable>(nested.type);
        return from;
    }
    /// A `LowCardinality(Nullable)` column keeps its NULL rows for a Nullable `to`, as `CAST` does.
    if (const auto * to_null = typeid_cast<const DataTypeNullable *>(to.get()))
        return from.type->lowCardinality() ? from : prepareForCast(from, to_null->getNestedType(), settings);

    if (const auto * to_arr = typeid_cast<const DataTypeArray *>(to.get()))
    {
        /// `CAST` converts a map as the array of its unnamed (key, value) tuples.
        if (const auto * from_map = typeid_cast<const DataTypeMap *>(from.type.get()); from_map && settings.allow_types_conversion)
            from = {assert_cast<const ColumnMap &>(*from.column).getNestedColumnPtr(), from_map->getNestedTypeWithUnnamedTuple(), from.name};
        const auto * from_arr = typeid_cast<const DataTypeArray *>(from.type.get());
        if (!from_arr)
            return from;
        const auto & from_arr_column = assert_cast<const ColumnArray &>(*from.column);
        auto prepared = prepareForCast(
            {from_arr_column.getDataPtr(), from_arr->getNestedType(), from.name}, to_arr->getNestedType(), settings);
        /// An unchanged array keeps its custom name, such as `LineString`, which `CAST` needs for the geometry promotions.
        if (prepared.column == from_arr_column.getDataPtr() && prepared.type == from_arr->getNestedType())
            return from;
        from.column = ColumnArray::create(prepared.column, from_arr_column.getOffsetsPtr());
        from.type = std::make_shared<DataTypeArray>(prepared.type);
        return from;
    }
    if (const auto * to_map = typeid_cast<const DataTypeMap *>(to.get()))
    {
        /// `CAST` builds a map from the (key, value) tuples of a map or an array, or from a tuple of a key array and a value array.
        const DataTypes key_value_types = to_map->getKeyValueTypes();
        if (isTuple(from.type))
            return prepareForCast(from, std::make_shared<DataTypeTuple>(DataTypes{
                std::make_shared<DataTypeArray>(key_value_types[0]), std::make_shared<DataTypeArray>(key_value_types[1])}), settings);
        const auto * from_map = typeid_cast<const DataTypeMap *>(from.type.get());
        if (from_map)
            from = {assert_cast<const ColumnMap &>(*from.column).getNestedColumnPtr(), from_map->getNestedType(), from.name};
        auto entries = prepareForCast(from, std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(key_value_types)), settings);
        if (!from_map)
            return entries;
        const auto & entry_type = assert_cast<const DataTypeArray &>(*entries.type).getNestedType();
        return {ColumnMap::create(entries.column), std::make_shared<DataTypeMap>(assert_cast<const DataTypeTuple &>(*entry_type).getElements()), from.name};
    }
    if (const auto * to_tuple = typeid_cast<const DataTypeTuple *>(to.get()))
    {
        const auto * from_tuple = typeid_cast<const DataTypeTuple *>(from.type.get());
        if (!from_tuple)
            return from;

        const auto & tuple_column = assert_cast<const ColumnTuple &>(*from.column);
        const auto & source_types = from_tuple->getElements();
        const auto & target_types = to_tuple->getElements();
        VectorWithMemoryTracking<std::optional<size_t>> source_positions;
        /// Without type conversion the column is only relabeled, which pairs the elements by position.
        if (settings.allow_types_conversion && from_tuple->hasExplicitNames() && to_tuple->hasExplicitNames())
            source_positions = from_tuple->matchElementsByName(*to_tuple, settings.case_insensitive);
        const bool match_by_name = !source_positions.empty()
            && (settings.tuple_elements_by_name || std::ranges::any_of(source_positions, [](const auto & pos) { return pos.has_value(); }));
        if (!match_by_name && source_types.size() != target_types.size())
            return from;

        VectorWithMemoryTracking<std::optional<size_t>> target_positions(source_types.size());
        if (match_by_name)
        {
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
            auto prepared = prepareForCast(
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
    /// `prepareForCast` expects the concrete column of each type, but `NativeReader` can give `Sparse` and `Replicated` ones.
    column.column = removeSpecialRepresentations(column.column);
    column = prepareForCast(column, requested_type, settings, column_i, block_missing_values);
    if (!settings.allow_types_conversion)
        column.column = recursiveLowCardinalityTypeConversion(column.column, column.type, requested_type);
    else if (settings.tuple_elements_by_name)
        column.column = castColumnMatchingTupleElementsByName(column, requested_type, settings.case_insensitive);
    else
        column.column = castColumn(column, requested_type);
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
    BlockMissingValues * block_missing_values,
    NestedColumnExtractHelper * nested)
{
    if (!allow_missing_columns)
        throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", header_column.name);

    if (block_missing_values)
        block_missing_values->setBits(column_i, num_rows);

    ColumnPtr on_path;
    for (size_t dot = header_column.name.rfind('.'); nested && !on_path && dot != String::npos && dot != 0;
         dot = header_column.name.rfind('.', dot - 1))
    {
        if (auto column = nested->extractColumn(header_column.name.substr(0, dot)))
            on_path = column->column->convertToFullColumnIfConst();
    }

    DataTypePtr type = header_column.type;
    Columns offsets;
    while (on_path && typeid_cast<const DataTypeArray *>(type.get()))
    {
        const auto * array = typeid_cast<const ColumnArray *>(on_path.get());
        if (!array)
            break;
        offsets.push_back(array->getOffsetsPtr());
        on_path = array->getDataPtr();
        type = assert_cast<const DataTypeArray &>(*type).getNestedType();
    }

    auto column = type->createColumn();
    type->insertManyDefaultsInto(*column, on_path ? on_path->size() : num_rows);
    ColumnPtr result = std::move(column);
    for (auto it = offsets.rbegin(); it != offsets.rend(); ++it)
        result = ColumnArray::create(result, *it);
    return result;
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
