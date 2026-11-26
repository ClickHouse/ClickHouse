#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCBlockInputFormat.h>

#if USE_ARROW

#include <Formats/FormatFactory.h>
#include <Processors/Port.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/Impl/ArrowGeoTypes.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCSchemaReader.h>
#include <Common/WKB.h>
#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WithFileSize.h>
#include <IO/copyData.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Block.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <Core/UUID.h>
#include <Formats/insertNullAsDefaultIfNeeded.h>
#include <Interpreters/castColumn.h>
#include <Common/quoteString.h>
#include <algorithm>
#include <functional>
#include <ranges>
#include <Common/assert_cast.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int THERE_IS_NO_COLUMN;
}

namespace
{
/// Compares dictionary value layouts and decoding semantics recursively.
bool dictionaryValueTypesEqual(const ArrowIPC::ArrowType & a, const ArrowIPC::ArrowType & b);

/// Struct and union element names identify dictionary value types; list and map container labels do not.
/// Metadata matters only where it changes value decoding: `isUUIDField`, and the `clickhouse.opaque` tag
/// naming the ClickHouse type a binary payload deserializes back into.
bool dictionaryValueFieldsEqual(const ArrowIPC::ArrowField & a, const ArrowIPC::ArrowField & b, bool compare_name)
{
    if (compare_name && a.name != b.name)
        return false;
    if (a.nullable != b.nullable)
        return false;
    if (a.dictionary != b.dictionary)
        return false;
    if (ArrowIPC::isUUIDField(a) != ArrowIPC::isUUIDField(b))
        return false;
    if (ArrowIPC::opaqueFieldTypeName(a) != ArrowIPC::opaqueFieldTypeName(b))
        return false;
    return dictionaryValueTypesEqual(a.type, b.type);
}

bool dictionaryValueTypesEqual(const ArrowIPC::ArrowType & a, const ArrowIPC::ArrowType & b)
{
    if (a.kind != b.kind || a.bit_width != b.bit_width || a.is_signed != b.is_signed
        || a.float_precision != b.float_precision || a.precision != b.precision || a.scale != b.scale
        || a.decimal_bit_width != b.decimal_bit_width || a.unit != b.unit || a.time_bit_width != b.time_bit_width
        || a.timezone != b.timezone || a.byte_width != b.byte_width || a.list_size != b.list_size
        || a.union_mode != b.union_mode || a.union_type_ids != b.union_type_ids
        || a.unsupported_type_name != b.unsupported_type_name
        || a.children.size() != b.children.size())
        return false;
    const bool compare_child_names = a.kind == ArrowIPC::TypeKind::Struct || a.kind == ArrowIPC::TypeKind::Union;
    for (size_t i = 0; i < a.children.size(); ++i)
    {
        if (!dictionaryValueFieldsEqual(a.children[i], b.children[i], compare_child_names))
            return false;
    }
    return true;
}
}

ArrowIPCBlockInputFormat::ArrowIPCBlockInputFormat(
    ReadBuffer & in_, SharedHeader header_, bool stream_, size_t max_block_size_, const FormatSettings & format_settings_)
    : IInputFormat(header_, &in_)
    , stream(stream_)
    , max_block_size(max_block_size_)
    , block_missing_values(getPort().getHeader().columns())
    , format_settings(format_settings_)
{
}

ArrowIPCBlockInputFormat::~ArrowIPCBlockInputFormat() = default;

void ArrowIPCBlockInputFormat::collectDictionaryFields(const ArrowIPC::ArrowFields & fields)
{
    for (const auto & field : fields)
    {
        if (field.dictionary)
        {
            /// The dictionary batch carries the plain value column: same type, but not dictionary-encoded,
            /// and nullable whatever the field declares. `Field.nullable` describes the encoded array — the
            /// index validity — while the dictionary values are an array of their own that may hold null
            /// entries; a field applies its own nullability when it materializes them (see
            /// `RecordBatchDecoder::decodeDictionary`). Fields sharing a dictionary id may thus differ in it.
            ArrowIPC::ArrowField value_field = field;
            value_field.dictionary.reset();
            value_field.nullable = true;
            const Int64 id = field.dictionary->id;
            auto it = dictionary_value_fields.find(id);
            if (it == dictionary_value_fields.end())
            {
                dictionary_value_fields.emplace(id, std::move(value_field));
            }
            /// Arrow dictionary ids are global within the file/stream: reusing one is valid only when every
            /// field describes the same dictionary value type. Reject a mismatch instead of silently
            /// overwriting the value schema — otherwise the shared `DictionaryBatch` would be decoded with
            /// one field's value type and materialized under another's (e.g. `Int32` values surfaced as a
            /// `LowCardinality(Date32)` column), bypassing validation or tripping type assertions.
            else if (!dictionaryValueFieldsEqual(it->second, value_field, /*compare_name=*/false))
            {
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Arrow IPC dictionary id {} is used by fields with different value types", id);
            }
        }
        collectDictionaryFields(field.type.children);
    }
}

namespace
{
/// When case-insensitive column matching is enabled, the named-tuple CAST that maps a decoded Arrow
/// struct onto the requested type matches element names case-sensitively, so differently-cased struct
/// fields would silently become default values (the library reader instead matches them). Rebuild the
/// decoded type so that every Tuple element name matching a requested element name case-insensitively is
/// renamed to the requested (exact) name; the subsequent CAST then matches. Only the type is rebuilt —
/// Tuple element names live in the type, not the column, so the column data is left untouched. Recurses
/// through Array / Map / Nullable / LowCardinality wrappers.
DataTypePtr alignStructFieldNamesCaseInsensitive(const DataTypePtr & from, const DataTypePtr & to)
{
    if (const auto * to_low = typeid_cast<const DataTypeLowCardinality *>(to.get()))
    {
        if (const auto * from_low = typeid_cast<const DataTypeLowCardinality *>(from.get()))
            return std::make_shared<DataTypeLowCardinality>(
                alignStructFieldNamesCaseInsensitive(from_low->getDictionaryType(), to_low->getDictionaryType()));
        return from;
    }
    if (const auto * to_null = typeid_cast<const DataTypeNullable *>(to.get()))
    {
        if (const auto * from_null = typeid_cast<const DataTypeNullable *>(from.get()))
            return std::make_shared<DataTypeNullable>(
                alignStructFieldNamesCaseInsensitive(from_null->getNestedType(), to_null->getNestedType()));
        return alignStructFieldNamesCaseInsensitive(from, to_null->getNestedType());
    }
    if (const auto * from_null = typeid_cast<const DataTypeNullable *>(from.get()))
        return std::make_shared<DataTypeNullable>(alignStructFieldNamesCaseInsensitive(from_null->getNestedType(), to));

    if (const auto * to_arr = typeid_cast<const DataTypeArray *>(to.get()))
    {
        if (const auto * from_arr = typeid_cast<const DataTypeArray *>(from.get()))
            return std::make_shared<DataTypeArray>(
                alignStructFieldNamesCaseInsensitive(from_arr->getNestedType(), to_arr->getNestedType()));
        return from;
    }
    if (const auto * to_map = typeid_cast<const DataTypeMap *>(to.get()))
    {
        if (const auto * from_map = typeid_cast<const DataTypeMap *>(from.get()))
            return std::make_shared<DataTypeMap>(
                alignStructFieldNamesCaseInsensitive(from_map->getKeyType(), to_map->getKeyType()),
                alignStructFieldNamesCaseInsensitive(from_map->getValueType(), to_map->getValueType()));
        return from;
    }
    if (const auto * to_tuple = typeid_cast<const DataTypeTuple *>(to.get()))
    {
        const auto * from_tuple = typeid_cast<const DataTypeTuple *>(from.get());
        if (!from_tuple)
            return from;

        /// Without names on both sides, `prepareArrowColumnForCast` matches elements by position (e.g. a struct with
        /// empty or repeated field names, read as an unnamed tuple), so descend the same way: named structs nested
        /// inside still need their names aligned. The source keeps its own names, or the lack of them.
        if (!from_tuple->hasExplicitNames() || !to_tuple->hasExplicitNames())
        {
            const auto & from_elems = from_tuple->getElements();
            const auto & to_elems = to_tuple->getElements();
            if (from_elems.size() != to_elems.size())
                return from;
            DataTypes new_elems;
            new_elems.reserve(from_elems.size());
            for (size_t i = 0; i < from_elems.size(); ++i)
                new_elems.push_back(alignStructFieldNamesCaseInsensitive(from_elems[i], to_elems[i]));
            return from_tuple->hasExplicitNames()
                ? std::make_shared<DataTypeTuple>(new_elems, from_tuple->getElementNames())
                : std::make_shared<DataTypeTuple>(new_elems);
        }

        const auto & from_elems = from_tuple->getElements();
        const auto & from_names = from_tuple->getElementNames();
        const auto & to_elems = to_tuple->getElements();
        const auto & to_names = to_tuple->getElementNames();

        UnorderedMapWithMemoryTracking<String, size_t> to_by_lower;
        to_by_lower.reserve(to_names.size());
        for (size_t i = 0; i < to_names.size(); ++i)
            to_by_lower.emplace(boost::to_lower_copy(to_names[i]), i); /// first occurrence wins on duplicates

        DataTypes new_elems;
        Strings new_names;
        new_elems.reserve(from_elems.size());
        new_names.reserve(from_names.size());
        for (size_t i = 0; i < from_elems.size(); ++i)
        {
            auto it = to_by_lower.find(boost::to_lower_copy(from_names[i]));
            if (it != to_by_lower.end())
            {
                new_names.push_back(to_names[it->second]);
                new_elems.push_back(alignStructFieldNamesCaseInsensitive(from_elems[i], to_elems[it->second]));
            }
            else
            {
                new_names.push_back(from_names[i]);
                new_elems.push_back(from_elems[i]);
            }
        }
        return std::make_shared<DataTypeTuple>(new_elems, new_names);
    }
    return from;
}

/// Match named struct fields recursively, dropping unrequested fields and defaulting missing ones.
/// Nullable tuples are converted with null rows excluded before applying the requested nullability.
/// Top-level null defaults also populate `missing_values` for column `DEFAULT` expressions.
ColumnWithTypeAndName prepareArrowColumnForCast(
    ColumnWithTypeAndName from, const DataTypePtr & to, const FormatSettings & settings,
    size_t column_index = 0, BlockMissingValues * missing_values = nullptr)
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
            if (has_nulls)
            {
                IColumn::Filter visible_rows(null_map.size());
                for (size_t i = 0; i < null_map.size(); ++i)
                    visible_rows[i] = !null_map[i];
                nested.column = nested.column->filter(visible_rows, -1);
            }
            nested = prepareArrowColumnForCast(std::move(nested), to_nested, settings);
            if (has_nulls)
            {
                if (settings.null_as_default)
                    insertNullAsDefaultIfNeeded(nested, {to_nested->createColumn(), to_nested, from.name}, 0, nullptr);
                auto converted = IColumn::mutate(castColumn(nested, to_nested)->convertToFullColumnIfConst());
                const size_t default_index = converted->size();
                to_nested->insertDefaultInto(*converted);
                auto indexes = ColumnUInt64::create(null_map.size());
                auto & index_data = indexes->getData();
                size_t visible_index = 0;
                for (size_t i = 0; i < null_map.size(); ++i)
                    index_data[i] = null_map[i] ? default_index : visible_index++;
                nested.column = converted->index(*indexes, 0);
                nested.type = to_nested;
            }
            if (!to->isNullable() && (settings.null_as_default || !settings.schema_inference_allow_nullable_tuple_type))
            {
                if (has_nulls && settings.null_as_default && missing_values)
                    missing_values->setBitsFromNullMap(column_index, null_map);
                return nested;
            }
        }
        else if (isTuple(nested.type) && !settings.schema_inference_allow_nullable_tuple_type && !to->isNullable()
            && !to->isLowCardinalityNullable())
        {
            /// A struct the decoder keeps nullable only because a subcolumn is taken from it too (see
            /// `RecordBatchDecoder::wrapsInNullable`), requested as a non-tuple type: without nullable tuples,
            /// the struct alone is decoded without its null map, and its null rows read as the values the
            /// decoder has already materialized for them. Drop the null map, keeping these values.
            return prepareArrowColumnForCast(std::move(nested), to, settings);
        }
        else
            nested = prepareArrowColumnForCast(std::move(nested), to_nested, settings);
        from.column = ColumnNullable::create(nested.column, from_null_column.getNullMapColumnPtr());
        from.type = std::make_shared<DataTypeNullable>(nested.type);
        return from;
    }
    if (const auto * to_null = typeid_cast<const DataTypeNullable *>(to.get()))
        return prepareArrowColumnForCast(std::move(from), to_null->getNestedType(), settings);

    if (const auto * to_arr = typeid_cast<const DataTypeArray *>(to.get()))
    {
        const auto * from_arr = typeid_cast<const DataTypeArray *>(from.type.get());
        if (!from_arr)
            return from;
        const auto & from_arr_column = assert_cast<const ColumnArray &>(*from.column);
        auto realigned = prepareArrowColumnForCast(
            {from_arr_column.getDataPtr(), from_arr->getNestedType(), from.name}, to_arr->getNestedType(), settings);
        from.column = ColumnArray::create(realigned.column, from_arr_column.getOffsetsPtr());
        from.type = std::make_shared<DataTypeArray>(realigned.type);
        return from;
    }
    if (const auto * to_map = typeid_cast<const DataTypeMap *>(to.get()))
    {
        const auto * from_map = typeid_cast<const DataTypeMap *>(from.type.get());
        if (!from_map)
            return from;
        const auto & from_map_column = assert_cast<const ColumnMap &>(*from.column);
        const auto & from_entries = from_map_column.getNestedData();
        auto realigned_key = prepareArrowColumnForCast(
            {from_entries.getColumnPtr(0), from_map->getKeyType(), from.name}, to_map->getKeyType(), settings);
        auto realigned_value = prepareArrowColumnForCast(
            {from_entries.getColumnPtr(1), from_map->getValueType(), from.name}, to_map->getValueType(), settings);
        from.column = ColumnMap::create(ColumnArray::create(
            ColumnTuple::create(Columns{realigned_key.column, realigned_value.column}), from_map_column.getNestedColumn().getOffsetsPtr()));
        from.type = std::make_shared<DataTypeMap>(realigned_key.type, realigned_value.type);
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
        const auto & target_names = to_tuple->getElementNames();
        const bool match_by_name = from_tuple->hasExplicitNames() && to_tuple->hasExplicitNames();
        if (!match_by_name && source_types.size() != target_types.size())
            return from;

        UnorderedMapWithMemoryTracking<String, size_t> source_positions;
        if (match_by_name)
        {
            const auto & source_names = from_tuple->getElementNames();
            source_positions.reserve(source_names.size());
            for (size_t i = 0; i < source_names.size(); ++i)
                source_positions.emplace(source_names[i], i);
        }

        Columns columns;
        DataTypes types;
        columns.reserve(target_types.size());
        types.reserve(target_types.size());
        for (size_t i = 0; i < target_types.size(); ++i)
        {
            size_t source_position = i;
            if (match_by_name)
            {
                auto it = source_positions.find(target_names[i]);
                if (it == source_positions.end())
                {
                    columns.push_back(target_types[i]->createColumnConstWithDefaultValue(tuple_column.size())
                        ->convertToFullColumnIfConst());
                    types.push_back(target_types[i]);
                    continue;
                }
                source_position = it->second;
            }
            auto prepared = prepareArrowColumnForCast(
                {tuple_column.getColumnPtr(source_position), source_types[source_position], from.name},
                target_types[i], settings);
            columns.push_back(std::move(prepared.column));
            types.push_back(std::move(prepared.type));
        }

        /// An empty tuple has no element columns to carry its row count.
        from.column = columns.empty() ? ColumnTuple::create(tuple_column.size()) : ColumnTuple::create(std::move(columns));
        from.type = to_tuple->hasExplicitNames()
            ? std::make_shared<DataTypeTuple>(std::move(types), target_names)
            : std::make_shared<DataTypeTuple>(std::move(types));
        return from;
    }
    return from;
}

}

namespace
{

/// How much decoding a field depends on a requested type, to choose the type kept for a normalized name that
/// several header columns with different types share (see `ArrowIPC::RequestedType::shared`), where
/// `mergeDecodingRequirements` cannot merge the types. A `Tuple` keeps the null map of a struct and the hints
/// of its fields, another composite the hints of its elements, a date or time type and a number decide how a
/// `date32` is read, and `Nullable` keeps the null map of a struct. The remaining types (`String`, `IPv6`) only
/// ask for raw-byte conversions, which are deferred for a shared name anyway.
int decodingRequirementRank(const DataTypePtr & type)
{
    const DataTypePtr stripped = ArrowIPC::stripHint(type);
    int rank = 0;
    if (isTuple(stripped))
        rank = 4;
    else if (isArray(stripped) || isMap(stripped))
        rank = 3;
    else if (isDateOrDate32(stripped) || isDateTimeOrDateTime64(stripped))
        rank = 2;
    else if (isNumber(stripped) || isDecimal(stripped))
        rank = 1;
    return rank * 2 + (type->isNullable() || type->isLowCardinalityNullable() ? 1 : 0);
}

/// The type kept for a normalized name requested with the types `kept` and `other` (see `prepareReader`). The
/// requirements of both are retained recursively: the elements of two named tuples are merged by name (the
/// elements only one of them has are kept as well), those of two unnamed tuples of the same size by position,
/// and the elements of two arrays and the keys and values of two maps likewise. So `Tuple(d String)` and
/// `Tuple(d Int32)` keep `Tuple(d Int32)`, which reads a `date32` as the day number. Elsewhere, the type that
/// decoding depends on more is kept (see `decodingRequirementRank`).
DataTypePtr mergeDecodingRequirements(const DataTypePtr & kept, const DataTypePtr & other, bool case_insensitive)
{
    if (kept->equals(*other))
        return kept;

    const DataTypePtr kept_stripped = ArrowIPC::stripHint(kept);
    const DataTypePtr other_stripped = ArrowIPC::stripHint(other);
    DataTypePtr merged;
    if (const auto * kept_array = typeid_cast<const DataTypeArray *>(kept_stripped.get()))
    {
        if (const auto * other_array = typeid_cast<const DataTypeArray *>(other_stripped.get()))
            merged = std::make_shared<DataTypeArray>(
                mergeDecodingRequirements(kept_array->getNestedType(), other_array->getNestedType(), case_insensitive));
    }
    else if (const auto * kept_map = typeid_cast<const DataTypeMap *>(kept_stripped.get()))
    {
        if (const auto * other_map = typeid_cast<const DataTypeMap *>(other_stripped.get()))
            merged = std::make_shared<DataTypeMap>(
                mergeDecodingRequirements(kept_map->getKeyType(), other_map->getKeyType(), case_insensitive),
                mergeDecodingRequirements(kept_map->getValueType(), other_map->getValueType(), case_insensitive));
    }
    else if (const auto * kept_tuple = typeid_cast<const DataTypeTuple *>(kept_stripped.get()))
    {
        const auto * other_tuple = typeid_cast<const DataTypeTuple *>(other_stripped.get());
        if (other_tuple && kept_tuple->hasExplicitNames() && other_tuple->hasExplicitNames())
        {
            DataTypes elements = kept_tuple->getElements();
            Strings names = kept_tuple->getElementNames();
            for (size_t i = 0; i < other_tuple->getElements().size(); ++i)
            {
                const auto & other_element = other_tuple->getElement(i);
                const auto & other_name = other_tuple->getElementNames()[i];
                if (const auto position = kept_tuple->tryGetPositionByName(other_name, case_insensitive))
                {
                    elements[*position] = mergeDecodingRequirements(elements[*position], other_element, case_insensitive);
                }
                else
                {
                    elements.push_back(other_element);
                    names.push_back(other_name);
                }
            }
            merged = std::make_shared<DataTypeTuple>(std::move(elements), std::move(names));
        }
        else if (other_tuple && !kept_tuple->hasExplicitNames() && !other_tuple->hasExplicitNames()
            && kept_tuple->getElements().size() == other_tuple->getElements().size())
        {
            DataTypes elements(kept_tuple->getElements().size());
            for (size_t i = 0; i < elements.size(); ++i)
                elements[i] = mergeDecodingRequirements(kept_tuple->getElement(i), other_tuple->getElement(i), case_insensitive);
            merged = std::make_shared<DataTypeTuple>(std::move(elements));
        }
        /// A `Nullable` tuple keeps the null map of the struct.
        if (merged && (kept->isNullable() || other->isNullable()))
            merged = std::make_shared<DataTypeNullable>(merged);
    }

    if (merged)
        return merged;
    return decodingRequirementRank(other) > decodingRequirementRank(kept) ? other : kept;
}

/// A field the decoder decodes at a dotted path (see `RecordBatchDecoder::structFieldPath`), and the number of
/// lists above it.
struct DecodedFieldAtPath
{
    const ArrowIPC::ArrowField * field = nullptr;
    size_t list_depth = 0;
};

/// Collects the fields decoded at the dotted `path`, searched at and below `field` decoded at `field_path`.
void collectDecodedFields(
    const ArrowIPC::ArrowField & field, const String & field_path, size_t list_depth, const String & path,
    bool case_insensitive, VectorWithMemoryTracking<DecodedFieldAtPath> & found)
{
    using ArrowIPC::TypeKind;
    if (field_path == path)
    {
        found.push_back({&field, list_depth});
        return;
    }
    if (!path.starts_with(field_path))
        return;
    const ArrowIPC::ArrowType & type = field.type;
    switch (type.kind)
    {
        case TypeKind::List:
        case TypeKind::LargeList:
        case TypeKind::FixedSizeList:
        case TypeKind::ListView:
        case TypeKind::LargeListView:
        case TypeKind::Map:
            collectDecodedFields(type.children.at(0), field_path, list_depth + 1, path, case_insensitive, found);
            break;
        case TypeKind::RunEndEncoded:
            collectDecodedFields(type.children.at(1), field_path, list_depth, path, case_insensitive, found);
            break;
        case TypeKind::Struct:
            for (size_t i = 0; i < type.children.size(); ++i)
            {
                String child_name = type.map_entries ? (i == 0 ? "keys" : "values")
                    : (type.positional_fields ? std::to_string(i + 1) : type.children[i].name);
                if (case_insensitive)
                    boost::to_lower(child_name);
                collectDecodedFields(type.children[i], field_path + "." + child_name, list_depth, path, case_insensitive, found);
            }
            break;
        default:
            break;
    }
}

/// `hint`, a requested type of `field`, with each unnamed `Tuple` of a struct with named fields given the names
/// of the fields it is matched with by position, and each named `Tuple` of a struct with positional fields
/// stripped of its names, recursively. The decoder matches the elements of a `Tuple` with the fields of a struct
/// in the same way (see `RecordBatchDecoder::structFieldHint`), so `mergeDecodingRequirements` can then merge
/// a named and an unnamed `Tuple` element by element.
DataTypePtr nameTuplesAsDecoded(const DataTypePtr & hint, const ArrowIPC::ArrowField & field, bool case_insensitive)
{
    using ArrowIPC::TypeKind;
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(hint.get()))
        return std::make_shared<DataTypeNullable>(nameTuplesAsDecoded(nullable->getNestedType(), field, case_insensitive));

    const ArrowIPC::ArrowType & type = field.type;
    switch (type.kind)
    {
        case TypeKind::List:
        case TypeKind::LargeList:
        case TypeKind::FixedSizeList:
        case TypeKind::ListView:
        case TypeKind::LargeListView:
            if (const auto * array = typeid_cast<const DataTypeArray *>(hint.get()))
                return std::make_shared<DataTypeArray>(nameTuplesAsDecoded(array->getNestedType(), type.children.at(0), case_insensitive));
            return hint;
        case TypeKind::Map:
        {
            const auto * map = typeid_cast<const DataTypeMap *>(hint.get());
            const auto & entries = type.children.at(0).type.children;
            if (!map || entries.size() != 2)
                return hint;
            return std::make_shared<DataTypeMap>(
                nameTuplesAsDecoded(map->getKeyType(), entries[0], case_insensitive),
                nameTuplesAsDecoded(map->getValueType(), entries[1], case_insensitive));
        }
        case TypeKind::RunEndEncoded:
            return nameTuplesAsDecoded(hint, type.children.at(1), case_insensitive);
        case TypeKind::Struct:
        {
            const auto * tuple = typeid_cast<const DataTypeTuple *>(hint.get());
            if (!tuple)
                return hint;
            const auto & children = type.children;
            if (type.positional_fields || type.map_entries)
            {
                DataTypes elements = tuple->getElements();
                for (size_t i = 0; i < elements.size() && i < children.size(); ++i)
                    elements[i] = nameTuplesAsDecoded(elements[i], children[i], case_insensitive);
                return std::make_shared<DataTypeTuple>(std::move(elements));
            }
            if (tuple->hasExplicitNames())
            {
                DataTypes elements = tuple->getElements();
                for (const auto & child : children)
                    if (const auto position = tuple->tryGetPositionByName(child.name, case_insensitive))
                        elements[*position] = nameTuplesAsDecoded(elements[*position], child, case_insensitive);
                return std::make_shared<DataTypeTuple>(std::move(elements), tuple->getElementNames());
            }
            /// The elements past the fields of the struct are matched with none, and do not affect decoding.
            const size_t size = std::min(tuple->getElements().size(), children.size());
            if (size == 0)
                return hint;
            DataTypes elements;
            Strings names;
            for (size_t i = 0; i < size; ++i)
            {
                elements.push_back(nameTuplesAsDecoded(tuple->getElement(i), children[i], case_insensitive));
                names.push_back(children[i].name);
            }
            return std::make_shared<DataTypeTuple>(std::move(elements), std::move(names));
        }
        default:
            return hint;
    }
}

}

void ArrowIPC::collectRequestedTypes(
    const Block & header, bool case_insensitive, RequestedTypes & requested_types, SharedRequestedHeaderTypes & shared_header_types)
{
    requested_types.clear();
    shared_header_types.clear();
    for (const auto & header_column : header)
    {
        String name = header_column.name;
        if (case_insensitive)
            boost::to_lower(name);
        /// Record each requested column's target type by (normalized) name so the decoder can read a
        /// `date32` mapped to a numeric target as the raw day number — recursively for a `date32` nested in
        /// an Array/Tuple/Map, and for one addressed as a subcolumn (e.g. `t.d`). Mirrors the Apache Arrow
        /// library reader's recursive numeric type-hint handling.
        ///
        /// With case-insensitive matching, several header columns can share a normalized name (`T String` and
        /// `t Tuple(s IPv6)`). They are all taken from the one decoded field, so its raw bytes must not follow
        /// any one of them: the name is marked as shared, and each column is converted by its own type after
        /// decoding. The type that decoding depends on the most is kept (`T Tuple(s IPv6)` keeps the null map
        /// of the struct for `t String`, `T Int32` reads a `date32` as the day number for `t Int64` too), recursively
        /// for the elements of composites (see `mergeDecodingRequirements`).
        ///
        /// Every distinct type requested for the name is recorded, whether or not it equals the type kept so
        /// far: the merge of `Tuple(a Int128, b String)` and `Tuple(a String, b Int128)` is
        /// `Tuple(a Int128, b Int128)`, and a third column that requests exactly that type is one more
        /// representation for a key lookup (see `RequestedType::shared_types`).
        auto [it, inserted] = requested_types.try_emplace(std::move(name), RequestedType{.type = header_column.type, .shared = false, .shared_types = {}});
        auto & header_types = shared_header_types[it->first];
        if (std::ranges::any_of(header_types, [&](const auto & type) { return type->equals(*header_column.type); }))
            continue;
        header_types.push_back(header_column.type);
        if (header_types.size() > 1)
        {
            it->second.shared = true;
            it->second.shared_types = header_types;
            it->second.type = mergeDecodingRequirements(it->second.type, header_column.type, case_insensitive);
        }
    }
    /// Only the names with several types are shared.
    std::erase_if(shared_header_types, [](const auto & name_and_types) { return name_and_types.second.size() < 2; });
}

void ArrowIPC::resolveSharedRequestedTypes(
    const ArrowSchema & schema, const UnorderedSetWithMemoryTracking<String> * keep_top_level_fields, bool case_insensitive,
    const SharedRequestedHeaderTypes & shared_requested_header_types, RequestedTypes & requested_field_target_types)
{
    if (shared_requested_header_types.empty())
        return;

    /// The top-level fields that supply the columns, by normalized name: of the fields with the same normalized
    /// name only the last (see `RecordBatchDecoder::shadowedTopLevelFields` and `buildChunk`), and only the kept ones.
    UnorderedMapWithMemoryTracking<String, const ArrowIPC::ArrowField *> decoded_top_level;
    Strings decoded_top_level_names;
    for (const auto & top_level : schema.fields | std::views::reverse)
    {
        String top_level_name = top_level.name;
        if (case_insensitive)
            boost::to_lower(top_level_name);
        if ((!keep_top_level_fields || keep_top_level_fields->contains(top_level_name))
            && decoded_top_level.emplace(top_level_name, &top_level).second)
            decoded_top_level_names.push_back(top_level_name);
    }

    for (const auto & [name, header_types] : shared_requested_header_types)
    {
        /// The requested types are resolved against the field that supplies the columns, chosen as `buildChunk`
        /// chooses it: the top-level field with exactly this name, or else a subcolumn of the top-level field
        /// named by the part before the first dot. The decoder looks the requested type up by the dotted path
        /// alone, so every other decoded field with this path (the field `s` of a struct `t` next to a top-level
        /// field named `t.s`) is decoded with it as well: the requirements resolved against each of these
        /// fields are merged in too, so that none of them loses what it needs (a positional `Tuple(Int32)`
        /// names a different field in each of them).
        VectorWithMemoryTracking<DecodedFieldAtPath> fields;
        if (auto it = decoded_top_level.find(name); it != decoded_top_level.end())
            fields.push_back({it->second, 0});
        const String table_name = Nested::extractTableName(name);
        if (table_name != name)
            if (auto it = decoded_top_level.find(table_name); it != decoded_top_level.end())
                collectDecodedFields(*it->second, table_name, 0, name, case_insensitive, fields);
        for (const auto & other_name : decoded_top_level_names)
            if (other_name != name && other_name != table_name)
                collectDecodedFields(*decoded_top_level.at(other_name), other_name, 0, name, case_insensitive, fields);
        if (fields.empty())
            continue;

        /// The requested type of a field below lists has an `Array` per list (see `ArrowIPC::resolveHint`).
        auto name_tuples = [&](const DataTypePtr & type, const DecodedFieldAtPath & at) -> DataTypePtr
        {
            if (at.list_depth == 0)
                return nameTuplesAsDecoded(type, *at.field, case_insensitive);
            const auto * array = typeid_cast<const DataTypeArray *>(type.get());
            if (!array)
                return type;
            DataTypePtr element = array->getNestedType();
            for (size_t i = 1; i < at.list_depth; ++i)
            {
                const auto * nested_array = typeid_cast<const DataTypeArray *>(element.get());
                if (!nested_array)
                    return type;
                element = nested_array->getNestedType();
            }
            DataTypePtr named = nameTuplesAsDecoded(element, *at.field, case_insensitive);
            for (size_t i = 0; i < at.list_depth; ++i)
                named = std::make_shared<DataTypeArray>(named);
            return named;
        };

        DataTypePtr merged;
        for (const auto & at : fields)
        {
            for (const auto & header_type : header_types)
            {
                DataTypePtr named = name_tuples(header_type, at);
                merged = merged ? mergeDecodingRequirements(merged, named, case_insensitive) : named;
            }
        }
        requested_field_target_types.at(name).type = merged;
    }
}

void ArrowIPCBlockInputFormat::resolveSharedRequestedTypes()
{
    ArrowIPC::resolveSharedRequestedTypes(
        *arrow_schema, &requested_top_level_fields, format_settings.arrow.case_insensitive_column_matching,
        shared_requested_header_types, requested_field_target_types);
}

void ArrowIPCBlockInputFormat::prepareReader()
{
    /// Determine which top-level Arrow fields the requested header needs, so the reader can skip the rest
    /// (subset-of-columns support): a field is needed if a header column matches it by name, or is a
    /// `Nested`/struct subcolumn of it (a header column `name.sub` keeps the field `name`) — mirroring how
    /// `buildChunk` maps columns. This depends only on the header, so compute it before reading the file:
    /// the file reader uses it below to skip the dictionaries of unrequested columns. Names are lower-cased
    /// when case-insensitive matching is on, matching the lookup in `decodeBatch`.
    const bool case_insensitive = format_settings.arrow.case_insensitive_column_matching;
    auto normalize = [&](String s) -> String { if (case_insensitive) boost::to_lower(s); return s; };
    requested_top_level_fields.clear();
    visibility_only_structs.clear();
    for (const auto & header_column : getPort().getHeader())
    {
        requested_top_level_fields.insert(normalize(header_column.name));
        requested_top_level_fields.insert(normalize(Nested::extractTableName(header_column.name)));
    }
    /// Each requested column's target type by (normalized) name, for the decoder.
    ArrowIPC::collectRequestedTypes(getPort().getHeader(), case_insensitive, requested_field_target_types, shared_requested_header_types);

    if (stream)
        prepareStreamReader();
    else
        prepareFileReader();

    /// The read was cancelled while buffering a non-seekable input (`prepareFileReader` bailed out before
    /// parsing): leave `arrow_schema` unset and return without dereferencing it. `read` then sees
    /// `is_stopped` and returns an empty chunk — a clean cancellation rather than a truncated-file error.
    if (is_stopped)
        return;

    /// GeoParquet geometry columns (schema-level "geo" metadata) are decoded from WKB/WKT into geo types.
    if (format_settings.parquet.allow_geoparquet_parser)
    {
        auto geo_it = arrow_schema->custom_metadata.find("geo");
        geo_columns = parseGeoMetadataEncoding(
            geo_it != arrow_schema->custom_metadata.end() ? &geo_it->second : nullptr);
    }

    /// Repeated column names were made unique by `parseSchema`.
    collectDictionaryFields(arrow_schema->fields);
    decoder = std::make_unique<ArrowIPC::RecordBatchDecoder>(*arrow_schema, format_settings, dictionaries);

    /// Collect the dictionaries the requested columns reference and the field positions each one's values
    /// are decoded for; the stream reader skips the DictionaryBatch bodies of dictionaries used only by
    /// unrequested columns. (The file reader has already computed and used this above, before
    /// decoding its dictionaries up front; recomputing it here is cheap and keeps the stream path — whose
    /// dictionaries are decoded lazily while reading — correct.)
    dictionary_uses = decoder->collectDictionaryUses(&requested_top_level_fields, &requested_field_target_types);
    visibility_only_structs = decoder->collectVisibilityOnlyStructs(&requested_top_level_fields, &requested_field_target_types);

    prepared = true;
}

void ArrowIPCBlockInputFormat::prepareStreamReader()
{
    message_reader.emplace(*in);

    ArrowIPC::MessageReader::Message msg;
    if (!message_reader->readNextMessage(msg))
        throw Exception(ErrorCodes::INCORRECT_DATA, "The Arrow stream is empty");
    if (msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_Schema)
        throw Exception(ErrorCodes::INCORRECT_DATA, "The first Arrow IPC message must be the schema");

    arrow_schema = ArrowIPC::parseSchema(*msg.header->header_as_Schema(), msg.header->version());
    resolveSharedRequestedTypes();
    /// The schema message has no body, but be defensive in case a producer emits a (zero-length) one.
    message_reader->skipBody(msg.body_length);
}

void ArrowIPCBlockInputFormat::prepareFileReader()
{
    /// The file format requires random access. Use the input directly when it is seekable, otherwise
    /// load it entirely into memory (matching the Apache Arrow library's behaviour for such inputs).
    /// The file format needs random access. Use the input directly only when it is genuinely a
    /// seekable file with a known size (`tryGetFileSizeFromReadBuffer` returns 0 for pipes); otherwise
    /// load it entirely into memory, matching the Apache Arrow library's behaviour for such inputs.
    chassert(in); /// the input buffer is set in the constructor and is never null
    size_t file_size = 0;
    /// Use random access only when the input is genuinely seekable, its size is known, and
    /// `input_format_allow_seeks` is not disabled — mirroring `asArrowFile` in the library reader.
    seekable = format_settings.seekable_read ? dynamic_cast<SeekableReadBuffer *>(in) : nullptr;
    std::optional<size_t> known_size;
    if (seekable)
        known_size = tryGetFileSizeFromReadBuffer(*in);
    if (seekable && known_size && *known_size > 0 && seekable->checkIfActuallySeekable())
    {
        file_size = *known_size;
    }
    else
    {
        /// Non-seekable input: the file format needs the whole file in memory (its footer is at the end).
        /// Validate the leading "ARROW1" magic before buffering the (possibly huge, possibly non-Arrow)
        /// remainder, and copy it with a cancellation-aware loop — matching the Apache Arrow library
        /// reader's fail-fast and cancellation behavior instead of slurping the entire stream first.
        static constexpr std::string_view ARROW_FILE_MAGIC = "ARROW1";
        char magic[ARROW_FILE_MAGIC.size()] = {};
        in->readStrict(magic, sizeof(magic));
        if (std::string_view(magic, sizeof(magic)) != ARROW_FILE_MAGIC)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Not an Arrow file: the leading {} magic is missing", ARROW_FILE_MAGIC);
        auto data = std::make_unique<ArrowIPC::InMemoryFile>();
        data->write(magic, sizeof(magic));
        copyData(*in, *data, is_stopped);
        data->finalize();
        /// `copyData` returns early when the read was cancelled, leaving `data` partial. Do not parse
        /// that truncated buffer as an Arrow file — bail out and let `prepareReader`/`read` finish cleanly
        /// (matching the library path, which returned without parsing after a cancelled `asArrowFile`).
        if (is_stopped)
            return;
        file_size = data->count();
        memory_buffer = data->getSeekableReadBuffer();
        file_data = std::move(data);
        seekable = memory_buffer.get();
    }
    message_reader.emplace(*seekable);

    ArrowIPC::ArrowFileFooter footer = ArrowIPC::readArrowFileFooter(*seekable, file_size);
    arrow_schema = std::move(footer.schema);
    /// Before the dictionaries are decoded below with the requested types.
    resolveSharedRequestedTypes();
    for (const auto & block : footer.record_batch_blocks)
        record_batch_blocks.push_back({block.offset, block.metadata_length, block.body_length});

    /// For count-only reads, the row count comes from the record-batch metadata (`batch->length()`), so
    /// no record batch is ever decoded and the dictionaries are never needed. Skip decoding the
    /// dictionary batches up front: it is wasted work, materializes potentially large dictionaries, and
    /// would needlessly fail on a corrupt dictionary body when the counts are already in the footer.
    if (!need_only_count)
    {
        /// File dictionaries apply to every record batch, including batches preceding them in the file.
        /// Deltas extend dictionaries in footer order. Each id permits only one non-delta batch.
        collectDictionaryFields(arrow_schema->fields);
        /// Subset reads: only the dictionaries the requested columns reference are decoded; the bodies of
        /// dictionaries used solely by unrequested top-level fields are skipped (see the loop below).
        auto temp_decoder = std::make_unique<ArrowIPC::RecordBatchDecoder>(*arrow_schema, format_settings, dictionaries);
        dictionary_uses = temp_decoder->collectDictionaryUses(&requested_top_level_fields, &requested_field_target_types);
        UnorderedSetWithMemoryTracking<Int64> base_dictionary_ids;
        for (const auto & block : footer.dictionary_blocks)
        {
            seekable->seek(block.offset, SEEK_SET);
            ArrowIPC::MessageReader::Message msg;
            /// Bound the metadata read by the footer block's declared metadata size, so a malformed footer
            /// cannot make the reader allocate a huge metadata buffer from the length prefix at `block.offset`.
            if (!message_reader->readNextMessage(msg, block.metadata_length)
                || msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_DictionaryBatch)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Expected a dictionary batch in the Arrow file");

            const auto * dict_batch = msg.header->header_as_DictionaryBatch();
            const Int64 id = dict_batch->id();
            /// A dictionary referenced only by unrequested (skipped) top-level fields is never used by the
            /// chunk this query builds. Do not decode its body: that would be wasted work and could fail or
            /// allocate a large/corrupt dictionary for a column the query did not ask for. The next loop
            /// iteration seeks to the following block, so the unread body is harmless. Check this before any
            /// body-length/data validation so a missing/corrupt unrequested dictionary cannot fail a
            /// projected read.
            if (!dictionary_uses.contains(id))
                continue;
            if (!dict_batch->isDelta() && !base_dictionary_ids.insert(id).second)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC file contains a replacement for dictionary id {}", id);
            /// The footer block fixes this message's body size; a message claiming a different (e.g. huge)
            /// body length is corrupt and would otherwise force reading past the footer-declared boundary.
            if (msg.body_length != block.body_length)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Arrow IPC dictionary batch body length {} does not match the footer block length {}",
                    msg.body_length, block.body_length);
            decodeDictionaryBatch(*temp_decoder, *dict_batch, msg.body_length);
        }
    }
}

void ArrowIPCBlockInputFormat::decodeDictionaryBatch(
    ArrowIPC::RecordBatchDecoder & batch_decoder, const ArrowIPC::flatbuf::DictionaryBatch & dict_batch, Int64 body_length)
{
    const Int64 id = dict_batch.id();
    /// `DictionaryBatch.data` is optional in the FlatBuffer schema; reject a missing one rather than
    /// dereferencing null below.
    if (!dict_batch.data())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC dictionary batch has no data");
    /// Checked here rather than in `readNextMessage`, so a dictionary that is skipped is never validated.
    ArrowIPC::MessageReader::checkRecordBatchAlignment(*dict_batch.data());
    auto field_it = dictionary_value_fields.find(id);
    if (field_it == dictionary_value_fields.end())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC dictionary batch for unknown dictionary id {}", id);
    const ArrowIPC::ArrowField & value_field = field_it->second;

    /// Reject a batch declaring buffers/nodes beyond its single value field before materializing the body,
    /// so surplus buffers are never read or decompressed only to be ignored.
    const ArrowIPC::ArrowFields value_fields{value_field};
    batch_decoder.validateBatchLayout(*dict_batch.data(), value_fields);
    message_reader->readBody(*dict_batch.data(), body_length, body_buffer);

    /// Decode the values once for each use of this dictionary (the position of a field encoding it, with the
    /// requested type the field resolves there), so every field gets the decoding it would get for inline
    /// values in a record batch. The registered type must describe the registered column: the raw-byte
    /// rewrite the record-batch columns go through reinterprets fixed_size_binary values under an IPv6 /
    /// big-integer hint and re-declares values the decoder already converted or read raw, so
    /// `decodeDictionary` later builds its `LowCardinality` from a consistent (values, type) pair.
    for (const ArrowIPC::DictionaryUse & use : dictionary_uses.at(id))
    {
        auto decoded = batch_decoder.decodeDictionaryValues(
            *dict_batch.data(), body_buffer, value_field, use, &requested_field_target_types);
        ColumnWithTypeAndName values(decoded.column, decoded.type, value_field.name);
        /// No field: a dictionary is how a `LowCardinality` is written, and that wraps only numbers, strings,
        /// `Date` and `DateTime`, each of which has an Arrow type of its own. So a dictionary's values are
        /// never an opaque column, and the rewrite has no field metadata to act on here.
        /// A `Variant` is repaired later, on the materialized column: a dictionary nested in a composite is
        /// declared by its Arrow type, which a repaired value type would contradict.
        if (use.hint && !use.keep_raw_bytes)
            reinterpretRawByteColumns(values, use.hint, /*field=*/nullptr, /*repair_variants=*/false);

        /// Arrow doesn't require dictionary values to be unique, and repeated values are allowed: `decodeDictionary`
        /// builds a `LowCardinality` by inserting the values into its own unique dictionary and remapping the indexes.
        dictionaries.set(id, use, {values.column, values.type, std::move(decoded.null_map)}, dict_batch.isDelta());
    }
}

namespace
{

/// An integer or float alternative strictly wider than a `width`-byte decoded integer.
bool variantElementWidensTo(const DataTypePtr & alternative, size_t width)
{
    const WhichDataType which(alternative);
    if (which.isFloat32())
        return width <= 2;
    if (which.isFloat64())
        return width <= 4;
    return which.isInteger() && alternative->getSizeOfValueInMemory() > width;
}

/// The natural pairing of a decoded element with an alternative: the alternative it was itself inferred as.
/// Decoded element names are unique (the decoder rejects a union whose children map to one ClickHouse type).
bool variantElementPrefersType(const DataTypePtr & decoded, const DataTypePtr & alternative)
{
    return ArrowIPC::stripHint(alternative)->getName() == decoded->getName();
}

/// Whether a decoded `Variant` element can be repaired into a requested alternative: its own decoded type,
/// otherwise only an alternative the ordinary (non-union) Arrow column path already reaches from `decoded`.
/// Three exclusions whose reason the arms below do not show: a `UInt8` value outside 0/1 would render as
/// `true` under the equal-comparing `Bool`; a cast between tuples with differing names fills the target
/// fields rather than rejecting them, and this walk keeps the decoded names; and `Date32` decoding is
/// hint-dependent (range-checked, saturated or verbatim) while this layer supplies no hint, whereas the
/// admitted widenings read no hint and are in range at every scale.
bool variantElementMatchesType(const DataTypePtr & decoded, const DataTypePtr & alternative)
{
    const DataTypePtr to = ArrowIPC::stripHint(alternative);
    const WhichDataType from(decoded);
    const WhichDataType which(to);

    /// An element admitted by no arm must still claim its own alternative, or it leaves a sibling ambiguous.
    if (variantElementPrefersType(decoded, alternative))
        return true;
    if (isBool(decoded))
        return which.isUInt8() || which.isEnum8() || variantElementWidensTo(to, 0);
    if (from.isInt8() || from.isUInt8())
        return which.isInt8() || (which.isUInt8() && !isBool(to)) || which.isEnum8() || variantElementWidensTo(to, 1);
    if (from.isInt16() || from.isUInt16())
        return which.isInt16() || which.isUInt16() || which.isEnum16() || variantElementWidensTo(to, 2)
            || (from.isUInt16() && which.isDate());
    if (from.isInt32() || from.isUInt32())
        return which.isInt32() || which.isUInt32() || variantElementWidensTo(to, 4)
            || (from.isUInt32() && (which.isIPv4() || which.isDateTime() || which.isDateTime64()));
    if (from.isInt64() || from.isUInt64())
        return which.isInt64() || which.isUInt64() || variantElementWidensTo(to, 8)
            || (from.isInt64() && which.isInterval());
    if (from.isFloat32())
        return which.isFloat32() || which.isFloat64();
    if (from.isFloat64())
        return which.isFloat64();
    if (from.isString())
        return which.isStringOrFixedString() || ArrowIPC::rawByteWidth(which) != 0;
    if (from.isFixedString())
    {
        const size_t n = assert_cast<const DataTypeFixedString &>(*decoded).getN();
        if (which.isFixedString())
            return assert_cast<const DataTypeFixedString &>(*to).getN() == n;
        return which.isString() || (which.isUUID() && n == 16) || ArrowIPC::rawByteWidth(which) == n;
    }
    if (from.isUUID())
        return which.isUUID() || which.isString();
    if (from.isDecimal())
        return which.isDecimal();
    /// Another scale or time zone is what the flat column path returns for the same request.
    if (from.isDateTime())
        return which.isDateTime() || which.isDateTime64();
    if (from.isDateTime64())
        return which.isDateTime64();
    if (from.isTime64())
        return which.isTime64();
    return false;
}

/// The alternative each element of a decoded `Variant` must be repaired into, in the decoded type's global
/// order, null where nothing is forced. `Variant` sorts its alternatives, so the positional correspondence
/// between Arrow union children and requested alternatives is lost and is reconstructed structurally here;
/// an element whose correspondence stays ambiguous is left unassigned, so no guess is ever made. Two
/// elements are never left on one alternative name: `DataTypeVariant` deduplicates by name while the column
/// keeps one element per union child, so a shared name would describe a column whose layout contradicts its
/// type.
DataTypes variantElementTargets(const DataTypeVariant & from, const DataTypeVariant & to)
{
    const DataTypes & elements = from.getVariants();
    const DataTypes & alternatives = to.getVariants();
    DataTypes targets(elements.size());

    VectorWithMemoryTracking<VectorWithMemoryTracking<size_t>> candidates(elements.size());
    for (size_t e = 0; e < elements.size(); ++e)
        for (size_t a = 0; a < alternatives.size(); ++a)
            if (variantElementMatchesType(elements[e], alternatives[a]))
                candidates[e].push_back(a);

    const auto preferred_candidates = [&](size_t e)
    {
        VectorWithMemoryTracking<size_t> preferred;
        for (const size_t a : candidates[e])
            if (variantElementPrefersType(elements[e], alternatives[a]))
                preferred.push_back(a);
        return preferred;
    };
    /// Erasing the taken alternative elsewhere is what keeps two elements off one alternative.
    const auto take = [&](size_t e, size_t a)
    {
        targets[e] = alternatives[a];
        for (size_t other = 0; other < elements.size(); ++other)
            if (other != e)
                std::erase(candidates[other], a);
    };

    bool changed = true;
    while (changed)
    {
        changed = false;
        for (size_t e = 0; e < elements.size(); ++e)
        {
            if (targets[e] || candidates[e].size() != 1)
                continue;
            take(e, candidates[e].front());
            changed = true;
        }

        if (changed)
            continue;

        for (size_t e = 0; e < elements.size() && !changed; ++e)
        {
            if (targets[e] || candidates[e].size() < 2)
                continue;
            /// A lone preference is never contested: the decoded element names are pairwise distinct.
            const VectorWithMemoryTracking<size_t> preferred = preferred_candidates(e);
            if (preferred.size() != 1)
                continue;
            take(e, preferred.front());
            changed = true;
        }
    }

    /// Every duplicate pairs one assigned name with one decoded name, so each pass drops at least one
    /// assignment and the loop terminates; the worst case is no assignment, which leaves the column as decoded.
    for (bool dropped = true; dropped;)
    {
        dropped = false;
        VectorWithMemoryTracking<bool> drop(elements.size(), false);
        for (size_t e = 0; e < elements.size(); ++e)
            for (size_t other = 0; targets[e] && other < elements.size(); ++other)
                if (other != e && (targets[other] ? targets[other] : elements[other])->getName() == targets[e]->getName())
                    drop[e] = true;
        for (size_t e = 0; e < elements.size(); ++e)
            if (drop[e])
            {
                targets[e] = nullptr;
                dropped = true;
            }
    }

    return targets;
}

std::pair<ColumnPtr, DataTypePtr> repairVariantElements(
    const ColumnPtr & col, const DataTypePtr & from_type, const DataTypeVariant & from_variant,
    const DataTypeVariant & to_variant, bool case_insensitive, const FormatSettings & format_settings);

/// The schema field of the `i`-th child, or null when the field is unknown or does not have that child.
/// Walking the fields alongside the types lets a leaf read its own metadata; a null field simply means no
/// metadata is available and the leaf behaves as it did before.
/// A `RunEndEncoded` field decodes to a column of its values field's shape, so metadata walks look through it.
const ArrowIPC::ArrowField * skipRunEndEncoded(const ArrowIPC::ArrowField * field)
{
    while (field && !field->dictionary && field->type.kind == ArrowIPC::TypeKind::RunEndEncoded)
        field = &field->type.children.at(1);
    return field;
}

const ArrowIPC::ArrowField * childField(const ArrowIPC::ArrowField * field, size_t i)
{
    field = skipRunEndEncoded(field);
    if (!field || i >= field->type.children.size())
        return nullptr;
    return skipRunEndEncoded(&field->type.children[i]);
}

/// Recursively rewrite the raw-byte leaves (fixed_size_binary / binary) of a decoded column into the
/// UUID / IPv6 / big-integer types the requested `to_type` asks for, descending through Nullable, Array,
/// Tuple and Map so nested shapes convert too. Anything not recognised is returned unchanged for the
/// subsequent `castColumn`. Returns the (possibly rewritten) column and its new type.
/// The walk follows the decoded (source) shape and resolves each level's requested type with the same
/// hint helpers the decoder's recursion uses (`arrayElementHint`, `tupleElementHint`, `mapEntriesHint`,
/// `stripHint`): the decoder converts binary leaves under those rules — by tuple element name, through
/// `Nullable`/`LowCardinality` wrappers — so this rewrite must resolve targets identically, or a
/// converted leaf (physically typed, still declared by its Arrow type) would reach `castColumn`
/// unreconciled.
/// `ancestor_nulls` (may be null) marks rows that are null at an enclosing Nullable level: the Arrow spec
/// leaves the bytes of such rows undefined, so the leaf width sniff must not let them force the String
/// fallback (and text-parsing of the whole column in the cast); their values decode as type defaults.
/// Only row-aligned levels propagate it — an Array/Map child lives in a different row space.
std::pair<ColumnPtr, DataTypePtr> reinterpretRawBytes(
    const ColumnPtr & col, const DataTypePtr & from_type, const DataTypePtr & to_type,
    const NullMap * ancestor_nulls, bool case_insensitive, const ArrowIPC::ArrowField * field,
    const FormatSettings & format_settings, bool repair_variants, bool type_only = false)
{
    field = skipRunEndEncoded(field);
    const DataTypePtr from_no_null = removeNullable(from_type);

    const auto * from_arr = typeid_cast<const DataTypeArray *>(from_no_null.get());
    const auto * from_tup = typeid_cast<const DataTypeTuple *>(from_no_null.get());
    const auto * from_map = typeid_cast<const DataTypeMap *>(from_no_null.get());

    /// Schema inference can wrap a composite in Nullable (e.g. `Nullable(Tuple(...))`). The composite
    /// recursion below matches on the unwrapped column, so peel a Nullable column wrapper here first and
    /// restore it afterwards (dropping it if the reinterpreted composite cannot itself be Nullable).
    if (from_arr || from_tup || from_map)
    {
        if (const auto * col_nullable = typeid_cast<const ColumnNullable *>(col.get()))
        {
            /// The peeled wrapper's nulls make the rows under them invisible for every row-aligned
            /// descendant; compose them into the inherited set.
            NullMap combined_storage;
            const NullMap * combined = ArrowIPC::unionNullMaps(col_nullable->getNullMapData(), ancestor_nulls, combined_storage);
            auto [new_nested, new_nested_type]
                = reinterpretRawBytes(
                    col_nullable->getNestedColumnPtr(), from_no_null, to_type, combined, case_insensitive, field, format_settings, repair_variants, type_only);
            /// A leaf the decoder already converted comes back with the same column but a new type, so
            /// change detection must look at both.
            if (new_nested.get() == col_nullable->getNestedColumnPtr().get() && new_nested_type.get() == from_no_null.get())
                return {col, from_type};
            if (new_nested->canBeInsideNullable())
                return {ColumnNullable::create(new_nested, col_nullable->getNullMapColumnPtr()),
                        std::make_shared<DataTypeNullable>(new_nested_type)};
            return {std::move(new_nested), std::move(new_nested_type)};
        }
    }

    if (from_arr)
    {
        const DataTypePtr elem_target = ArrowIPC::arrayElementHint(to_type);
        if (!elem_target)
            return {col, from_type};
        const auto & col_arr = assert_cast<const ColumnArray &>(*col);
        auto [new_data, new_data_type] = reinterpretRawBytes(
            col_arr.getDataPtr(), from_arr->getNestedType(), elem_target, /*ancestor_nulls=*/nullptr, case_insensitive,
            childField(field, 0), format_settings, repair_variants, type_only);
        if (new_data.get() == col_arr.getDataPtr().get() && new_data_type.get() == from_arr->getNestedType().get())
            return {col, from_type};
        /// The immutable factory shares both children; `IColumn::mutate` here would deep-clone the
        /// (possibly unchanged and shared) element column just to rewrap it.
        auto new_arr = ColumnArray::create(new_data, col_arr.getOffsetsPtr());
        return {std::move(new_arr), std::make_shared<DataTypeArray>(new_data_type)};
    }

    if (from_tup)
    {
        const auto & col_tup = assert_cast<const ColumnTuple &>(*col);
        const auto & from_elems = from_tup->getElements();
        const auto & from_names = from_tup->getElementNames();
        Columns new_cols(from_elems.size());
        DataTypes new_types(from_elems.size());
        bool changed = false;
        for (size_t i = 0; i < from_elems.size(); ++i)
        {
            /// Resolve the element's target exactly as the decoder did when it decoded this element: by
            /// name for a named target Tuple (so reordered or subset targets pair correctly), by position
            /// for an unnamed one, and also by position for a struct with empty or repeated field names,
            /// which is decoded as an unnamed Tuple. An element the request does not mention stays as decoded.
            const DataTypePtr elem_target = from_tup->hasExplicitNames()
                ? ArrowIPC::tupleElementHint(to_type, from_names[i], i, case_insensitive)
                : ArrowIPC::tupleElementHintByPosition(to_type, i);
            if (elem_target)
            {
                auto [c, t] = reinterpretRawBytes(
                    col_tup.getColumnPtr(i), from_elems[i], elem_target, ancestor_nulls, case_insensitive,
                    childField(field, i), format_settings, repair_variants, type_only);
                changed |= c.get() != col_tup.getColumnPtr(i).get() || t.get() != from_elems[i].get();
                new_cols[i] = std::move(c);
                new_types[i] = std::move(t);
            }
            else
            {
                new_cols[i] = col_tup.getColumnPtr(i);
                new_types[i] = from_elems[i];
            }
        }
        if (!changed)
            return {col, from_type};
        DataTypePtr new_tuple_type = from_tup->hasExplicitNames()
            ? std::make_shared<DataTypeTuple>(new_types, from_names)
            : std::make_shared<DataTypeTuple>(new_types);
        return {ColumnTuple::create(new_cols), std::move(new_tuple_type)};
    }

    if (from_map)
    {
        const DataTypePtr entries_target = ArrowIPC::mapEntriesHint(to_type);
        if (!entries_target)
            return {col, from_type};
        const auto & col_map = assert_cast<const ColumnMap &>(*col);
        /// A Map is physically an Array(Tuple(key, value)); recurse into that shape so raw-byte keys/values
        /// convert, then rewrap.
        auto from_nested = std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(DataTypes{from_map->getKeyType(), from_map->getValueType()}));
        auto to_nested = std::make_shared<DataTypeArray>(entries_target);
        auto [new_nested, new_nested_type] = reinterpretRawBytes(
            col_map.getNestedColumnPtr(), from_nested, to_nested, ancestor_nulls, case_insensitive, field, format_settings, repair_variants, type_only);
        if (new_nested.get() == col_map.getNestedColumnPtr().get() && new_nested_type.get() == from_nested.get())
            return {col, from_type};
        const auto & new_tuple = assert_cast<const DataTypeTuple &>(*assert_cast<const DataTypeArray &>(*new_nested_type).getNestedType());
        auto new_map_type = std::make_shared<DataTypeMap>(new_tuple.getElement(0), new_tuple.getElement(1));
        return {ColumnMap::create(new_nested), std::move(new_map_type)};
    }

    if (const auto * from_variant = typeid_cast<const DataTypeVariant *>(from_no_null.get()))
    {
        const auto * to_variant = typeid_cast<const DataTypeVariant *>(ArrowIPC::stripHint(to_type).get());
        if (!to_variant || !repair_variants)
            return {col, from_type};
        return repairVariantElements(col, from_type, *from_variant, *to_variant, case_insensitive, format_settings);
    }

    /// Leaf. The target is the innermost requested type, `Nullable`/`LowCardinality`-transparent like
    /// every decoder hint (`LowCardinality(IPv6)` requests the same raw bytes as `IPv6`; the later cast
    /// restores the wrappers).
    const DataTypePtr to_leaf = ArrowIPC::stripHint(to_type);

    const auto * nullable = typeid_cast<const ColumnNullable *>(col.get());
    const IColumn & nested = nullable ? nullable->getNestedColumn() : *col;
    const NullMap * null_map = nullable ? &nullable->getNullMapData() : nullptr;
    /// A rewritten leaf keeps the source column's Nullable wrapper; the request's own wrappers are
    /// restored by the later cast.
    const auto with_source_nullability = [&](DataTypePtr type) -> DataTypePtr
    {
        return nullable ? std::make_shared<DataTypeNullable>(std::move(type)) : type;
    };

    /// A `date32` under a Decimal hint was read as the raw day number (`date32_as_number` in the
    /// decoder), and no `Date32` -> Decimal cast exists; re-declare the (physically `Int32`) column as
    /// `Int32` so the ordinary Int -> Decimal cast finishes the conversion.
    if (WhichDataType(from_no_null).isDate32() && isDecimal(to_leaf))
        return {col, with_source_nullability(std::make_shared<DataTypeInt32>())};
    /// The same for the values of a dictionary, which are registered without the post-decode rewrite when the
    /// field is decoded for several requested columns (see `DictionaryUse::keep_raw_bytes`). The values of a
    /// `LowCardinality(Date32)` are physically `Int32` too, so only the declared type changes.
    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(from_no_null.get()))
    {
        const DataTypePtr & dictionary_type = low_cardinality_type->getDictionaryType();
        if (WhichDataType(removeNullable(dictionary_type)).isDate32() && isDecimal(to_leaf))
        {
            const DataTypePtr int_type = std::make_shared<DataTypeInt32>();
            return {col, with_source_nullability(std::make_shared<DataTypeLowCardinality>(
                dictionary_type->isNullable() ? makeNullable(int_type) : int_type))};
        }
    }

    /// The leaf's undefined rows are those null at its own level or at any enclosing level; both must be
    /// exempt from the width sniff and decode as defaults.
    NullMap combined_storage;
    const auto undefined_rows = [&]() -> const NullMap *
    {
        return null_map ? ArrowIPC::unionNullMaps(*null_map, ancestor_nulls, combined_storage) : ancestor_nulls;
    };

    /// A column with no Arrow mapping that the writer wrote with `serializeBinary` carries the ClickHouse
    /// type it came from. Its bytes are not text, so the cast the caller applies to a `String` column - the
    /// text parser - cannot read them; deserialize them here instead, where the type travels with the
    /// column and the two cannot disagree.
    if (field && field->type.kind == ArrowIPC::TypeKind::Binary)
    {
        if (const auto * str = typeid_cast<const ColumnString *>(&nested))
        {
            if (MutableColumnPtr typed
                = ArrowIPC::deserializeOpaqueBinaryLeaf(*str, undefined_rows(), to_leaf, *field, format_settings))
            {
                if (nullable && to_leaf->canBeInsideNullable())
                    return {ColumnNullable::create(std::move(typed), nullable->getNullMapColumnPtr()),
                            std::make_shared<DataTypeNullable>(to_leaf)};
                return {ColumnPtr(std::move(typed)), to_leaf};
            }
        }
    }

    const WhichDataType which(to_leaf);
    const bool raw_target = which.isUUID() || ArrowIPC::rawByteWidth(which) != 0;
    if (!raw_target)
        return {col, from_type};

    /// The decoder already converts a variable binary leaf whose type hint reaches it (mask-aware, so
    /// it also covers invisibility this phase cannot see: dropped struct null maps, masked list
    /// ranges); only the declared type needs reconciling then.
    if (nested.getDataType() == to_leaf->getTypeId())
    {
        if (from_no_null->equals(*to_leaf))
            return {col, from_type};
        return {col, with_source_nullability(to_leaf)};
    }

    /// A top-level dictionary is decoded as `LowCardinality` (see `RecordBatchDecoder::decodeDictionary`). Its
    /// values keep their raw bytes when the field is decoded for several requested columns (see
    /// `DictionaryUse::keep_raw_bytes`); convert them for this column through the full column, which keeps
    /// the nulls of the rows. The later cast restores a requested `LowCardinality`.
    if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(&nested))
    {
        const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(from_no_null.get());
        if (!low_cardinality_type
            || low_cardinality->getDictionary().getNestedNotNullableColumn()->getDataType() == to_leaf->getTypeId())
            return {col, from_type};
        return reinterpretRawBytes(
            low_cardinality->convertToFullColumn(), low_cardinality_type->getDictionaryType(), to_type, ancestor_nulls,
            case_insensitive, field, format_settings, repair_variants, type_only);
    }

    /// Only the type is asked for: a `fixed_size_binary` leaf gets the requested type, whatever its width is.
    /// The width is checked when the values are converted.
    if (type_only && typeid_cast<const ColumnFixedString *>(&nested))
        return {col, with_source_nullability(to_leaf)};

    MutableColumnPtr typed;
    if (const auto * fixed = typeid_cast<const ColumnFixedString *>(&nested))
        typed = ArrowIPC::reinterpretFixedStringLeaf(*fixed, to_leaf);
    else if (const auto * str = typeid_cast<const ColumnString *>(&nested))
        typed = ArrowIPC::reinterpretStringLeaf(*str, undefined_rows(), to_leaf);
    if (!typed)
        return {col, from_type};

    ColumnPtr result = std::move(typed);
    if (nullable)
        result = ColumnNullable::create(result, nullable->getNullMapColumnPtr());
    return {std::move(result), with_source_nullability(to_leaf)};
}

/// Rebuild a decoded `Variant` column with each element under the alternative the requested `Variant`
/// forces on it, or return the input unchanged. A `Variant` -> `Variant` cast only appends alternatives.
std::pair<ColumnPtr, DataTypePtr> repairVariantElements(
    const ColumnPtr & col, const DataTypePtr & from_type, const DataTypeVariant & from_variant,
    const DataTypeVariant & to_variant, bool case_insensitive, const FormatSettings & format_settings)
{
    const DataTypes & elements = from_variant.getVariants();
    const DataTypes targets = variantElementTargets(from_variant, to_variant);
    bool any_change = false;
    for (size_t g = 0; g < elements.size() && !any_change; ++g)
        any_change = targets[g] && targets[g]->getName() != elements[g]->getName();
    if (!any_change)
        return {col, from_type};

    /// Repairing helps only when every element ends up under an alternative the request names, matched by
    /// full name as the cast matches them: otherwise the cast rejects the read whatever this walk does.
    for (size_t g = 0; g < elements.size(); ++g)
    {
        if (targets[g])
            continue;
        bool requested = false;
        for (const DataTypePtr & alternative : to_variant.getVariants())
            requested |= alternative->getName() == elements[g]->getName();
        if (!requested)
            return {col, from_type};
    }

    const auto & variant_col = assert_cast<const ColumnVariant &>(*col);
    const size_t num_elements = variant_col.getNumVariants();

    Columns new_elements(num_elements);
    DataTypes new_types(num_elements);
    for (size_t local = 0; local < num_elements; ++local)
    {
        /// Element columns are in Arrow child order while the decoded alternatives are sorted by name.
        const size_t global = variant_col.globalDiscriminatorByLocal(static_cast<ColumnVariant::Discriminator>(local));
        const DataTypePtr & from_element = elements[global];
        ColumnPtr element = variant_col.getVariantPtrByLocalDiscriminator(local);
        DataTypePtr element_type = from_element;
        if (const DataTypePtr & target = targets[global])
        {
            /// The ordinary recursion, so a raw-byte element converts as the same bytes would in a plain
            /// column; an element holds only its rows' values, so no undefined values and no ancestor nulls.
            /// No field: a union is decoded without consulting its children's `clickhouse.opaque` tags.
            std::tie(element, element_type) = reinterpretRawBytes(
                element, from_element, target, /*ancestor_nulls=*/nullptr, case_insensitive, /*field=*/nullptr,
                format_settings, /*repair_variants=*/true);
            /// By name, not `equals`: `Bool` and `UInt8` compare equal yet the element must end up under
            /// exactly the requested one.
            if (element_type->getName() != target->getName())
            {
                element = castColumn({element, element_type, ""}, target);
                element_type = target;
            }
        }
        new_elements[local] = std::move(element);
        new_types[local] = std::move(element_type);
    }

    auto new_variant_type = std::make_shared<DataTypeVariant>(new_types);
    const DataTypes & new_alternatives = new_variant_type->getVariants();
    chassert(new_alternatives.size() == new_types.size());
    VectorWithMemoryTracking<ColumnVariant::Discriminator> new_local_to_global(num_elements);
    for (size_t local = 0; local < num_elements; ++local)
        for (size_t global = 0; global < new_alternatives.size(); ++global)
            if (new_alternatives[global]->getName() == new_types[local]->getName())
                new_local_to_global[local] = static_cast<ColumnVariant::Discriminator>(global);

    auto new_column = ColumnVariant::create(
        variant_col.getLocalDiscriminatorsPtr(), variant_col.getOffsetsPtr(), new_elements, new_local_to_global);
    return {std::move(new_column), std::move(new_variant_type)};
}

/// The type requested for the dotted normalized name of a subcolumn, if it is requested (null otherwise).
using FindRequestedType = std::function<DataTypePtr(const String &)>;

/// `type` without its `levels` outer `Array` layers (see `ArrowIPC::arrayElementHint`), or null.
DataTypePtr peelArrays(DataTypePtr type, size_t levels)
{
    for (size_t i = 0; i < levels && type; ++i)
        type = ArrowIPC::arrayElementHint(type);
    return type;
}

/// The dotted name of the subcolumn `name` of the column at the dotted `path`, normalized as the requested names are.
String childRequestPath(const String & path, std::string_view name, bool case_insensitive)
{
    String child = path + "." + String(name);
    if (case_insensitive)
        boost::to_lower(child);
    return child;
}

/// The type requested for the decoded column of type `type` at the dotted `path` (below `array_depth` arrays)
/// by the requested column of that name, or else assembled from the requested subcolumns below it, with the
/// decoded types of the parts no request mentions (`Tuple(s IPv6)` for the keys `Tuple(s String)` of a map `t`
/// with `t.keys.s Array(IPv6)` requested). Null when neither the column nor a subcolumn of it is requested.
DataTypePtr requestedTypeOrSubcolumns(
    const DataTypePtr & type, const String & path, size_t array_depth, const FindRequestedType & find_requested,
    bool case_insensitive)
{
    if (DataTypePtr requested = peelArrays(find_requested(path), array_depth))
        return requested;
    const DataTypePtr stripped = removeNullable(type);
    if (const auto * array = typeid_cast<const DataTypeArray *>(stripped.get()))
    {
        DataTypePtr element = requestedTypeOrSubcolumns(array->getNestedType(), path, array_depth + 1, find_requested, case_insensitive);
        return element ? std::make_shared<DataTypeArray>(element) : nullptr;
    }
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(stripped.get()))
    {
        DataTypes elements = tuple->getElements();
        bool found = false;
        for (size_t i = 0; i < elements.size(); ++i)
        {
            if (DataTypePtr element = requestedTypeOrSubcolumns(
                    elements[i], childRequestPath(path, tuple->getNameByPosition(i + 1), case_insensitive), array_depth,
                    find_requested, case_insensitive))
            {
                elements[i] = std::move(element);
                found = true;
            }
        }
        if (!found)
            return nullptr;
        if (tuple->hasExplicitNames())
            return std::make_shared<DataTypeTuple>(std::move(elements), tuple->getElementNames());
        return std::make_shared<DataTypeTuple>(std::move(elements));
    }
    if (const auto * map = typeid_cast<const DataTypeMap *>(stripped.get()))
    {
        DataTypePtr key = requestedTypeOrSubcolumns(
            map->getKeyType(), childRequestPath(path, "keys", case_insensitive), array_depth + 1, find_requested, case_insensitive);
        DataTypePtr value = requestedTypeOrSubcolumns(
            map->getValueType(), childRequestPath(path, "values", case_insensitive), array_depth + 1, find_requested, case_insensitive);
        if (!key && !value)
            return nullptr;
        return std::make_shared<DataTypeMap>(key ? key : map->getKeyType(), value ? value : map->getValueType());
    }
    return nullptr;
}

/// `column` of type `type`, decoded at `position` of a top-level field, without the null maps of the structs at
/// the positions in `visibility_only`: the decoder keeps such a null map only to tell the hidden rows to the
/// post-decode raw-byte conversion, and no requested column asks for it. The positions are built as
/// `RecordBatchDecoder::collectVisibilityOnlyStructs` builds them: the elements of a `Tuple` are the children of
/// the struct, the key and the value of a `Map` are the children of its entries, an `Array` adds nothing.
std::pair<ColumnPtr, DataTypePtr> dropVisibilityOnlyNulls(
    const ColumnPtr & column, const DataTypePtr & type, const String & position,
    const UnorderedSetWithMemoryTracking<String> & visibility_only)
{
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        const auto * nullable = typeid_cast<const ColumnNullable *>(column.get());
        if (!nullable)
            return {column, type};
        auto [nested, nested_type] = dropVisibilityOnlyNulls(
            nullable->getNestedColumnPtr(), nullable_type->getNestedType(), position, visibility_only);
        if (isTuple(nested_type) && visibility_only.contains(position))
            return {std::move(nested), std::move(nested_type)};
        if (nested.get() == nullable->getNestedColumnPtr().get())
            return {column, type};
        return {ColumnNullable::create(nested, nullable->getNullMapColumnPtr()), std::make_shared<DataTypeNullable>(nested_type)};
    }
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        const auto * array = typeid_cast<const ColumnArray *>(column.get());
        if (!array)
            return {column, type};
        auto [data, data_type] = dropVisibilityOnlyNulls(
            array->getDataPtr(), array_type->getNestedType(), position, visibility_only);
        if (data.get() == array->getDataPtr().get())
            return {column, type};
        return {ColumnArray::create(data, array->getOffsetsPtr()), std::make_shared<DataTypeArray>(data_type)};
    }
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto * tuple = typeid_cast<const ColumnTuple *>(column.get());
        if (!tuple || tuple->tupleSize() != tuple_type->getElements().size())
            return {column, type};
        Columns columns(tuple->tupleSize());
        DataTypes types(tuple->tupleSize());
        bool changed = false;
        for (size_t i = 0; i < columns.size(); ++i)
        {
            std::tie(columns[i], types[i]) = dropVisibilityOnlyNulls(
                tuple->getColumnPtr(i), tuple_type->getElement(i), fmt::format("{}/{}", position, i), visibility_only);
            changed |= columns[i].get() != tuple->getColumnPtr(i).get();
        }
        if (!changed)
            return {column, type};
        return {ColumnTuple::create(columns),
                tuple_type->hasExplicitNames() ? std::make_shared<DataTypeTuple>(types, tuple_type->getElementNames())
                                               : std::make_shared<DataTypeTuple>(types)};
    }
    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        const auto * map = typeid_cast<const ColumnMap *>(column.get());
        if (!map)
            return {column, type};
        const auto & entries = map->getNestedData();
        auto [keys, keys_type] = dropVisibilityOnlyNulls(
            entries.getColumnPtr(0), map_type->getKeyType(), position + "/0", visibility_only);
        auto [values, values_type] = dropVisibilityOnlyNulls(
            entries.getColumnPtr(1), map_type->getValueType(), position + "/1", visibility_only);
        if (keys.get() == entries.getColumnPtr(0).get() && values.get() == entries.getColumnPtr(1).get())
            return {column, type};
        return {ColumnMap::create(ColumnArray::create(ColumnTuple::create(Columns{keys, values}), map->getNestedColumn().getOffsetsPtr())),
                std::make_shared<DataTypeMap>(keys_type, values_type)};
    }
    return {column, type};
}

/// `column` of type `type` with only the nulls of `own_column` of type `own_type`. Both are the same subcolumn:
/// `column` is taken from a decoded column whose structs keep the null maps the decoder added for the
/// post-decode conversion, and is already converted, while `own_column` is taken from the decoded column without
/// these null maps (see `dropVisibilityOnlyNulls`), and is not converted. A subcolumn inherits the nulls of the
/// structs above it, so `column` has a `Nullable` wherever `own_column` has one, with at least the same nulls;
/// a `Nullable` only `column` has is dropped, and the rows it hides keep the values decoded for them (the
/// defaults, for the converted ones).
std::pair<ColumnPtr, DataTypePtr> restoreOwnNulls(
    const ColumnPtr & column, const DataTypePtr & type, const ColumnPtr & own_column, const DataTypePtr & own_type)
{
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        const auto * nullable = typeid_cast<const ColumnNullable *>(column.get());
        if (!nullable)
            return {column, type};
        const auto * own_nullable_type = typeid_cast<const DataTypeNullable *>(own_type.get());
        const auto * own_nullable = typeid_cast<const ColumnNullable *>(own_column.get());
        if (!own_nullable_type || !own_nullable)
            return restoreOwnNulls(nullable->getNestedColumnPtr(), nullable_type->getNestedType(), own_column, own_type);
        auto [nested, nested_type] = restoreOwnNulls(
            nullable->getNestedColumnPtr(), nullable_type->getNestedType(),
            own_nullable->getNestedColumnPtr(), own_nullable_type->getNestedType());
        if (!nested->canBeInsideNullable())
            return {std::move(nested), std::move(nested_type)};
        return {ColumnNullable::create(nested, own_nullable->getNullMapColumnPtr()), std::make_shared<DataTypeNullable>(nested_type)};
    }
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        const auto * array = typeid_cast<const ColumnArray *>(column.get());
        const auto * own_array_type = typeid_cast<const DataTypeArray *>(own_type.get());
        const auto * own_array = typeid_cast<const ColumnArray *>(own_column.get());
        if (!array || !own_array_type || !own_array)
            return {column, type};
        auto [data, data_type] = restoreOwnNulls(
            array->getDataPtr(), array_type->getNestedType(), own_array->getDataPtr(), own_array_type->getNestedType());
        if (data.get() == array->getDataPtr().get())
            return {column, type};
        return {ColumnArray::create(data, array->getOffsetsPtr()), std::make_shared<DataTypeArray>(data_type)};
    }
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto * tuple = typeid_cast<const ColumnTuple *>(column.get());
        const auto * own_tuple_type = typeid_cast<const DataTypeTuple *>(own_type.get());
        const auto * own_tuple = typeid_cast<const ColumnTuple *>(own_column.get());
        if (!tuple || !own_tuple_type || !own_tuple || tuple->tupleSize() != own_tuple->tupleSize()
            || tuple->tupleSize() != tuple_type->getElements().size() || tuple->tupleSize() != own_tuple_type->getElements().size())
            return {column, type};
        Columns columns(tuple->tupleSize());
        DataTypes types(tuple->tupleSize());
        bool changed = false;
        for (size_t i = 0; i < columns.size(); ++i)
        {
            std::tie(columns[i], types[i]) = restoreOwnNulls(
                tuple->getColumnPtr(i), tuple_type->getElement(i), own_tuple->getColumnPtr(i), own_tuple_type->getElement(i));
            changed |= columns[i].get() != tuple->getColumnPtr(i).get();
        }
        if (!changed)
            return {column, type};
        return {ColumnTuple::create(columns),
                tuple_type->hasExplicitNames() ? std::make_shared<DataTypeTuple>(types, tuple_type->getElementNames())
                                               : std::make_shared<DataTypeTuple>(types)};
    }
    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        const auto * map = typeid_cast<const ColumnMap *>(column.get());
        const auto * own_map_type = typeid_cast<const DataTypeMap *>(own_type.get());
        const auto * own_map = typeid_cast<const ColumnMap *>(own_column.get());
        if (!map || !own_map_type || !own_map)
            return {column, type};
        const auto & entries = map->getNestedData();
        const auto & own_entries = own_map->getNestedData();
        auto [keys, keys_type] = restoreOwnNulls(
            entries.getColumnPtr(0), map_type->getKeyType(), own_entries.getColumnPtr(0), own_map_type->getKeyType());
        auto [values, values_type] = restoreOwnNulls(
            entries.getColumnPtr(1), map_type->getValueType(), own_entries.getColumnPtr(1), own_map_type->getValueType());
        if (keys.get() == entries.getColumnPtr(0).get() && values.get() == entries.getColumnPtr(1).get())
            return {column, type};
        return {ColumnMap::create(ColumnArray::create(ColumnTuple::create(Columns{keys, values}), map->getNestedColumn().getOffsetsPtr())),
                std::make_shared<DataTypeMap>(keys_type, values_type)};
    }
    return {column, type};
}

/// Whether `subcolumn` names a subcolumn the type declares (an element of a tuple, `keys` of a map), which
/// `IDataType::getSubcolumnInfo` prefers to any key lookup of a map along the name: a field `u.m.key_1` of a
/// struct is taken instead of the key `1` of the map `m` of its field `u`. With case-insensitive matching a
/// name that differs from a declared one only by case resolves to it too.
bool isDeclaredSubcolumn(const DataTypePtr & type, std::string_view subcolumn, bool case_insensitive)
{
    for (const auto & name : type->getSubcolumnNames())
    {
        if (case_insensitive ? boost::iequals(name, subcolumn) : name == subcolumn)
            return true;
    }
    return false;
}

/// The type a column of the decoded type `type` gets when its raw bytes are reinterpreted for `hint` (see
/// `reinterpretRawBytes`). No value is converted, so a `fixed_size_binary` leaf of a width the requested type
/// cannot be read from does not throw here: the caller only asks whether a candidate resolves a name, and a
/// candidate that does not must not fail the query (`u` with `m Map(UInt8, FixedString(1))` for the lookup
/// `u.m.key_foo`, which the field `u.m` resolves). The width is checked when the chosen source is converted.
DataTypePtr typeConvertedByHint(const DataTypePtr & type, const DataTypePtr & hint, bool case_insensitive, const FormatSettings & format_settings)
{
    return reinterpretRawBytes(
        type->createColumn(), type, hint, /*ancestor_nulls=*/nullptr, case_insensitive, /*field=*/nullptr, format_settings,
        /*repair_variants=*/true, /*type_only=*/true).second;
}

/// The requested type of the whole decoded column of type `type` implied by a requested subcolumn of it:
/// `subcolumn` is the dotted name below the column (`key_1`, `u.key_1`, `values.key_1`) and `subcolumn_type`
/// is its requested type. Returns null unless the path goes through a dynamic key lookup of a map (`key_1`).
/// A key lookup synthesizes a default for every row without the key, and that default has the type of the
/// decoded values: when they keep their raw bytes (see `RecordBatchDecoder::rawByteTarget`), it is an empty
/// string, which cannot be reinterpreted after the extraction. So the values are converted before it, and
/// the defaults are then created in the requested type, as when the values are converted while decoding.
/// The path is resolved as `IDataType::getSubcolumnInfo` resolves it: a key lookup takes the whole rest of the
/// name as the serialized key (`key_a.b` looks up `a.b`), and `keys` / `values` of a map are arrays of its keys
/// and values, below which a lookup applies to each element (`t.values.key_1` for a map of maps).
/// `requested` (may be null) is the type requested for the column at this level, when it is requested too
/// (otherwise it is looked up by its dotted name `path` with `find_requested`, which has an `Array` for each of
/// the `array_depth` arrays above): the keys of a map are converted to the key type of the map or of its `keys`
/// subcolumn, so that the lookup parses the key in the representation it is requested with
/// (`t Map(IPv6, UInt8)` or `t.keys Array(IPv6)`, and `t.key_6162:...:6f70`). Arrays are transparent: a key
/// lookup below an array (`t Array(Map(UInt8, IPv6))` and `t.key_1`) has the type `Array(IPv6)`, and is converted
/// element-wise.
/// The caller checks that the name is not a declared subcolumn (see `isDeclaredSubcolumn`): that one is taken
/// instead of a key lookup.
DataTypePtr keyLookupSourceHint(
    const DataTypePtr & type, std::string_view subcolumn, const DataTypePtr & subcolumn_type,
    DataTypePtr requested, const String & path, size_t array_depth, const FindRequestedType & find_requested,
    bool case_insensitive, const FormatSettings & format_settings)
{
    if (!requested)
        requested = peelArrays(find_requested(path), array_depth);
    const DataTypePtr stripped = removeNullable(type);
    const DataTypePtr requested_stripped = ArrowIPC::stripHint(requested);
    const size_t dot = subcolumn.find('.');
    const std::string_view component = subcolumn.substr(0, dot);
    const std::string_view rest = dot == std::string_view::npos ? std::string_view{} : subcolumn.substr(dot + 1);
    if (component.empty())
        return nullptr;
    auto child_path = [&](std::string_view name) { return childRequestPath(path, name, case_insensitive); };

    if (const auto * array = typeid_cast<const DataTypeArray *>(stripped.get()))
    {
        const auto * subcolumn_array = typeid_cast<const DataTypeArray *>(removeNullable(subcolumn_type).get());
        if (!subcolumn_array)
            return nullptr;
        DataTypePtr element_hint = keyLookupSourceHint(
            array->getNestedType(), subcolumn, subcolumn_array->getNestedType(), ArrowIPC::arrayElementHint(requested_stripped),
            path, array_depth + 1, find_requested, case_insensitive, format_settings);
        if (!element_hint)
            return nullptr;
        return std::make_shared<DataTypeArray>(element_hint);
    }

    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(stripped.get()))
    {
        /// The name of an element can contain dots (`u.m`), and the elements of an unnamed tuple are named by
        /// their positions (`1`), as the subcolumns of a tuple are. Several elements can be a prefix of the name
        /// (`u` with a field `m`, and `u.m`): `IDataType::getSubcolumnInfo` takes the lookup from the last element
        /// that resolves the rest of the name, so the elements are tried from the last one, and the one that
        /// resolves it is the only one converted. The prefix is compared as it is spelled: a name that is not
        /// declared is not matched case-insensitively (see `NestedColumnExtractHelper::resolveSubcolumn`).
        for (size_t position = tuple->getElements().size(); position > 0; --position)
        {
            const size_t i = position - 1;
            const String element_name = tuple->getNameByPosition(i + 1);
            if (subcolumn.size() <= element_name.size() + 1 || subcolumn[element_name.size()] != '.'
                || !subcolumn.starts_with(element_name))
                continue;
            const std::string_view element_subcolumn = subcolumn.substr(element_name.size() + 1);
            DataTypePtr element_hint = keyLookupSourceHint(
                tuple->getElement(i), element_subcolumn, subcolumn_type,
                tuple->hasExplicitNames() ? ArrowIPC::tupleElementHint(requested_stripped, element_name, i, case_insensitive)
                                          : ArrowIPC::tupleElementHintByPosition(requested_stripped, i),
                child_path(element_name), array_depth, find_requested, case_insensitive, format_settings);
            /// Whether the element resolves the rest of the name is decided with its type as converted by the
            /// hint, which is what the subcolumn is extracted from: a key lookup parses the key in the type of
            /// the keys, and `key_6162:...:6f70` is a key of the `IPv6` keys the `FixedString(16)` ones become,
            /// not of the decoded ones.
            if (!element_hint || !typeConvertedByHint(tuple->getElement(i), element_hint, case_insensitive, format_settings)
                    ->tryGetSubcolumnType(element_subcolumn))
            {
                /// Not converted: the element is taken as decoded if it resolves the name, and skipped otherwise.
                if (tuple->getElement(i)->tryGetSubcolumnType(element_subcolumn))
                    return nullptr;
                continue;
            }
            /// A named tuple hint with only this element: `reinterpretRawBytes` leaves the other elements as
            /// decoded. An unnamed tuple is matched by position, so the other elements get their decoded types,
            /// which keep them as they are.
            if (tuple->hasExplicitNames())
                return std::make_shared<DataTypeTuple>(DataTypes{element_hint}, Strings{element_name});
            DataTypes elements = tuple->getElements();
            elements[i] = std::move(element_hint);
            return std::make_shared<DataTypeTuple>(std::move(elements));
        }
        return nullptr;
    }

    if (const auto * map = typeid_cast<const DataTypeMap *>(stripped.get()))
    {
        const auto * requested_map = typeid_cast<const DataTypeMap *>(requested_stripped.get());
        if (component.starts_with("key_"))
        {
            DataTypePtr key_hint = map->getKeyType();
            if (requested_map)
                key_hint = requested_map->getKeyType();
            else if (DataTypePtr requested_keys = requestedTypeOrSubcolumns(
                         map->getKeyType(), child_path("keys"), array_depth + 1, find_requested, case_insensitive))
                key_hint = requested_keys;
            /// The whole rest of the name is the serialized key, so the looked-up value has the requested type.
            return std::make_shared<DataTypeMap>(key_hint, subcolumn_type);
        }
        if ((component == "keys" || component == "values") && !rest.empty())
        {
            const bool keys = component == "keys";
            const DataTypePtr & element_type = keys ? map->getKeyType() : map->getValueType();
            DataTypePtr requested_elements;
            if (requested_map)
                requested_elements = std::make_shared<DataTypeArray>(keys ? requested_map->getKeyType() : requested_map->getValueType());
            DataTypePtr elements_hint = keyLookupSourceHint(
                std::make_shared<DataTypeArray>(element_type), rest, subcolumn_type, requested_elements, child_path(component),
                array_depth, find_requested, case_insensitive, format_settings);
            if (!elements_hint)
                return nullptr;
            const DataTypePtr element_hint = assert_cast<const DataTypeArray &>(*elements_hint).getNestedType();
            return std::make_shared<DataTypeMap>(keys ? element_hint : map->getKeyType(), keys ? map->getValueType() : element_hint);
        }
    }

    return nullptr;
}

}

namespace
{

void reinterpretRawByteColumnsImpl(
    ColumnWithTypeAndName & column, const DataTypePtr & to_type, const ArrowIPC::ArrowField * field, const FormatSettings & format_settings,
    bool repair_variants)
{
    const auto * constant = typeid_cast<const ColumnConst *>(column.column.get());
    auto [new_column, new_type] = reinterpretRawBytes(
        constant ? constant->getDataColumnPtr() : column.column, column.type, to_type,
        /*ancestor_nulls=*/nullptr, format_settings.arrow.case_insensitive_column_matching, field, format_settings, repair_variants);
    if (constant)
        new_column = ColumnConst::create(new_column, constant->size());
    column.column = std::move(new_column);
    column.type = std::move(new_type);
}

}

void ArrowIPCBlockInputFormat::reinterpretRawByteColumns(
    ColumnWithTypeAndName & column, const DataTypePtr & to_type, const ArrowIPC::ArrowField * field, bool repair_variants) const
{
    reinterpretRawByteColumnsImpl(column, to_type, field, format_settings, repair_variants);
}

ColumnPtr ArrowIPCBlockInputFormat::decodeGeoColumn(const ColumnPtr & source, const GeoColumnMetadata & geo_metadata, bool precise_float_parsing)
{
    DataTypePtr type = getGeoDataType(geo_metadata.type);
    MutableColumnPtr column = type->createColumn();

    const IColumn * data = source.get();
    const ColumnNullable * nullable = nullptr;
    if (source->isNullable())
    {
        nullable = &assert_cast<const ColumnNullable &>(*source);
        data = &nullable->getNestedColumn();
    }
    /// The schema-level "geo" metadata is untrusted: a file may tag a non-binary field (e.g. an Int32 or a
    /// list) as geometry. Reject it with a clean INCORRECT_DATA instead of letting the cast below misbehave
    /// (a bad-cast exception in checked builds, undefined behavior in release). WKB/WKT geometry is always a
    /// variable-length binary column, which decodes to ColumnString.
    const auto * strings_ptr = typeid_cast<const ColumnString *>(data);
    if (!strings_ptr)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow column tagged as geometry in the \"geo\" metadata must decode to a binary/string column, but got {}",
            data->getName());
    const auto & strings = *strings_ptr;

    for (size_t i = 0; i < strings.size(); ++i)
    {
        if (nullable && nullable->isNullAt(i))
        {
            column->insertDefault();
            continue;
        }
        const std::string_view ref = strings.getDataAt(i);
        ReadBuffer in_buffer(const_cast<char *>(ref.data()), ref.size(), 0);
        GeometricObject object = geo_metadata.encoding == GeoEncoding::WKB
            ? parseWKBFormat(in_buffer) : parseWKTFormat(in_buffer, precise_float_parsing);
        appendObjectToGeoColumn(object, geo_metadata.type, *column);
    }
    return column;
}

Chunk ArrowIPCBlockInputFormat::buildChunk(ArrowIPC::RecordBatchDecoder::DecodedColumns & decoded, size_t num_rows)
{
    return ArrowIPC::buildChunk(
        getPort().getHeader(),
        decoded,
        num_rows,
        format_settings,
        format_settings.arrow.allow_missing_columns,
        geo_columns,
        format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr,
        requested_field_target_types,
        visibility_only_structs);
}

Chunk ArrowIPC::buildChunk(
    const Block & header,
    RecordBatchDecoder::DecodedColumns & decoded,
    size_t num_rows,
    const FormatSettings & format_settings,
    bool allow_missing_columns,
    const UnorderedMapWithMemoryTracking<String, GeoColumnMetadata> & geo_columns,
    BlockMissingValues * block_missing_values_ptr,
    const RequestedTypes & requested_field_target_types,
    const RecordBatchDecoder::VisibilityOnlyStructs & visibility_only_structs)
{
    /// Map decoded column name -> index (lower-cased when case-insensitive matching is on).
    const bool case_insensitive = format_settings.arrow.case_insensitive_column_matching;
    UnorderedMapWithMemoryTracking<String, size_t> name_to_index;
    name_to_index.reserve(decoded.size());
    for (size_t i = 0; i < decoded.size(); ++i)
    {
        String key = decoded[i].name;
        if (case_insensitive)
            boost::to_lower(key);
        /// With case-insensitive matching, names like `A` and `a` fold to the same key. Keep the last
        /// occurrence (`insert_or_assign`, not `emplace`) so the native reader selects the same column the
        /// Apache Arrow library path did for the same schema and setting.
        name_to_index.insert_or_assign(std::move(key), i);
    }

    /// Cache of `Nested`/struct table extractors keyed by the (possibly lower-cased) nested table name.
    /// The backing block must outlive the extractor (it holds a reference into it), so keep both.
    UnorderedMapWithMemoryTracking<String, std::pair<std::shared_ptr<Block>, std::shared_ptr<NestedColumnExtractHelper>>> nested_extractors;
    /// The same for the decoded columns without the null maps the decoder keeps only for the post-decode
    /// conversion (see `dropVisibilityOnlyNulls`). No extractor for a column that has none of them.
    UnorderedMapWithMemoryTracking<String, std::pair<std::shared_ptr<Block>, std::shared_ptr<NestedColumnExtractHelper>>> own_nulls_extractors;

    Columns columns;
    columns.reserve(header.columns());
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(i);
        String search_name = header_column.name;
        if (case_insensitive)
            boost::to_lower(search_name);

        ColumnWithTypeAndName column;
        /// Set only for a column taken whole from the batch. A column extracted out of a flattened Nested
        /// table below is a piece of another field, so it has no field of its own to read metadata from.
        const ArrowIPC::ArrowField * src_field = nullptr;
        auto it = name_to_index.find(search_name);
        if (it == name_to_index.end())
        {
            bool read_from_nested = false;

            /// A subcolumn of a `Nested`/struct field: `table.a` is extracted from the Arrow field `table`.
            const String nested_table_name = Nested::extractTableName(header_column.name);
            String search_nested = nested_table_name;
            if (case_insensitive)
                boost::to_lower(search_nested);

            auto nested_it = name_to_index.find(search_nested);
            DataTypePtr key_lookup_hint;
            if (nested_it != name_to_index.end() && header_column.name.size() > nested_table_name.size() + 1)
            {
                /// The types requested for the column and its subcolumns (see `keyLookupSourceHint`). A name that
                /// several header columns share (see `ArrowIPC::RequestedType::shared`) has several of them, and
                /// `shared_choices` tells which one is taken: the lookup is resolved with each combination.
                struct SharedChoice
                {
                    String name;
                    size_t index = 0;
                    size_t count = 0;
                };
                VectorWithMemoryTracking<SharedChoice> shared_choices;
                auto find_requested = [&](const String & name) -> DataTypePtr
                {
                    auto requested_it = requested_field_target_types.find(name);
                    if (requested_it == requested_field_target_types.end())
                        return nullptr;
                    const auto & shared_types = requested_it->second.shared_types;
                    if (shared_types.size() < 2)
                        return requested_it->second.type;
                    auto choice = std::ranges::find(shared_choices, name, &SharedChoice::name);
                    if (choice == shared_choices.end())
                    {
                        shared_choices.push_back({.name = name, .index = 0, .count = shared_types.size()});
                        choice = std::prev(shared_choices.end());
                    }
                    return shared_types[choice->index];
                };
                /// The next combination, as an odometer: the names found with the current one are the last digits.
                /// Each shared name multiplies the number of combinations (every element of a tuple key of a map
                /// can be requested through its own shared subcolumn), so it is bounded: resolving a lookup must
                /// not take time exponential in the number of the requested columns.
                static constexpr size_t max_shared_combinations = 256;
                size_t shared_combinations = 1;
                auto next_shared_choices = [&]
                {
                    for (auto & choice : shared_choices | std::views::reverse)
                    {
                        if (++choice.index < choice.count)
                        {
                            if (++shared_combinations > max_shared_combinations)
                                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Cannot resolve the map key lookup {}: the columns requested with the same "
                                    "case-insensitive names and different types make more than {} combinations of types "
                                    "for the keys of the map. Request these columns with one type each, "
                                    "or disable case-insensitive column matching",
                                    backQuote(header_column.name), max_shared_combinations);
                            return true;
                        }
                        choice.index = 0;
                    }
                    return false;
                };

                const auto & decoded_type = decoded[nested_it->second].type;
                const std::string_view subcolumn = std::string_view(header_column.name).substr(nested_table_name.size() + 1);
                if (!isDeclaredSubcolumn(decoded_type, subcolumn, case_insensitive))
                {
                    key_lookup_hint = keyLookupSourceHint(
                        decoded_type, subcolumn, header_column.type, /*requested=*/nullptr, search_nested,
                        /*array_depth=*/0, find_requested, case_insensitive, format_settings);

                    /// The columns that share a name can request different representations of the keys of a map
                    /// (`T Map(String, UInt8)` and `t Map(IPv6, UInt8)`), and the lookup parses its key in the
                    /// type of the keys: `t.key_6162:...:6f70` is a key of the `IPv6` keys only, and
                    /// `t.key_abcdefghijklmnop` of the `String` ones. The lookup is taken from the representation
                    /// that resolves it, whatever the order of the columns in the header. If several do, the
                    /// one that converts the decoded column is preferred to the one that keeps it as decoded,
                    /// and then the one whose type has the smaller name.
                    if (!shared_choices.empty())
                    {
                        struct Candidate
                        {
                            bool resolves = false;
                            bool converts = false;
                            String name;
                        };
                        auto candidate_of = [&](const DataTypePtr & hint)
                        {
                            const DataTypePtr source_type = hint
                                ? typeConvertedByHint(decoded_type, hint, case_insensitive, format_settings) : decoded_type;
                            return Candidate{
                                .resolves = source_type->tryGetSubcolumnType(subcolumn) != nullptr,
                                .converts = !source_type->equals(*decoded_type),
                                .name = hint ? hint->getName() : String{}};
                        };
                        auto preferred = [](const Candidate & a, const Candidate & b)
                        {
                            if (a.resolves != b.resolves)
                                return a.resolves;
                            if (a.converts != b.converts)
                                return a.converts;
                            return a.name < b.name;
                        };
                        Candidate best = candidate_of(key_lookup_hint);
                        while (next_shared_choices())
                        {
                            DataTypePtr hint = keyLookupSourceHint(
                                decoded_type, subcolumn, header_column.type, /*requested=*/nullptr, search_nested,
                                /*array_depth=*/0, find_requested, case_insensitive, format_settings);
                            Candidate candidate = candidate_of(hint);
                            if (preferred(candidate, best))
                            {
                                best = std::move(candidate);
                                key_lookup_hint = std::move(hint);
                            }
                        }
                    }
                }
            }
            if (key_lookup_hint)
            {
                /// Convert the map values for this column before the key lookup (see `keyLookupSourceHint`).
                /// The conversion is specific to this column, so the extractor is not cached.
                const auto & source = decoded[nested_it->second];
                ColumnWithTypeAndName source_column(source.column, source.type, nested_table_name);
                reinterpretRawByteColumnsImpl(source_column, key_lookup_hint, /*field=*/nullptr, format_settings, /*repair_variants=*/true);
                /// The values are converted, so the null maps kept only for that are not needed anymore.
                if (auto visibility_only_it = visibility_only_structs.find(source.field); visibility_only_it != visibility_only_structs.end())
                    std::tie(source_column.column, source_column.type) = dropVisibilityOnlyNulls(
                        source_column.column, source_column.type, /*position=*/{}, visibility_only_it->second);
                const Block block({std::move(source_column)});
                NestedColumnExtractHelper helper(block, case_insensitive);
                if (auto nested_column = helper.extractColumn(header_column.name))
                {
                    column = *nested_column;
                    if (case_insensitive)
                        column.name = header_column.name;
                    read_from_nested = true;
                }
            }
            else if (nested_it != name_to_index.end())
            {
                auto extractor_it = nested_extractors.find(search_nested);
                if (extractor_it == nested_extractors.end())
                {
                    auto & src = decoded[nested_it->second];
                    ColumnWithTypeAndName nested_column(src.column, src.type, nested_table_name);

                    auto block = std::make_shared<Block>(Block({std::move(nested_column)}));
                    auto helper = std::make_shared<NestedColumnExtractHelper>(*block, case_insensitive);
                    extractor_it = nested_extractors.emplace(search_nested, std::make_pair(block, helper)).first;
                }
                /// The requested spelling, not the lower-cased one: the helper matches names
                /// case-insensitively itself, and an exact element name outranks a folded match.
                if (auto nested_column = extractor_it->second.second->extractColumn(header_column.name))
                {
                    column = *nested_column;
                    if (case_insensitive)
                        column.name = header_column.name;
                    read_from_nested = true;
                }

                /// A struct can keep its null map only for the conversion below, which must skip the rows it
                /// hides (see `RecordBatchDecoder::collectVisibilityOnlyStructs`). No requested column asks for
                /// these nulls, so they must not reach the subcolumn: it is converted with them, and then gets
                /// the nulls it has in the decoded column without these null maps.
                const auto visibility_only_it = visibility_only_structs.find(decoded[nested_it->second].field);
                if (read_from_nested && visibility_only_it != visibility_only_structs.end())
                {
                    auto own_nulls_it = own_nulls_extractors.find(search_nested);
                    if (own_nulls_it == own_nulls_extractors.end())
                    {
                        const auto & src = decoded[nested_it->second];
                        auto [own_column, own_type] = dropVisibilityOnlyNulls(
                            src.column, src.type, /*position=*/{}, visibility_only_it->second);
                        std::shared_ptr<Block> block;
                        std::shared_ptr<NestedColumnExtractHelper> helper;
                        if (own_column.get() != src.column.get())
                        {
                            block = std::make_shared<Block>(Block({ColumnWithTypeAndName(own_column, own_type, nested_table_name)}));
                            helper = std::make_shared<NestedColumnExtractHelper>(*block, case_insensitive);
                        }
                        own_nulls_it = own_nulls_extractors.emplace(search_nested, std::make_pair(block, helper)).first;
                    }
                    if (const auto & helper = own_nulls_it->second.second)
                    {
                        if (auto own = helper->extractColumn(header_column.name))
                        {
                            reinterpretRawByteColumnsImpl(column, header_column.type, /*field=*/nullptr, format_settings, /*repair_variants=*/true);
                            std::tie(column.column, column.type) = restoreOwnNulls(column.column, column.type, own->column, own->type);
                        }
                    }
                }
            }

            if (!read_from_nested)
            {
                if (!allow_missing_columns)
                    throw Exception(
                        ErrorCodes::THERE_IS_NO_COLUMN,
                        "Column '{}' is not present in the Arrow IPC data", header_column.name);

                auto missing = header_column.type->createColumn();
                missing->insertManyDefaults(num_rows);
                if (block_missing_values_ptr)
                    block_missing_values_ptr->setBits(i, num_rows);
                columns.push_back(std::move(missing));
                continue;
            }
        }
        else
        {
            auto & src = decoded[it->second];
            column = ColumnWithTypeAndName(src.column, src.type, src.name);
            src_field = src.field;
        }

        /// GeoParquet: parse the WKB/WKT binary column into the geo type (declared in the schema-level
        /// "geo" metadata, or implied by a `Geometry` header type hint).
        /// The metadata is keyed by the name in the file, which differs from the requested one with
        /// case-insensitive matching.
        auto geo_it = geo_columns.find(src_field ? column.name : header_column.name);
        /// A native (GeoArrow) encoding is nested lists of `struct<x, y>` with the same layout as
        /// the geo type, so it is converted to the geo type by a cast (and then to the requested
        /// type, e.g. `Geometry`, below).
        if (geo_it != geo_columns.end() && geo_it->second.encoding == GeoEncoding::Native)
        {
            const DataTypePtr geo_type = getGeoDataType(geo_it->second.type);
            reinterpretRawByteColumnsImpl(column, geo_type, src_field, format_settings, /*repair_variants=*/true);
            try
            {
                column = prepareArrowColumnForCast(column, geo_type, format_settings, i, block_missing_values_ptr);
                if (format_settings.null_as_default)
                    insertNullAsDefaultIfNeeded(column, {geo_type->createColumn(), geo_type, header_column.name}, i, block_missing_values_ptr);
                column.column = castColumn(column, geo_type);
                column.type = geo_type;
            }
            catch (Exception & e)
            {
                e.addMessage(fmt::format(
                    "while converting GeoParquet column {} from type {} to type {}",
                    backQuote(header_column.name), column.type->getName(), geo_type->getName()));
                throw;
            }
        }
        else if (geo_it != geo_columns.end())
        {
            column.column = ArrowIPCBlockInputFormat::decodeGeoColumn(column.column, geo_it->second, format_settings.precise_float_parsing);
            column.type = getGeoDataType(geo_it->second.type);
        }
        else if (format_settings.parquet.allow_geoparquet_parser && header_column.type->getName() == "Geometry")
        {
            const GeoColumnMetadata mixed{.encoding = GeoEncoding::WKB, .type = GeoType::Mixed, .covering_bbox = std::nullopt};
            column.column = ArrowIPCBlockInputFormat::decodeGeoColumn(column.column, mixed, format_settings.precise_float_parsing);
            column.type = getGeoDataType(GeoType::Mixed);
        }
        else
            reinterpretRawByteColumnsImpl(column, header_column.type, src_field, format_settings, /*repair_variants=*/true);

        /// A column taken whole can contain structs whose null maps the decoder keeps only for the conversion
        /// above and for the conversions of the other columns taken from the same field (see
        /// `RecordBatchDecoder::collectVisibilityOnlyStructs`). No requested column asks for these nulls, and
        /// the cast below does not look into a struct it does not convert element-wise (`t String` with
        /// `t.u.s IPv6`), so they are removed from this column once its own leaves are converted. The decoded
        /// column keeps them for the other columns.
        if (src_field)
        {
            if (auto visibility_only_it = visibility_only_structs.find(src_field); visibility_only_it != visibility_only_structs.end())
                std::tie(column.column, column.type) = dropVisibilityOnlyNulls(
                    column.column, column.type, /*position=*/{}, visibility_only_it->second);
        }

        /// Match differently-cased struct field names against the requested type when case-insensitive
        /// column matching is enabled, so the named-tuple CAST below does not turn them into defaults.
        if (case_insensitive)
            column.type = alignStructFieldNamesCaseInsensitive(column.type, header_column.type);

        try
        {
            column = prepareArrowColumnForCast(column, header_column.type, format_settings, i, block_missing_values_ptr);
            if (format_settings.null_as_default)
                insertNullAsDefaultIfNeeded(column, header_column, i, block_missing_values_ptr);
            column.column = castColumn(column, header_column.type);
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format(
                "while converting column {} from type {} to type {}",
                backQuote(header_column.name), column.type->getName(), header_column.type->getName()));
            throw;
        }
        columns.push_back(std::move(column.column));
    }

    return Chunk(std::move(columns), num_rows);
}

bool ArrowIPCBlockInputFormat::readStreamBatch()
{
    const size_t batch_start = in->count();

    while (!is_stopped)
    {
        ArrowIPC::MessageReader::Message msg;
        if (!message_reader->readNextMessage(msg))
        {
            /// Drain the buffer so the underlying stream is fully consumed (HTTP keepalive, etc.).
            in->eof();
            return false;
        }

        switch (msg.header->header_type())
        {
            case ArrowIPC::flatbuf::MessageHeader_RecordBatch:
            {
                const auto * batch = msg.header->header_as_RecordBatch();
                if (batch->length() < 0)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch has a negative length {}", batch->length());
                batch_rows = static_cast<size_t>(batch->length());

                if (need_only_count)
                {
                    message_reader->skipBody(msg.body_length);
                }
                else
                {
                    /// Read only the body buffers needed by requested columns, skipping all other ranges.
                    const auto reachable = decoder->reachableTopLevelBuffers(*batch, &requested_top_level_fields);
                    message_reader->readBody(*batch, msg.body_length, body_buffer, &reachable);
                    decoded_batch = decoder->decodeBatch(
                        *batch, body_buffer, &requested_top_level_fields, &requested_field_target_types, &reachable);
                }

                approx_bytes_read_for_chunk += in->count() - batch_start;
                return true;
            }
            case ArrowIPC::flatbuf::MessageHeader_DictionaryBatch:
            {
                /// Count-only never decodes a record batch, so the dictionaries are not needed; skip the
                /// body instead of decoding (and possibly failing on) it.
                if (need_only_count)
                {
                    message_reader->skipBody(msg.body_length);
                    continue;
                }
                const auto * dict_batch = msg.header->header_as_DictionaryBatch();
                const Int64 id = dict_batch->id();
                /// Subset reads: a dictionary referenced only by unrequested (skipped) top-level fields is
                /// never used, so skip its body instead of decoding (and possibly failing/allocating on) it.
                /// Check this before validating `data` so a missing/corrupt unrequested dictionary cannot fail
                /// a projected read.
                if (!dictionary_uses.contains(id))
                {
                    message_reader->skipBody(msg.body_length);
                    continue;
                }
                decodeDictionaryBatch(*decoder, *dict_batch, msg.body_length);
                continue;
            }
            case ArrowIPC::flatbuf::MessageHeader_Schema:
                /// A redundant schema message; it carries no body. Ignore and continue.
                message_reader->skipBody(msg.body_length);
                continue;
            default:
                message_reader->skipBody(msg.body_length);
                continue;
        }
    }
    return false;
}

bool ArrowIPCBlockInputFormat::readFileBatch()
{
    if (record_batch_current >= record_batch_blocks.size())
        return false;

    const BlockInfo & block = record_batch_blocks[record_batch_current++];
    seekable->seek(block.offset, SEEK_SET);

    ArrowIPC::MessageReader::Message msg;
    /// Bound the metadata read by the footer block's declared metadata size (see the dictionary loop).
    if (!message_reader->readNextMessage(msg, block.metadata_length) || msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_RecordBatch)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected a record batch in the Arrow file");
    /// The footer block fixes this message's body size; a message claiming a different (e.g. huge) body
    /// length is corrupt and would otherwise force a large allocation reading past the block boundary.
    if (msg.body_length != block.body_length)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC record batch body length {} does not match the footer block length {}",
            msg.body_length, block.body_length);

    const auto * batch = msg.header->header_as_RecordBatch();
    if (batch->length() < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch has a negative length {}", batch->length());
    batch_rows = static_cast<size_t>(batch->length());

    if (!need_only_count)
    {
        /// Read only the body buffers needed by requested columns, skipping all other ranges.
        const auto reachable = decoder->reachableTopLevelBuffers(*batch, &requested_top_level_fields);
        message_reader->readBody(*batch, msg.body_length, body_buffer, &reachable);
        decoded_batch = decoder->decodeBatch(
            *batch, body_buffer, &requested_top_level_fields, &requested_field_target_types, &reachable);
    }

    approx_bytes_read_for_chunk += block.metadata_length + block.body_length;
    return true;
}

Chunk ArrowIPCBlockInputFormat::generateChunkFromBatch()
{
    if (need_only_count)
    {
        batch_offset = batch_rows;
        return getChunkForCount(batch_rows);
    }

    /// Ordinary columns already occupy memory for the whole batch and can be moved without copying.
    /// Batches containing only constants need a row limit before those constants or defaults expand.
    const bool all_constant = std::ranges::all_of(decoded_batch, [](const auto & column)
    {
        return isColumnConst(*column.column);
    });
    const size_t remaining_rows = batch_rows - batch_offset;
    const size_t num_rows = all_constant ? std::min(max_block_size, remaining_rows) : remaining_rows;
    ArrowIPC::RecordBatchDecoder::DecodedColumns decoded;
    if (num_rows == batch_rows)
    {
        decoded = std::move(decoded_batch);
    }
    else
    {
        decoded.reserve(decoded_batch.size());
        for (const auto & column : decoded_batch)
            decoded.push_back({column.name, column.type, column.column->cut(batch_offset, num_rows)});
    }
    batch_offset += num_rows;
    if (batch_offset == batch_rows)
        decoded_batch.clear();

    /// Constants expand after deciding whether the batch requires slicing.
    for (auto & column : decoded)
        column.column = column.column->convertToFullColumnIfConst();
    return buildChunk(decoded, num_rows);
}

Chunk ArrowIPCBlockInputFormat::read()
{
    block_missing_values.clear();
    approx_bytes_read_for_chunk = 0;

    if (!prepared)
        prepareReader();

    while (!is_stopped)
    {
        if (batch_offset < batch_rows)
            return generateChunkFromBatch();

        decoded_batch.clear();
        batch_rows = 0;
        batch_offset = 0;
        if (!(stream ? readStreamBatch() : readFileBatch()))
            return {};
    }
    decoded_batch.clear();
    return {};
}

const BlockMissingValues * ArrowIPCBlockInputFormat::getMissingValues() const
{
    return &block_missing_values;
}

void ArrowIPCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();
    prepared = false;
    decoder.reset();
    arrow_schema.reset();
    geo_columns.clear();
    message_reader.reset();
    dictionary_value_fields.clear();
    dictionary_uses.clear();
    dictionaries.clear();
    decoded_batch.clear();
    batch_rows = 0;
    batch_offset = 0;
    record_batch_blocks.clear();
    record_batch_current = 0;
    seekable = nullptr;
    memory_buffer.reset();
    file_data.reset();
    block_missing_values.clear();
    approx_bytes_read_for_chunk = 0;
}

void registerInputFormatArrow(FormatFactory & factory);
void registerInputFormatArrow(FormatFactory & factory)
{
    factory.registerInputFormat(
        "Arrow",
        [](ReadBuffer & buf,
           const Block & sample,
           const RowInputFormatParams & params,
           const FormatSettings & format_settings) -> InputFormatPtr
        {
            return std::make_shared<ArrowIPCBlockInputFormat>(
                buf, std::make_shared<const Block>(sample), false, params.max_block_size_rows, format_settings);
        }, /*supports_reset_parser=*/ true);
    factory.markFormatSupportsSubsetOfColumns("Arrow");
    factory.registerInputFormat(
        "ArrowStream",
        [](ReadBuffer & buf,
           const Block & sample,
           const RowInputFormatParams & params,
           const FormatSettings & format_settings) -> InputFormatPtr
        {
            return std::make_shared<ArrowIPCBlockInputFormat>(
                buf, std::make_shared<const Block>(sample), true, params.max_block_size_rows, format_settings);
        }, /*supports_reset_parser=*/ true);
    factory.markFormatSupportsSubsetOfColumns("ArrowStream");

    factory.setDocumentation("Arrow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache Arrow](https://arrow.apache.org/) comes with two built-in columnar storage formats.
ClickHouse supports read and write operations for these formats.
`Arrow` is Apache Arrow's "file mode" format, designed for in-memory random access.

## Data types matching {#data-types-matching}

The table below shows the supported data types and how they correspond to ClickHouse [data types](/reference/data-types/index) in `INSERT` and `SELECT` queries.

| Arrow data type (`INSERT`)              | ClickHouse data type                                                                                       | Arrow data type (`SELECT`) |
|-----------------------------------------|------------------------------------------------------------------------------------------------------------|----------------------------|
| `BOOL`                                  | [Bool](/reference/data-types/boolean)                                                       | `BOOL`                     |
| `UINT8`, `BOOL`                         | [UInt8](/reference/data-types/int-uint)                                                     | `UINT8`                    |
| `INT8`                                  | [Int8](/reference/data-types/int-uint)/[Enum8](/reference/data-types/enum)   | `INT8`                     |
| `UINT16`                                | [UInt16](/reference/data-types/int-uint)                                                    | `UINT16`                   |
| `INT16`                                 | [Int16](/reference/data-types/int-uint)/[Enum16](/reference/data-types/enum) | `INT16`                    |
| `UINT32`                                | [UInt32](/reference/data-types/int-uint)                                                    | `UINT32`                   |
| `INT32`                                 | [Int32](/reference/data-types/int-uint)                                                     | `INT32`                    |
| `UINT64`                                | [UInt64](/reference/data-types/int-uint)                                                    | `UINT64`                   |
| `INT64`                                 | [Int64](/reference/data-types/int-uint)                                                     | `INT64`                    |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/reference/data-types/float)                                                      | `FLOAT32`                  |
| `DOUBLE`                                | [Float64](/reference/data-types/float)                                                      | `FLOAT64`                  |
| `DATE32`                                | [Date32](/reference/data-types/date32)                                                      | `UINT16`                   |
| `DATE64`                                | [DateTime](/reference/data-types/datetime)                                                  | `UINT32`                   |
| `TIMESTAMP`                             | [DateTime64](/reference/data-types/datetime64)                                              | `TIMESTAMP`                |
| `TIME32`, `TIME64`                      | [Time64](/reference/data-types/time64)                                              | `TIME32`, `TIME64`                |
| `STRING`, `BINARY`                      | [String](/reference/data-types/string)                                                      | `BINARY`                   |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/reference/data-types/fixedstring)                                            | `FIXED_SIZE_BINARY`        |
| `DECIMAL`                               | [Decimal](/reference/data-types/decimal)                                                    | `DECIMAL`                  |
| `DECIMAL256`                            | [Decimal256](/reference/data-types/decimal)                                                 | `DECIMAL256`               |
| `LIST`, `LIST_VIEW`                     | [Array](/reference/data-types/array)                                                        | `LIST`                     |
| `STRUCT`                                | [Tuple](/reference/data-types/tuple)                                                        | `STRUCT`                   |
| `MAP`                                   | [Map](/reference/data-types/map)                                                            | `MAP`                      |
| `UINT32`                                | [IPv4](/reference/data-types/ipv4)                                                          | `UINT32`                   |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/reference/data-types/ipv6)                                                          | `FIXED_SIZE_BINARY`        |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/reference/data-types/int-uint)                             | `FIXED_SIZE_BINARY`        |
| `DURATION`                              | [Interval](/reference/data-types/special-data-types/interval) (Nanosecond/Microsecond/Millisecond/Second) | `DURATION`    |
| `INT64`                                 | [Interval](/reference/data-types/special-data-types/interval) (Minute/Hour/Day/Week/Month/Quarter/Year) | `INT64`         |
| `INTERVAL`                              | [Interval](/reference/data-types/special-data-types/interval) (Month), or a [Tuple](/reference/data-types/tuple) of intervals for `DAY_TIME` and `MONTH_DAY_NANO` | `INT64`, `STRUCT` |

Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types can also be nested.

The `DICTIONARY` type is supported for `INSERT` queries, and for `SELECT` queries there is an [`output_format_arrow_low_cardinality_as_dictionary`](/reference/settings/formats/output-format#output_format_arrow_low_cardinality_as_dictionary) setting that allows to output [LowCardinality](/reference/data-types/lowcardinality) type as a `DICTIONARY` type. Note that there might be unused values in `LowCardinality` dictionary, which can lead to unused values in Arrow `DICTIONARY` during output.

Unsupported Arrow data types:
- `JSON`
- `ENUM`.

The data types of ClickHouse table columns do not have to match the corresponding Arrow data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/reference/functions/regular-functions/type-conversion-functions#CAST) the data to the data type set for the ClickHouse table column.

## Example usage {#example-usage}

In the example below we use the `forex` dataset available in the
[ClickHouse SQL playground](https://sql.clickhouse.com).

### Selecting data {#selecting-data}

We select one day of `EUR/USD` exchange rates from the playground and save it
into a local `forex_eurusd.arrow` file. We query the playground over the HTTP
interface, where the host is `sql-clickhouse.clickhouse.com` and the user is
`demo` (which has no password):

```bash
curl "https://sql-clickhouse.clickhouse.com:8443/?user=demo&database=forex" \
    --data-binary "
        SELECT
            concat(base, '.', quote) AS base_quote,
            datetime AS last_update,
            CAST(bid, 'Float32') AS bid,
            CAST(ask, 'Float32') AS ask,
            ask - bid AS spread
        FROM forex
        WHERE base = 'EUR' AND quote = 'USD'
            AND datetime >= '2020-01-01' AND datetime < '2020-01-02'
        ORDER BY datetime ASC
        FORMAT Arrow
        SETTINGS output_format_arrow_compression_method='zstd'" > forex_eurusd.arrow
```

### Reading the file back {#reading-data}

We can now read the local Arrow file back with
[`clickhouse-local`](/concepts/features/tools-and-utilities/clickhouse-local) using the
[`file`](/reference/functions/table-functions/file) table function. The file is
self-describing, so the `Arrow` format infers the schema automatically:

```bash
clickhouse-local --query "
    SELECT *
    FROM file('forex_eurusd.arrow', Arrow)
    ORDER BY last_update ASC
    LIMIT 5
    FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬─────bid─┬─────ask─┬────────────────spread─┐
1. │ EUR.USD    │ 2020-01-01 17:00:00.065 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
2. │ EUR.USD    │ 2020-01-01 17:00:10.447 │  1.1212 │ 1.12192 │ 0.0007200241088867188 │
3. │ EUR.USD    │ 2020-01-01 17:00:10.498 │ 1.12117 │ 1.12161 │ 0.0004400014877319336 │
4. │ EUR.USD    │ 2020-01-01 17:00:12.579 │  1.1212 │ 1.12161 │ 0.0004100799560546875 │
5. │ EUR.USD    │ 2020-01-01 17:00:12.630 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
   └────────────┴─────────────────────────┴─────────┴─────────┴───────────────────────┘
```

### Inserting data {#inserting-data}

To load an Arrow file into a ClickHouse table, pipe it into `clickhouse-client`
with `FORMAT Arrow`:

```bash
cat forex_eurusd.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

## Format settings {#format-settings}

| Setting                                                                                                                  | Description                                                                                        | Default      |
|--------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|--------------|
| `input_format_arrow_allow_missing_columns`                                                                               | Allow missing columns while reading Arrow input formats                                            | `1`          |
| `input_format_arrow_case_insensitive_column_matching`                                                                    | Ignore case when matching Arrow columns with CH columns.                                           | `0`          |
| `input_format_arrow_import_nested`                                                                                       | Obsolete setting, does nothing.                                                                    | `0`          |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`                                             | Skip columns with unsupported types while schema inference for format Arrow                        | `0`          |
| `output_format_arrow_compression_method`                                                                                 | Compression method for Arrow output format. Supported codecs: lz4_frame, zstd, none (uncompressed) | `lz4_frame`  |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                                                                   | Use Arrow FIXED_SIZE_BINARY type instead of Binary for FixedString columns.                        | `1`          |
| `output_format_arrow_low_cardinality_as_dictionary`                                                                      | Enable output LowCardinality type as Dictionary Arrow type                                         | `0`          |
| `output_format_arrow_record_batch_size`                                                                                  | Target rows per record batch when combining small blocks. Buffering can increase memory use and delay the first batch until the query finishes. `0` disables the row target. | `0`          |
| `output_format_arrow_record_batch_size_bytes`                                                                            | Target bytes of accumulated block data per record batch. Buffering can increase memory use and delay the first batch until the query finishes. `0` disables the byte target.              | `0`          |
| `output_format_arrow_string_as_string`                                                                                   | Use Arrow String type instead of Binary for String columns                                         | `1`          |
| `output_format_arrow_unsupported_types`                                                                                  | What to write for a type that has no Arrow equivalent (e.g. `JSON`, `Dynamic`, `QBit`, `AggregateFunction`): `throw`, `text` (one `serializeText` value per row, in whichever Arrow type a `String` column would use) or `binary` (one `serializeBinary` value per row, as Arrow `Binary`). An `AggregateFunction` is `Binary` in `text` mode too, since its text form is the raw aggregate state. | `binary`     |
| `output_format_arrow_unsupported_types_as_binary`                                                                        | Superseded by `output_format_arrow_unsupported_types`: `0` means `throw`, `1` means `binary`. Only consulted while that setting is left at its default. | `1`          |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                                                                  | Always use 64 bit integers for dictionary indexes in Arrow format                                  | `0`          |
| `output_format_arrow_use_signed_indexes_for_dictionary`                                                                  | Use signed integers for dictionary indexes in Arrow format                                         | `1`          |
)DOCS_MD"});

    factory.setDocumentation("ArrowStream", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

`ArrowStream` is Apache Arrow's "stream mode" format. It is designed for in-memory stream processing.

## Example usage {#example-usage}

In the example below we use the `forex` dataset which is available in the
[ClickHouse SQL playground](https://sql.clickhouse.com). You can connect to it
remotely with `clickhouse-client` using the host `sql-clickhouse.clickhouse.com`
and the user `demo` (which has no password). The `forex` table lives in the
`forex` database, so we select it as the default database:

```bash
clickhouse-client --secure --host sql-clickhouse.clickhouse.com --user demo --database forex
```

The `forex` table stores currency exchange rates. We can inspect its size and
how well it compresses on disk by querying [`system.columns`](/reference/system-tables/columns):

```sql title="Query"
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    sum(data_compressed_bytes) / sum(data_uncompressed_bytes) AS compression_ratio
FROM system.columns
WHERE (database = 'forex') AND (table = 'forex')
GROUP BY table
ORDER BY table ASC
```

```response title="Response"
   ┌─table─┬─compressed_size─┬─uncompressed_size─┬───compression_ratio─┐
1. │ forex │ 63.69 GiB       │ 280.48 GiB        │ 0.22708227109363446 │
   └───────┴─────────────────┴───────────────────┴─────────────────────┘
```

Unlike the [`Arrow`](/reference/formats/Arrow/Arrow) "file mode" format, which
requires the whole result before it can be read, `ArrowStream` is delivered as a
sequence of record batches that a consumer can read incrementally as they
arrive. This makes it well suited to streaming a query result straight into a
visualization or analytics tool without first materializing the entire dataset.

To stream the result, send the query over ClickHouse's HTTP interface with a
`POST` request and read the response as an Arrow stream. We disable compression
of the Arrow output via the
[`output_format_arrow_compression_method`](/reference/settings/formats/output-format#output_format_arrow_compression_method)
setting so that consumers can decode batches directly as they are received.

The `ArrowStream` output is raw binary, so rather than printing it to the
terminal we pipe it into a consumer. The stream is self-describing (it carries
its own schema), so here we pipe it straight into
[`clickhouse-local`](/concepts/features/tools-and-utilities/clickhouse-local), which reads the
incoming batches with `--input-format ArrowStream` and queries them as a table.
The `forex` table is large, so we bound the remote query with a `WHERE`
predicate and a `LIMIT` to keep this example small:

```bash
curl "https://sql-clickhouse.clickhouse.com:8443/?user=demo&database=forex" \
    --data-binary "
        SELECT
            concat(base, '.', quote) AS base_quote,
            datetime AS last_update,
            CAST(bid, 'Float32') AS bid,
            CAST(ask, 'Float32') AS ask,
            ask - bid AS spread
        FROM forex
        WHERE base = 'USD' AND quote = 'CHF'
        ORDER BY datetime ASC
        LIMIT 5
        FORMAT ArrowStream
        SETTINGS output_format_arrow_compression_method='none'" \
  | clickhouse-local --input-format ArrowStream \
      --query "SELECT * FROM table ORDER BY last_update ASC FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬────bid─┬────ask─┬────────────────spread─┐
1. │ USD.CHF    │ 2000-05-30 17:23:44.000 │  1.688 │ 1.6885 │ 0.0005000829696655273 │
2. │ USD.CHF    │ 2000-05-30 17:23:46.000 │ 1.6885 │  1.689 │ 0.0004999637603759766 │
3. │ USD.CHF    │ 2000-05-30 17:23:48.000 │ 1.6886 │ 1.6891 │ 0.0005000829696655273 │
4. │ USD.CHF    │ 2000-05-30 17:23:49.000 │ 1.6888 │ 1.6893 │ 0.0004999637603759766 │
5. │ USD.CHF    │ 2000-05-30 17:24:45.000 │  1.689 │ 1.6895 │ 0.0004999637603759766 │
   └────────────┴─────────────────────────┴────────┴────────┴───────────────────────┘
```

The same stream can be consumed incrementally by any Arrow-aware client, which
reads it batch-by-batch rather than buffering the result in full. For example,
using the [Apache Arrow JavaScript library](https://arrow.apache.org/docs/js/), a
`RecordBatchReader` yields each record batch as soon as it is streamed from the
server:

```js
const reader = await RecordBatchReader.from(response);
await reader.open();
for await (const recordBatch of reader) {
    const batchTable = new Table(recordBatch);
    const ipcStream = tableToIPC(batchTable, 'stream');
    const bytes = new Uint8Array(ipcStream);
    table.update(bytes);
}
```

For a full walkthrough of streaming `ArrowStream` data from ClickHouse into a
real-time visualization with [Perspective](https://perspective.finos.org/), see
the blog post
[Streaming real-time visualizations with ClickHouse, Apache Arrow and Perspective](https://clickhouse.com/blog/streaming-real-time-visualizations-clickhouse-apache-arrow-perpsective).

## Format settings {#format-settings}

`ArrowStream` shares the same format settings as the [`Arrow`](/reference/formats/Arrow/Arrow) format.

| Setting                                                                      | Description                                                                                                                                | Default     |
|------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `input_format_arrow_allow_missing_columns`                                   | Allow missing columns while reading Arrow input formats                                                                                    | `1`         |
| `input_format_arrow_case_insensitive_column_matching`                        | Ignore case when matching Arrow columns with CH columns.                                                                                   | `0`         |
| `input_format_arrow_import_nested`                                           | Obsolete setting, does nothing.                                                                                                            | `0`         |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | Skip columns with unsupported types while schema inference for format Arrow                                                                | `0`         |
| `output_format_arrow_compression_method`                                     | Compression method for Arrow output format. Supported codecs: lz4_frame, zstd, none (uncompressed)                                         | `lz4_frame` |
| `output_format_arrow_date_as_uint16`                                         | Write Date values as plain 16-bit numbers (read back as UInt16), instead of converting to a 32-bit Arrow DATE32 type (read back as Date32). | `0`         |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | Use Arrow FIXED_SIZE_BINARY type instead of Binary for FixedString columns.                                                                | `1`         |
| `output_format_arrow_low_cardinality_as_dictionary`                          | Enable output LowCardinality type as Dictionary Arrow type                                                                                 | `0`         |
| `output_format_arrow_record_batch_size`                                      | Target rows per record batch when combining small blocks. Buffering can increase memory use and delay the first batch until the query finishes. `0` disables the row target. | `0`         |
| `output_format_arrow_record_batch_size_bytes`                                | Target bytes of accumulated block data per record batch. Buffering can increase memory use and delay the first batch until the query finishes. `0` disables the byte target.                                                      | `0`         |
| `output_format_arrow_string_as_string`                                       | Use Arrow String type instead of Binary for String columns                                                                                 | `1`         |
| `output_format_arrow_unsupported_types`                                      | What to write for a type that has no Arrow equivalent (e.g. `JSON`, `Dynamic`, `QBit`, `AggregateFunction`): `throw`, `text` (one `serializeText` value per row, in whichever Arrow type a `String` column would use) or `binary` (one `serializeBinary` value per row, as Arrow `Binary`). An `AggregateFunction` is `Binary` in `text` mode too, since its text form is the raw aggregate state. | `binary`    |
| `output_format_arrow_unsupported_types_as_binary`                            | Superseded by `output_format_arrow_unsupported_types`: `0` means `throw`, `1` means `binary`. Only consulted while that setting is left at its default. | `1`         |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | Always use 64 bit integers for dictionary indexes in Arrow format                                                                          | `0`         |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | Use signed integers for dictionary indexes in Arrow format                                                                                 | `1`         |
)DOCS_MD"});
}

void registerArrowSchemaReader(FormatFactory & factory);
void registerArrowSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "Arrow",
        [](ReadBuffer & buf, const FormatSettings & settings) -> SchemaReaderPtr
        {
            return std::make_shared<ArrowIPCSchemaReader>(buf, false, settings);
        });

    factory.registerAdditionalInfoForSchemaCacheGetter("Arrow", [](const FormatSettings & settings)
    {
        return fmt::format(
            "schema_inference_make_columns_nullable={};schema_inference_allow_nullable_tuple_type={};"
            "skip_columns_with_unsupported_types={};allow_geoparquet_parser={}",
            settings.schema_inference_make_columns_nullable,
            settings.schema_inference_allow_nullable_tuple_type,
            settings.arrow.skip_columns_with_unsupported_types_in_schema_inference,
            settings.parquet.allow_geoparquet_parser);
    });
    factory.registerSchemaReader(
        "ArrowStream",
        [](ReadBuffer & buf, const FormatSettings & settings) -> SchemaReaderPtr
        {
            return std::make_shared<ArrowIPCSchemaReader>(buf, true, settings);
        });

    factory.registerAdditionalInfoForSchemaCacheGetter("ArrowStream", [](const FormatSettings & settings)
    {
        return fmt::format(
            "schema_inference_make_columns_nullable={};schema_inference_allow_nullable_tuple_type={};"
            "skip_columns_with_unsupported_types={};allow_geoparquet_parser={}",
            settings.schema_inference_make_columns_nullable,
            settings.schema_inference_allow_nullable_tuple_type,
            settings.arrow.skip_columns_with_unsupported_types_in_schema_inference,
            settings.parquet.allow_geoparquet_parser);
    });
}

}

#endif
