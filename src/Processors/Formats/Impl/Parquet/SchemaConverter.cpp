#include <Processors/Formats/Impl/Parquet/SchemaConverter.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>
#include <Formats/FormatFilterInfo.h>
#include <Processors/Formats/Impl/Parquet/Decoding.h>

#include <fmt/ranges.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int DUPLICATE_COLUMN;
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
    extern const int TYPE_MISMATCH;
    extern const int TOO_DEEP_RECURSION;
    extern const int NOT_IMPLEMENTED;
    extern const int THERE_IS_NO_COLUMN;
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

namespace DB::Parquet
{

SchemaConverter::SchemaConverter(
    const parq::FileMetaData & file_metadata_, const ReadOptions & options_,
    const Block * sample_block_)
    : file_metadata(file_metadata_), options(options_), sample_block(sample_block_)
    , levels {LevelInfo {.def = 0, .rep = 0, .is_array = true}}
{
    if (options.format.parquet.allow_geoparquet_parser)
    {
        for (const auto & kv : file_metadata.key_value_metadata)
        {
            if (kv.key == "geo")
            {
                geo_columns = parseGeoMetadataEncoding(&kv.value);
                break;
            }
        }
    }
}

void SchemaConverter::checkHasColumns()
{
    if (file_metadata.schema.size() < 2)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet file has no columns");
    if (file_metadata.schema.at(0).num_children <= 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Schema root has no children");
}

void SchemaConverter::prepareForReading()
{
    chassert(sample_block);
    checkHasColumns();

    /// DFS the schema tree.
    size_t top_level_columns = size_t(file_metadata.schema.at(0).num_children);
    for (size_t i = 0; i < top_level_columns; ++i)
    {
        TraversalNode node;
        processSubtree(node);
    }

    /// Check that all requested columns were found.
    std::vector<UInt8> found_columns(sample_block->columns());
    for (const auto & col : output_columns)
    {
        if (!col.idx_in_output_block.has_value())
            continue;
        size_t idx = col.idx_in_output_block.value();
        if (found_columns.at(idx))
            throw Exception(ErrorCodes::DUPLICATE_COLUMN, "There are multiple columns with name `{}` in the parquet file", sample_block->getByPosition(idx).name);
        found_columns[idx] = true;
    }
    for (const String & name : external_columns)
    {
        size_t idx = sample_block->getPositionByName(name, /* case_insensitive= */ false);
        if (found_columns.at(idx))
            throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Name clash between PREWHERE condition and a column in parquet file: {}", name);
        found_columns[idx] = true;
    }

    for (size_t i = 0; i < found_columns.size(); ++i)
    {
        if (found_columns[i])
            continue;
        if (!options.format.parquet.allow_missing_columns)
            throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Column {} was not found in parquet schema", sample_block->getByPosition(i).name);

        OutputColumnInfo & missing_output = output_columns.emplace_back();
        missing_output.idx_in_output_block = i;
        missing_output.name = sample_block->getByPosition(i).name;
        missing_output.type = sample_block->getByPosition(i).type;
        missing_output.is_missing_column = true;
    }
}

NamesAndTypesList SchemaConverter::inferSchema()
{
    chassert(!sample_block);
    checkHasColumns();
    size_t top_level_columns = size_t(file_metadata.schema.at(0).num_children);
    NamesAndTypesList res;
    for (size_t i = 0; i < top_level_columns; ++i)
    {
        TraversalNode node;
        node.requested = true;
        processSubtree(node);
        if (node.output_idx.has_value())
        {
            const OutputColumnInfo & col = output_columns.at(node.output_idx.value());
            res.emplace_back(col.name, col.type);
        }
    }
    return res;
}

std::string_view SchemaConverter::useColumnMapperIfNeeded(const parq::SchemaElement & element) const
{
    if (!column_mapper)
        return element.name;
    const auto & map = column_mapper->getFieldIdToClickHouseName();
    if (!element.__isset.field_id)
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Missing field_id for column {}", element.name);
    auto it = map.find(element.field_id);
    if (it == map.end())
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Parquet file has column {} with field_id {} that is not in datalake metadata", element.name, element.field_id);
    auto split = Nested::splitName(std::string_view(it->second), /*reverse=*/ true);
    return split.second.empty() ? split.first : split.second;
}

void SchemaConverter::processSubtree(TraversalNode & node)
{
    if (node.type_hint)
        chassert(node.requested);
    if (schema_idx >= file_metadata.schema.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parquet schema tree");
    node.element = &file_metadata.schema.at(schema_idx);
    schema_idx += 1;

    std::optional<size_t> idx_in_output_block;
    size_t wrap_in_arrays = 0;

    if (node.schema_context == SchemaContext::None)
    {
        node.appendNameComponent(node.element->name, useColumnMapperIfNeeded(*node.element));

        if (sample_block)
        {
            /// Doing this lookup on each schema element to support reading individual tuple elements.
            /// E.g.:
            ///   insert into function file('t.parquet') select [(10,20,30)] as x;
            ///   select * from file('t.parquet', Parquet, '`x.2` Array(UInt8)'); -- outputs [20]
            std::optional<size_t> pos = sample_block->findPositionByName(node.name, options.format.parquet.case_insensitive_column_matching);
            if (pos.has_value())
            {
                if (node.requested)
                    throw Exception(ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE, "Requested column {} is part of another requested column", node.getNameForLogging());

                node.requested = true;
                node.name = sample_block->getByPosition(pos.value()).name; // match case
                node.type_hint = sample_block->getByPosition(pos.value()).type;

                for (size_t i = 1; i < levels.size(); ++i)
                {
                    if (levels[i].is_array)
                    {
                        const DataTypeArray * array = typeid_cast<const DataTypeArray *>(node.type_hint.get());
                        if (!array)
                            throw Exception(ErrorCodes::TYPE_MISMATCH, "Requested type of nested column {} doesn't match parquet schema: parquet type is Array, requested type is {}", node.getNameForLogging(), node.type_hint->getName());
                        node.type_hint = array->getNestedType();
                        wrap_in_arrays += 1;
                    }
                }
                chassert(wrap_in_arrays == levels.back().rep);

                idx_in_output_block = pos;
            }
        }
    }

    size_t prev_levels_size = levels.size();
    SCOPE_EXIT(
        {
            chassert(levels.size() >= prev_levels_size);
            levels.resize(prev_levels_size);
        });

    if (node.element->repetition_type != parq::FieldRepetitionType::REQUIRED)
    {
        LevelInfo prev = levels.back();
        if (prev.def == UINT8_MAX)
            throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "Parquet column {} has extremely deeply nested (>255 levels) arrays or nullables", node.getNameForLogging());
        auto level = LevelInfo {.def = UInt8(prev.def + 1), .rep = prev.rep};
        if (node.element->repetition_type == parq::FieldRepetitionType::REPEATED)
        {
            level.rep += 1; // no overflow, rep <= def
            level.is_array = true;

            /// We'll first process schema for array element type, then wrap it in Array type.
            if (node.type_hint)
            {
                const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(node.type_hint.get());
                if (!array_type)
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Requested type of column {} doesn't match parquet schema: parquet type is Array, requested type is {}", node.getNameForLogging(), node.type_hint->getName());
                node.type_hint = array_type->getNestedType();
            }
        }
        chassert(level.def == levels.size());
        levels.push_back(level);
    }

    /// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md

    if (!processSubtreePrimitive(node) &&
        !processSubtreeMap(node) &&
        !processSubtreeArrayOuter(node) &&
        !processSubtreeArrayInner(node))
    {
        processSubtreeTuple(node);
    }

    if (!node.output_idx.has_value())
        return;
    if (!node.requested)
        return; // we just needed to recurse to children, not interested in output_idx

    auto make_array = [&](UInt8 rep)
    {
        size_t array_idx = output_columns.size();
        OutputColumnInfo & array = output_columns.emplace_back();
        const OutputColumnInfo & array_element = output_columns.at(node.output_idx.value());
        array.name = node.name;
        array.primitive_start = array_element.primitive_start;
        array.primitive_end = primitive_columns.size();
        array.type = std::make_shared<DataTypeArray>(array_element.type);
        array.nested_columns = {*node.output_idx};
        array.rep = rep;
        node.output_idx = array_idx;
    };

    if (node.element->repetition_type == parq::FieldRepetitionType::REPEATED)
    {
        /// Array of some kind. Can be a child of List or Map, or a standalone repeated field.
        /// We dispatch all 3 cases to this one code path to minimize probability of bugs.
        chassert(levels.size() == prev_levels_size + 1);
        make_array(levels.back().rep);
    }

    if (idx_in_output_block.has_value())
    {
        /// If the requested column is inside some arrays of tuples (requested using `arr.elem`
        /// syntax), add intermediate OutputColumnInfo-s to create those arrays.
        for (size_t i = 0; i < wrap_in_arrays; ++i)
            make_array(levels[prev_levels_size - 1].rep - i);

        output_columns[node.output_idx.value()].idx_in_output_block = idx_in_output_block;
    }
}

bool SchemaConverter::processSubtreePrimitive(TraversalNode & node)
{
    /// `parquet.thrift` says "[num_children] is not set when the element is a primitive type".
    /// If it's set but has value 0, logically it would make sense to interpret it as empty tuple/struct.
    /// But in practice some writers are sloppy about it and set this field to 0 (rather than unset)
    /// for primitive columns. E.g.
    /// tests/queries/0_stateless/data_hive/partitioning/non_existing_column=Elizabeth/sample.parquet
    bool is_primitive = !node.element->__isset.num_children || (node.element->num_children == 0 && node.element->__isset.type);
    if (!is_primitive)
        return false;

    primitive_column_idx += 1;
    if (!node.requested)
        return true;
    if (!node.element->__isset.type)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet metadata is missing physical type for column {}", node.getNameForLogging());

    DataTypePtr primitive_type_hint = node.type_hint;
    bool output_nullable = false;
    bool output_nullable_if_not_json = false;
    if (primitive_type_hint)
    {
        if (primitive_type_hint->lowCardinality())
        {
            primitive_type_hint = assert_cast<const DataTypeLowCardinality &>(*primitive_type_hint).getDictionaryType();
        }
        if (primitive_type_hint->isNullable())
        {
            output_nullable = true;
            primitive_type_hint = assert_cast<const DataTypeNullable &>(*primitive_type_hint).getNestedType();
        }
    }
    /// Force map key to be non-nullable because clickhouse Map doesn't support nullable map key.
    else if (!options.schema_inference_force_not_nullable && node.schema_context != SchemaContext::MapKey)
    {
        if (levels.back().is_array == false)
        {
            /// This schema element is OPTIONAL or inside an OPTIONAL tuple.
            output_nullable = true;
        }
        else if (options.schema_inference_force_nullable)
        {
            if (options.format.schema_inference_make_json_columns_nullable)
                output_nullable = true;
            else
                /// Historically, the setting schema_inference_make_columns_nullable wasn't applied
                /// to JSON columns (presumably because Nullable(Object) used to not be allowed).
                /// Keep this behavior for compatibility.
                output_nullable_if_not_json = true;
        }
    }

    auto geo_it = geo_columns.find(node.getParquetName());
    auto geo_metadata = geo_it == geo_columns.end() ? std::nullopt : std::optional(geo_it->second);

    DataTypePtr inferred_type;
    DataTypePtr raw_decoded_type;
    PageDecoderInfo decoder;
    try
    {
        processPrimitiveColumn(*node.element, primitive_type_hint, decoder, raw_decoded_type, inferred_type, geo_metadata);
    }
    catch (Exception & e)
    {
        if (options.format.parquet.skip_columns_with_unsupported_types_in_schema_inference &&
            (e.code() == ErrorCodes::INCORRECT_DATA || e.code() == ErrorCodes::NOT_IMPLEMENTED))
        {
            return true;
        }
        else
        {
            e.addMessage("column '" + node.getNameForLogging() + "'");
            throw;
        }
    }

    /// GeoParquet types like Point or Polygon can't be inside Nullable.
    if (typeid_cast<const DataTypeArray *>(inferred_type.get()) || typeid_cast<const DataTypeTuple *>(inferred_type.get()))
    {
        output_nullable = false;
        output_nullable_if_not_json = false;
    }

    size_t primitive_idx = primitive_columns.size();
    PrimitiveColumnInfo & primitive = primitive_columns.emplace_back();
    primitive.column_idx = primitive_column_idx - 1;
    primitive.schema_idx = schema_idx - 1;
    primitive.name = node.name;
    primitive.levels = levels;
    primitive.output_nullable = output_nullable || (output_nullable_if_not_json && !typeid_cast<const DataTypeObject *>(inferred_type.get()));
    primitive.decoder = std::move(decoder);
    primitive.raw_decoded_type = raw_decoded_type;
    for (const auto & level : levels)
        if (level.is_array)
            primitive.max_array_def = level.def;

    node.output_idx = output_columns.size();
    OutputColumnInfo & output = output_columns.emplace_back();
    output.name = node.name;
    output.primitive_start = primitive_idx;
    output.primitive_end = primitive_idx + 1;
    output.is_primitive = true;

    if (!primitive.raw_decoded_type)
        primitive.raw_decoded_type = inferred_type;

    primitive.intermediate_type = primitive.raw_decoded_type;
    if (primitive.output_nullable)
    {
        primitive.intermediate_type = std::make_shared<DataTypeNullable>(primitive.intermediate_type);
        inferred_type = std::make_shared<DataTypeNullable>(inferred_type);
    }

    primitive.final_type = node.type_hint ? node.type_hint : inferred_type;
    primitive.needs_cast = !primitive.final_type->equals(*primitive.intermediate_type);

    output.type = primitive.final_type;

    return true;
}

bool SchemaConverter::processSubtreeMap(TraversalNode & node)
{
    /// Map, aka Array(Tuple(2)).
    ///   required group `name` (MAP or MAP_KEY_VALUE):
    ///     repeated group "key_value" (maybe MAP_KEY_VALUE):
    ///       reqiured <recurse> "key"
    ///       <recurse> "value"

    if (node.element->converted_type != parq::ConvertedType::MAP && node.element->converted_type != parq::ConvertedType::MAP_KEY_VALUE && !node.element->logicalType.__isset.MAP)
        return false;
    /// If an element is declared as MAP, but doesn't have the expected structure of children
    /// and grandchildren, we fall back to interpreting it as array of tuples, as if there were
    /// no MAP annotation on it. Also fall back if Tuple type was requested
    /// (presumably `Tuple(Array(Tuple(key, value))` - a literal interpretation of the schema tree)
    /// (not to be confused with the case when `Array(Tuple(key, value))` was requested).
    if (node.schema_context != SchemaContext::None && node.schema_context != SchemaContext::ListElement)
        return false;
    if (typeid_cast<const DataTypeTuple *>(node.type_hint.get()))
        return false;
    if (node.element->num_children != 1)
        return false;
    const parq::SchemaElement & child = file_metadata.schema.at(schema_idx);
    if (child.repetition_type != parq::FieldRepetitionType::REPEATED || child.num_children != 2)
        return false;

    DataTypePtr array_type_hint;
    bool no_map = false; // return plain Array(Tuple) instead of Map
    if (node.type_hint)
    {
        if (const DataTypeMap * map_type = typeid_cast<const DataTypeMap *>(node.type_hint.get()))
        {
            array_type_hint = map_type->getNestedType();
        }
        else if (typeid_cast<const DataTypeArray *>(node.type_hint.get()))
        {
            array_type_hint = node.type_hint;
            no_map = true;
        }
        else
        {
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Requested type of column {} doesn't match parquet schema: parquet type is Map, requested type is {}", node.getNameForLogging(), node.type_hint->getName());
        }
    }
    /// (MapTupleAsPlainTuple is needed to skip a level in the column name: it changes
    /// `my_map.key_value.key` to `my_map.key`.
    TraversalNode subnode = node.prepareToRecurse(no_map ? SchemaContext::MapTupleAsPlainTuple : SchemaContext::MapTuple, array_type_hint);
    processSubtree(subnode);

    if (!node.requested || !subnode.output_idx.has_value())
        return true;
    size_t array_idx = subnode.output_idx.value();

    /// Support explicitly requesting Array(Tuple) type for map columns. Useful e.g. if the map
    /// key type is something that's not allowed as Map key in clickhouse.
    if (no_map)
    {
        node.output_idx = array_idx;
    }
    else
    {
        node.output_idx = output_columns.size();
        OutputColumnInfo & output = output_columns.emplace_back();
        const OutputColumnInfo & array = output_columns.at(array_idx);

        output.name = node.name;
        output.primitive_start = array.primitive_start;
        output.primitive_end = array.primitive_end;
        output.type = std::make_shared<DataTypeMap>(array.type);
        output.nested_columns = {array_idx};
    }

    return true;
}

bool SchemaConverter::processSubtreeArrayOuter(TraversalNode & node)
{
    /// Array:
    ///   required group `name` (List):
    ///     repeated group "list":
    ///       <recurse> "element"
    ///
    /// I.e. it's a double-wrapped burrito. To unwrap it into one Array, we have to coordinate
    /// across two levels of recursion: processSubtreeArrayOuter for the outer wrapper,
    /// processSubtreeArrayInner for the inner wrapper.

    if (node.element->converted_type != parq::ConvertedType::LIST && !node.element->logicalType.__isset.LIST)
        return false;
    if (node.schema_context != SchemaContext::None && node.schema_context != SchemaContext::ListElement)
        return false;
    if (node.element->num_children != 1)
        return false;
    const parq::SchemaElement & child = file_metadata.schema.at(schema_idx);
    if (child.repetition_type != parq::FieldRepetitionType::REPEATED || child.num_children != 1)
        return false;

    TraversalNode subnode = node.prepareToRecurse(SchemaContext::ListTuple, node.type_hint);
    processSubtree(subnode);

    if (!node.requested || !subnode.output_idx.has_value())
        return true;

    node.output_idx = subnode.output_idx;

    return true;
}

bool SchemaConverter::processSubtreeArrayInner(TraversalNode & node)
{
    if (node.schema_context != SchemaContext::ListTuple)
        return false;

    /// Array (middle schema element).
    chassert(node.element->repetition_type == parq::FieldRepetitionType::REPEATED &&
             node.element->num_children == 1); // caller checked this
    /// (type_hint is already unwrapped to be element type, because of REPEATED)
    TraversalNode subnode = node.prepareToRecurse(SchemaContext::ListElement, node.type_hint);
    processSubtree(subnode);

    if (!node.requested || !subnode.output_idx.has_value())
        return true;

    node.output_idx = subnode.output_idx;

    return true;
}

void SchemaConverter::processSubtreeTuple(TraversalNode & node)
{
    /// Tuple (possibly a Map key_value tuple):
    ///   (required|optional) group `name`:
    ///     <recurse> `name1`
    ///     <recurse> `name2`
    ///     ...

    const DataTypeTuple * tuple_type_hint = typeid_cast<const DataTypeTuple *>(node.type_hint.get());
    if (node.type_hint && !tuple_type_hint)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Requested type of column {} doesn't match parquet schema: parquet type is Tuple, requested type is {}", node.getNameForLogging(), node.type_hint->getName());

    /// 3 modes:
    ///  * If type_hint has element names, we match elements from parquet to elements from type
    ///    hint tuple by name. If some elements are not in type hint, we skip them.
    ///    If elements are in different order, we reorder them to match type_hint.
    ///  * If type_hint has no names, we match elements sequentially and preserve order.
    ///  * If there's no type_hint, we preserve order, produce tuple with names.
    ///    Only in this mode, we allow skipping unsupported elements if
    ///    skip_columns_with_unsupported_types_in_schema_inference is true.
    ///    In other modes, we skip the whole tuple if any element is unsupported.

    bool lookup_by_name = false;
    std::vector<size_t> elements;
    if (node.type_hint)
    {
        if (tuple_type_hint->hasExplicitNames() && !tuple_type_hint->getElements().empty() &&
            node.schema_context != SchemaContext::MapTuple)
        {
            /// Allow reading a subset of tuple elements, matched by name, possibly reordered.
            lookup_by_name = true;
            elements.resize(tuple_type_hint->getElements().size(), UINT64_MAX);
        }
        else
        {
            if (tuple_type_hint->getElements().size() != size_t(node.element->num_children))
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Requested type of column {} doesn't match parquet schema: parquet type is Tuple with {} elements, requested type is Tuple with {} elements", node.getNameForLogging(), node.element->num_children, tuple_type_hint->getElements().size());
        }
    }
    if (!lookup_by_name && node.requested)
        elements.resize(size_t(node.element->num_children), UINT64_MAX);

    Strings names;
    DataTypes types;
    if (!node.type_hint && node.requested)
    {
        names.resize(elements.size());
        types.resize(elements.size());
    }

    size_t primitive_start = primitive_columns.size();
    size_t output_start = output_columns.size();
    size_t skipped_unsupported_columns = 0;
    std::vector<String> element_names_in_file;
    for (size_t i = 0; i < size_t(node.element->num_children); ++i)
    {
        const String & element_name = element_names_in_file.emplace_back(useColumnMapperIfNeeded(file_metadata.schema.at(schema_idx)));
        std::optional<size_t> idx_in_output_tuple = i - skipped_unsupported_columns;
        if (lookup_by_name)
        {
            idx_in_output_tuple = tuple_type_hint->tryGetPositionByName(element_name, options.format.parquet.case_insensitive_column_matching);

            if (idx_in_output_tuple.has_value() && elements.at(idx_in_output_tuple.value()) != UINT64_MAX)
                throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Parquet tuple {} has multiple elements with name `{}`", node.getNameForLogging(), element_name);
        }

        DataTypePtr element_type_hint;
        if (node.type_hint && idx_in_output_tuple.has_value())
            element_type_hint = tuple_type_hint->getElement(idx_in_output_tuple.value());

        const bool element_requested = node.requested && idx_in_output_tuple.has_value();

        TraversalNode subnode = node.prepareToRecurse(SchemaContext::None, element_type_hint);
        subnode.requested = element_requested;
        if (node.schema_context == SchemaContext::MapTuple && idx_in_output_tuple == 0)
            subnode.schema_context = SchemaContext::MapKey;

        processSubtree(subnode);
        auto element_idx = subnode.output_idx;

        if (element_requested)
        {
            if (!element_idx.has_value())
            {
                if (node.type_hint || node.schema_context == SchemaContext::MapTuple)
                {
                    /// If one of the elements is skipped, skip the whole tuple.
                    /// Remove previous elements.
                    primitive_columns.resize(primitive_start);
                    output_columns.resize(output_start);
                    return;
                }
                else
                {
                    skipped_unsupported_columns += 1;
                    elements.pop_back();
                    names.pop_back();
                    types.pop_back();
                    continue;
                }
            }

            elements.at(idx_in_output_tuple.value()) = element_idx.value();

            const auto & type = output_columns.at(element_idx.value()).type;
            if (node.type_hint)
            {
                chassert(type->equals(*element_type_hint));
            }
            else
            {
                names.at(idx_in_output_tuple.value()) = element_name;
                types.at(idx_in_output_tuple.value()) = type;
            }
        }
    }

    if (!node.requested)
        return;

    /// Map tuple in parquet has elements: {"key" , "value" },
    /// but DataTypeMap requires:          {"keys", "values"}.
    if (node.schema_context == SchemaContext::MapTuple)
        names = {"keys", "values"};

    DataTypePtr output_type;
    if (node.type_hint)
    {
        chassert(elements.size() == tuple_type_hint->getElements().size());
        for (size_t i = 0; i < elements.size(); ++i)
        {
            if (elements[i] != UINT64_MAX)
                continue;
            if (!options.format.parquet.allow_missing_columns)
                throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Requested tuple element {} of column {} was not found in parquet schema ({})", tuple_type_hint->getNameByPosition(i + 1), node.getNameForLogging(), element_names_in_file);

            elements[i] = output_columns.size();
            OutputColumnInfo & missing_output = output_columns.emplace_back();
            missing_output.name = node.name + "." + (tuple_type_hint->hasExplicitNames() ? tuple_type_hint->getNameByPosition(i + 1) : std::to_string(i + 1));
            missing_output.type = tuple_type_hint->getElement(i);
            missing_output.is_missing_column = true;
        }
        output_type = node.type_hint;
    }
    else
    {
        output_type = std::make_shared<DataTypeTuple>(types, names);
    }

    node.output_idx = output_columns.size();
    OutputColumnInfo & output = output_columns.emplace_back();
    output.name = node.name;
    output.primitive_start = primitive_start;
    output.primitive_end = primitive_columns.size();
    output.type = std::move(output_type);
    output.nested_columns = elements;
}

void SchemaConverter::processPrimitiveColumn(
    const parq::SchemaElement & element, DataTypePtr type_hint,
    PageDecoderInfo & out_decoder, DataTypePtr & out_decoded_type,
    DataTypePtr & out_inferred_type, std::optional<GeoColumnMetadata> geo_metadata) const
{
    /// Inputs:
    ///  * Parquet Type ("physical type"),
    ///  * Parquet ConvertedType (deprecated, but we have to support it),
    ///  * Parquet LogicalType,
    ///  * ClickHouse type hint (e.g. if the user specified column types explicitly).
    ///
    /// Outputs:
    ///  * out_decoder - how to decode the column (it then separately further dispatches to
    ///    different code paths depending on page encoding),
    ///  * out_inferred_type - data type most closely matching the parquet logical type, used for
    ///    schema inference.
    ///  * out_decoded_type - data type of decoding result, chosen for decoding convenience or speed
    ///    (e.g. matching the parquet physical type). If nullptr, equal to out_inferred_type.
    /// After parsing, columns are converted (using castColumn) from out_decoded_type to the final
    /// data type. E.g. maybe out_decoded_type is Int32 based on parquet physical type, but
    /// out_inferred_type is Int16 based on schema inference, and castColumn does the conversion.

    parq::Type::type type = element.type;
    std::optional<parq::ConvertedType::type> converted =
        element.__isset.converted_type ? std::make_optional(element.converted_type) : std::nullopt;
    const parq::LogicalType & logical = element.logicalType;
    using CONV = parq::ConvertedType;
    chassert(!out_inferred_type && !out_decoded_type);
    out_decoder.physical_type = type;

    auto get_output_type_index = [&]
    {
        chassert(out_inferred_type);
        return type_hint ? type_hint->getTypeId() : out_inferred_type->getTypeId();
    };

    auto dispatch_int_stats_converter = [&](bool allow_datetime_and_ipv4, IntConverter & converter) -> bool
    {
        WhichDataType which(get_output_type_index());
        if (which.isNativeInteger())
            converter.field_signed = which.isNativeInt();
        else switch (which.idx)
        {
            case TypeIndex::IPv4:
                if (allow_datetime_and_ipv4)
                {
                    converter.field_ipv4 = true;
                    converter.field_signed = false;
                }
                else
                    return false;
                break;
            case TypeIndex::Date:
                converter.field_signed = false;
                break;
            case TypeIndex::DateTime:
                if (!allow_datetime_and_ipv4)
                    return false;
                converter.field_signed = false;
                break;
            case TypeIndex::Enum8:
            case TypeIndex::Enum16:
            case TypeIndex::Date32:
                break;
            /// Not supported: DateTime64, Decimal*, Float*
            /// Not possible (in most cases): String, FixedString
            default:
                return false;
        }
        return true;
    };

    auto is_output_type_decimal = [&](size_t expected_size, UInt32 expected_scale) -> bool
    {
        const IDataType * output_type = type_hint ? type_hint.get() : out_inferred_type.get();
        WhichDataType which(output_type->getTypeId());
        if (which.isDecimal())
            return output_type->getSizeOfValueInMemory() == expected_size && getDecimalScale(*output_type) == expected_scale;
        else if (which.isDateTime64())
            return 8 == expected_size && assert_cast<const DataTypeDateTime64 *>(output_type)->getScale() == expected_scale;
        return false;
    };

    auto is_output_type_float = [&](size_t expected_size) -> bool
    {
        size_t size = 0;
        switch (get_output_type_index())
        {
            case TypeIndex::Float32: size = 4; break;
            case TypeIndex::Float64: size = 8; break;
            default: return false;
        }
        return size == expected_size;
    };

    auto is_output_type_string = [&]() -> bool
    {
        return get_output_type_index() == TypeIndex::String;
    };

    /// Escape hatch for reading raw plain-encoded values and bypassing data type stuff.
    /// If type FixedString is requested, and the parquet physical type is a fixed-size type of
    /// matching size, use a trivial FixedSizeConverter.
    /// E.g. don't do Decimal endianness conversion of INT96 timestamp conversion.
    if (const DataTypeFixedString * fixed_string_type = typeid_cast<const DataTypeFixedString *>(type_hint.get()))
    {
        size_t size = 0;
        bool found = true;
        switch (type)
        {
            case parq::Type::BOOLEAN: size = 1; break;
            case parq::Type::INT32: size = 4; break;
            case parq::Type::INT64: size = 8; break;
            case parq::Type::INT96: size = 12; break;
            case parq::Type::FLOAT: size = 4; break;
            case parq::Type::DOUBLE: size = 8; break;
            case parq::Type::FIXED_LEN_BYTE_ARRAY: size = size_t(element.type_length); break;

            /// BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY fall through to normal type dispatch.
            default:
                found = false;
                break;
        }
        if (found && size == fixed_string_type->getN())
        {
            out_inferred_type = std::make_shared<DataTypeFixedString>(size);
            auto converter = std::make_shared<FixedStringConverter>();
            converter->input_size = size;
            out_decoder.allow_stats = type == parq::Type::FIXED_LEN_BYTE_ARRAY && !element.__isset.converted_type && !element.__isset.logicalType;
            out_decoder.fixed_size_converter = std::move(converter);
            return;
        }
    }

    /// GeoParquet.
    /// Spec says "Geometry columns MUST be at the root of the schema", but we allow them to be
    /// nested in tuples etc, why not. (Though nesting in arrays/maps probably currently wouldn't
    /// work because our names omit the wrapper SchemaElement-s. That would be easy to fix by
    /// including them in parquet_name.)
    /// If type hint is String, ignore geoparquet and return raw bytes.
    if (geo_metadata.has_value() && (!type_hint || !typeid_cast<const DataTypeString *>(type_hint.get())))
    {
        if (type != parq::Type::BYTE_ARRAY)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type of GeoParquet column: {}", thriftToString(type));

        out_inferred_type = getGeoDataType(geo_metadata->type);
        out_decoder.string_converter = std::make_shared<GeoConverter>(*geo_metadata);
        return;
    }

    if (logical.__isset.STRING || logical.__isset.JSON || logical.__isset.BSON ||
        logical.__isset.ENUM || converted == CONV::UTF8 || converted == CONV::JSON ||
        converted == CONV::BSON || converted == CONV::ENUM)
    {
        if (type != parq::Type::BYTE_ARRAY && type != parq::Type::FIXED_LEN_BYTE_ARRAY)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected non-string physical type for string logical type: {}", thriftToString(element));
        /// Fall through to dispatch by physical type only.
    }
    else if (logical.__isset.INTEGER || (converted >= CONV::UINT_8 && converted <= CONV::INT_64))
    {
        bool is_signed = logical.INTEGER.isSigned;
        size_t bits = size_t(UInt8(logical.INTEGER.bitWidth));
        if (!logical.__isset.INTEGER)
        {
            switch (converted.value())
            {
                case CONV::UINT_8: is_signed = false; bits = 8; break;
                case CONV::UINT_16: is_signed = false; bits = 16; break;
                case CONV::UINT_32: is_signed = false; bits = 32; break;
                case CONV::UINT_64: is_signed = false; bits = 64; break;
                case CONV::INT_8: is_signed = true; bits = 8; break;
                case CONV::INT_16: is_signed = true; bits = 16; break;
                case CONV::INT_32: is_signed = true; bits = 32; break;
                case CONV::INT_64: is_signed = true; bits = 64; break;
                default:
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected integer logical type: {}", thriftToString(element));
            }
        }

        size_t physical_bits;
        if (type == parq::Type::INT32)
            physical_bits = 32;
        else if (type == parq::Type::INT64)
            physical_bits = 64;
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected non-int physical type for int logical type: {}", thriftToString(element));

        bits = std::min(bits, physical_bits);

        if (!is_signed && bits == 8)
            out_inferred_type = std::make_shared<DataTypeUInt8>();
        else if (!is_signed && bits == 16)
            out_inferred_type = std::make_shared<DataTypeUInt16>();
        else if (!is_signed && bits == 32)
            out_inferred_type = std::make_shared<DataTypeUInt32>();
        else if (!is_signed && bits == 64)
            out_inferred_type = std::make_shared<DataTypeUInt64>();
        else if (is_signed && bits == 8)
            out_inferred_type = std::make_shared<DataTypeInt8>();
        else if (is_signed && bits == 16)
            out_inferred_type = std::make_shared<DataTypeInt16>();
        else if (is_signed && bits == 32)
            out_inferred_type = std::make_shared<DataTypeInt32>();
        else if (is_signed && bits == 64)
            out_inferred_type = std::make_shared<DataTypeInt64>();
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected integer logical type: {}", thriftToString(element));

        /// Can't leave the signed->unsigned conversion to castColumn.
        /// E.g. if parquet type is UINT64, and the requested clickhouse type is Int128,
        /// casting Int64 -> UInt64 -> Int128 produces different result from Int64 -> Int128.
        auto converter = std::make_shared<IntConverter>();
        converter->input_size = physical_bits / 8;
        converter->input_signed = is_signed;
        if (physical_bits == 64)
        {
            out_decoded_type = is_signed
                ? std::static_pointer_cast<IDataType>(std::make_shared<DataTypeInt64>())
                : std::static_pointer_cast<IDataType>(std::make_shared<DataTypeUInt64>());
        }
        else if (bits == 8)
            converter->output_size = 1;
        else if (bits == 16)
            converter->output_size = 2;

        out_decoder.allow_stats = dispatch_int_stats_converter(/*allow_datetime_and_ipv4=*/ true, *converter);
        out_decoder.fixed_size_converter = std::move(converter);

        return;
    }
    else if (logical.__isset.TIMESTAMP || logical.__isset.TIME || converted == CONV::TIMESTAMP_MILLIS || converted == CONV::TIMESTAMP_MICROS || converted == CONV::TIME_MILLIS || converted == CONV::TIME_MICROS)
    {
        /// We interpret both timestamp (logical.TIMESTAMP) and time-of-day (logical.TIME)
        /// types as timestamps, since clickhouse doesn't have time-of-day type.
        /// E.g. time of day 12:34:56.789 turns into timestamp 1970-01-01 12:34:56.789.

        UInt32 scale;
        if (logical.TIMESTAMP.unit.__isset.MILLIS || logical.TIME.unit.__isset.MILLIS || converted == CONV::TIMESTAMP_MILLIS || converted == CONV::TIME_MILLIS)
            scale = 3;
        else if (logical.TIMESTAMP.unit.__isset.MICROS || logical.TIME.unit.__isset.MICROS || converted == CONV::TIMESTAMP_MICROS || converted == CONV::TIME_MICROS)
            scale = 6;
        else if (logical.TIMESTAMP.unit.__isset.NANOS || logical.TIME.unit.__isset.NANOS)
            scale = 9;
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected timestamp units: {}", thriftToString(element));

        if (type != parq::Type::INT64 && type != parq::Type::INT32)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type for timestamp logical type: {}", thriftToString(element));

        /// Can't leave int -> DateTime64 conversion to castColumn as it interprets the integer as seconds.
        String timezone = "UTC";
        if (!options.format.parquet.local_time_as_utc &&
            ((logical.__isset.TIMESTAMP && !logical.TIMESTAMP.isAdjustedToUTC) ||
             (logical.__isset.TIME && !logical.TIME.isAdjustedToUTC)))
            timezone = "";
        out_inferred_type = std::make_shared<DataTypeDateTime64>(scale, timezone);
        auto converter = std::make_shared<IntConverter>();
        /// Note: TIMESTAMP is always INT64. INT32 is only for weird unimportant case of TIME_MILLIS
        /// (i.e. time of day rather than timestamp).
        converter->input_size = type == parq::Type::INT32 ? 4 : 8;

        if (scale == 3 && converter->input_size == 8 && type_hint && type_hint->getTypeId() == TypeIndex::DateTime)
        {
            /// Special case: converting milliseconds to seconds.
            /// We generally don't do such conversions during decoding, leaving it to castColumn.
            /// And we usually don't use stats when a nontrivial castColumn is needed.
            /// But in this case it's important to make stats work.
            /// This comes up when round-tripping DateTime values through parquet. Our writer writes
            /// DateTime (seconds) as TIMESTAMP_MILLIS (milliseconds) because parquet doesn't have
            /// a more suitable type. It's probably common to then read it back with DateTime type
            /// hint. It's pretty important for min/max stats to work with timestamps, so we add
            /// this special case.
            ///
            /// We could generalize it and allow arbitrary Decimal scale and signedness conversions,
            /// but it doesn't seem worth the complexity and risk of bugs.
            converter->field_timestamp_from_millis = true;
            converter->field_signed = false;
            out_decoder.allow_stats = true;
        }
        else
        {
            converter->field_decimal_scale = scale;
            out_decoder.allow_stats = is_output_type_decimal(sizeof(Int64), scale);
            if (converter->input_size == 4)
                /// Can't leave Decimal32 -> DateTime64 conversion to castColumn because this
                /// particular cast is not supported for some reason.
                converter->output_size = 8;
        }

        out_decoder.fixed_size_converter = std::move(converter);

        return;
    }
    else if (logical.__isset.DATE || converted == CONV::DATE)
    {
        if (type != parq::Type::INT32)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type for date logical type: {}", thriftToString(element));

        /// Skip date range check if plain integer type is requested.
        bool output_plain_int = type_hint && WhichDataType(type_hint->getTypeId()).isNativeInteger();
        if (output_plain_int)
            out_inferred_type = std::make_shared<DataTypeInt32>();
        else
            out_inferred_type = std::make_shared<DataTypeDate32>();
        auto converter = std::make_shared<IntConverter>();
        converter->input_size = 4;

        if (!output_plain_int)
        {
            converter->date_overflow_behavior = options.format.date_time_overflow_behavior;

            /// Prior to introducing `date_time_overflow_behavior`, out parquet reader threw an error
            /// in case date was out of range.
            /// In order to leave this behavior as default, we also throw when
            /// `date_time_overflow_mode == ignore`, as it is the setting's default value
            /// (As we want to make this backwards compatible, not break any workflows.)
            if (converter->date_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Ignore)
                converter->date_overflow_behavior = FormatSettings::DateTimeOverflowBehavior::Throw;
        }

        out_decoder.allow_stats = dispatch_int_stats_converter(/*allow_datetime_and_ipv4=*/ false, *converter);
        out_decoder.fixed_size_converter = std::move(converter);

        return;
    }
    else if (logical.__isset.DECIMAL || converted == CONV::DECIMAL)
    {
        UInt32 precision = logical.__isset.DECIMAL ? logical.DECIMAL.precision : element.precision;
        UInt32 scale = logical.__isset.DECIMAL ? logical.DECIMAL.scale : element.scale;
        precision = std::max(precision, scale);

        UInt32 max_precision = 0;
        if (type == parq::Type::INT32 || type == parq::Type::INT64)
        {
            max_precision = type == parq::Type::INT32 ? 9 : 18;
            auto converter = std::make_shared<IntConverter>();
            converter->input_size = type == parq::Type::INT32 ? 4 : 8;
            converter->field_decimal_scale = scale;
            out_decoder.fixed_size_converter = std::move(converter);
        }
        else if (type == parq::Type::FIXED_LEN_BYTE_ARRAY)
        {
            size_t input_size = element.type_length;

            if (input_size > 32)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet decimal value too long: {} bytes (at most 32 is supported)", input_size);

            if (precision <= 9 && input_size <= 4)
            {
                max_precision = 9;
                /// FIXED_LEN_BYTE_ARRAY decimals are big endian, while INT32/INT64 are little-endian.
                out_decoder.fixed_size_converter = std::make_shared<BigEndianDecimalFixedSizeConverter<Int32>>(input_size, scale);
            }
            else if (precision <= 18 && input_size <= 8)
            {
                max_precision = 18;
                out_decoder.fixed_size_converter = std::make_shared<BigEndianDecimalFixedSizeConverter<Int64>>(input_size, scale);
            }
            else if (precision <= 38 && input_size <= 16)
            {
                max_precision = 38;
                out_decoder.fixed_size_converter = std::make_shared<BigEndianDecimalFixedSizeConverter<Int128>>(input_size, scale);
            }
            else
            {
                max_precision = 76;
                out_decoder.fixed_size_converter = std::make_shared<BigEndianDecimalFixedSizeConverter<Int256>>(input_size, scale);
            }
        }
        else if (type == parq::Type::BYTE_ARRAY)
        {
            if (precision <= 9)
            {
                max_precision = 9;
                out_decoder.string_converter = std::make_shared<BigEndianDecimalStringConverter<Int32>>(scale);
            }
            else if (precision <= 18)
            {
                max_precision = 18;
                out_decoder.string_converter = std::make_shared<BigEndianDecimalStringConverter<Int64>>(scale);
            }
            else if (precision <= 38)
            {
                max_precision = 38;
                out_decoder.string_converter = std::make_shared<BigEndianDecimalStringConverter<Int128>>(scale);
            }
            else
            {
                max_precision = 76;
                out_decoder.string_converter = std::make_shared<BigEndianDecimalStringConverter<Int256>>(scale);
            }
        }
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type for Decimal column: {}", thriftToString(type));

        if (precision > max_precision)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet decimal type precision or scale is too big ({} digits) for physical type {}", precision, thriftToString(type));

        out_inferred_type = createDecimal<DataTypeDecimal>(precision, scale);
        size_t output_size = out_inferred_type->getSizeOfValueInMemory();
        out_decoder.allow_stats = is_output_type_decimal(output_size, scale);

        return;
    }
    else if (logical.__isset.MAP || logical.__isset.LIST || converted == CONV::MAP ||
             converted == CONV::MAP_KEY_VALUE || converted == CONV::LIST)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected compound logical type for leaf column: {}", thriftToString(element));
    }
    else if (logical.__isset.UNKNOWN)
    {
        /// This means all values are nulls. We could use DataTypeNothing here, or fall through to
        /// dispatch by physical type. Either way seems fine, currently we do the latter.
    }
    else if (logical.__isset.UUID)
    {
        if (type != parq::Type::FIXED_LEN_BYTE_ARRAY || element.type_length != 16)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type for UUID column: {}", thriftToString(element));

        /// TODO [parquet]: Support UUIDs. Make sure to get the byte order right, it seems tricky.
        /// For now, fall through to reading as FixedString(16).
    }
    else if (logical.__isset.FLOAT16)
    {
        if (type != parq::Type::FIXED_LEN_BYTE_ARRAY || element.type_length != 2)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type for FLOAT16 column: {}", thriftToString(element));

        out_inferred_type = std::make_shared<DataTypeFloat32>();
        out_decoder.fixed_size_converter = std::make_shared<Float16Converter>();
        /// (Comment in 03263_parquet_write_bloom_filter in parquet.thrift doesn't mention sort order
        ///  for FLOAT16, so presumably it's undefined. Leaving allow_stats == false.)
        return;
    }
    else if (converted == CONV::INTERVAL)
    {
        /// TODO [parquet]: Support. For now, fall through to reading as FixedString(12).
    }
    else if (element.__isset.logicalType || element.__isset.converted_type)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected logical/converted type: {}", thriftToString(element));
    }

    // If we didn't `return` above, dispatch by physical type.
    switch (type)
    {
        case parq::Type::BOOLEAN:
        {
            out_inferred_type = DataTypeFactory::instance().get("Bool");
            auto converter = std::make_shared<IntConverter>();
            converter->input_size = 1;
            converter->input_signed = false;
            converter->field_signed = false;
            out_decoder.allow_stats = dispatch_int_stats_converter(/*allow_datetime_and_ipv4=*/ false, *converter);
            out_decoder.fixed_size_converter = std::move(converter);
            return;
        }
        case parq::Type::INT32:
        {
            out_inferred_type = std::make_shared<DataTypeInt32>();
            auto converter = std::make_shared<IntConverter>();
            converter->input_size = 4;
            out_decoder.allow_stats = dispatch_int_stats_converter(/*allow_datetime_and_ipv4=*/ true, *converter);
            out_decoder.fixed_size_converter = std::move(converter);
            return;
        }
        case parq::Type::INT64:
        {
            out_inferred_type = std::make_shared<DataTypeInt64>();
            auto converter = std::make_shared<IntConverter>();
            converter->input_size = 8;
            out_decoder.allow_stats = dispatch_int_stats_converter(/*allow_datetime_and_ipv4=*/ false, *converter);
            out_decoder.fixed_size_converter = std::move(converter);
            return;
        }
        case parq::Type::INT96:
        {
            out_inferred_type = std::make_shared<DataTypeDateTime64>(9, "UTC");
            out_decoder.fixed_size_converter = std::make_shared<Int96Converter>();
            /// (Leaving allow_stats == false because INT96 sort order is undefined.)
            return;
        }
        case parq::Type::FLOAT:
        {
            out_inferred_type = std::make_shared<DataTypeFloat32>();
            auto converter = std::make_shared<FloatConverter<float>>();
            out_decoder.allow_stats = is_output_type_float(converter->input_size);
            out_decoder.fixed_size_converter = std::move(converter);
            return;
        }
        case parq::Type::DOUBLE:
        {
            out_inferred_type = std::make_shared<DataTypeFloat64>();
            auto converter = std::make_shared<FloatConverter<double>>();
            out_decoder.allow_stats = is_output_type_float(converter->input_size);
            out_decoder.fixed_size_converter = std::move(converter);
            return;
        }
        case parq::Type::BYTE_ARRAY:
        {
            out_inferred_type = std::make_shared<DataTypeString>();
            auto converter = std::make_shared<TrivialStringConverter>();
            out_decoder.allow_stats = is_output_type_string();
            out_decoder.string_converter = std::move(converter);

            if ((logical.__isset.JSON || converted == CONV::JSON) && options.format.parquet.enable_json_parsing)
            {
                /// Just output ColumnString and leave json parsing to castColumn.
                /// Alternatively, we could add a custom StringConverter to avoid copying the strings.
                out_decoded_type = std::move(out_inferred_type);
                out_inferred_type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);
            }
            return;
        }
        case parq::Type::FIXED_LEN_BYTE_ARRAY:
        {
            if (type_hint)
            {
                /// If parquet type is FIXED_LEN_BYTE_ARRAY(16), and type hint is [U]Int128, assume
                /// it's binary little-endian [U]Int128. That's how clickhouse parquet writer writes
                /// [U]Int128 (btw, we should probably change that to Decimal).
                /// Same for FIXED_LEN_BYTE_ARRAY(32) and [U]Int256.
                /// We can't leave this conversion to castColumn because it would parse as text.
                WhichDataType which(type_hint->getTypeId());
                if (which.isInteger() && !which.isNativeInteger() &&
                    type_hint->getSizeOfValueInMemory() == size_t(element.type_length))
                {
                    out_inferred_type = type_hint;
                }
            }

            if (!out_inferred_type)
                out_inferred_type = std::make_shared<DataTypeFixedString>(size_t(element.type_length));
            auto converter = std::make_shared<FixedStringConverter>();
            converter->input_size = size_t(element.type_length);
            out_decoder.fixed_size_converter = std::move(converter);

            /// (The case where type_hint is FixedString is handled above, no need to check for it here.)
            out_decoder.allow_stats = !logical.__isset.UUID && WhichDataType(get_output_type_index()).isString();
            return;
        }
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type: {}", thriftToString(element));
}

}
