#include <Processors/Formats/Impl/Parquet/SchemaConverter.h>

#include <Common/StringUtils.h>
#include <Common/checkStackSize.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/NestedUtils.h>
#include <Formats/FormatFilterInfo.h>
#include <Processors/Formats/Impl/Parquet/Decoding.h>
#include <Processors/Formats/Impl/Parquet/VariantUtils.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/Exception.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <algorithm>
#include <cctype>
#include <fmt/ranges.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int DUPLICATE_COLUMN;
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
    extern const int TOO_DEEP_RECURSION;
    extern const int NOT_IMPLEMENTED;
    extern const int THERE_IS_NO_COLUMN;
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

namespace DB::Parquet
{

namespace
{

bool isVariantLikeType(const IDataType * type)
{
    if (!type)
        return false;

    if (typeid_cast<const DataTypeDynamic *>(type))
        return true;
    if (typeid_cast<const DataTypeString *>(type))
        return true;
    if (typeid_cast<const DataTypeObject *>(type))
        return true;
    if (typeid_cast<const DataTypeVariant *>(type))
        return true;
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type))
        return isVariantLikeType(nullable->getNestedType().get());
    return false;
}

bool startsWithCaseInsensitiveASCII(std::string_view value, std::string_view prefix)
{
    if (value.size() < prefix.size())
        return false;

    for (size_t i = 0; i < prefix.size(); ++i)
    {
        if (std::tolower(static_cast<unsigned char>(value[i])) != std::tolower(static_cast<unsigned char>(prefix[i])))
            return false;
    }

    return true;
}

DataTypePtr makeExplicitParsedObjectTypeFromMetadataHint(std::string_view hint, bool primitive_output_nullable)
{
    DataTypePtr type;

    if (hint == "Dynamic" || hint == "Nullable(Dynamic)")
    {
        type = std::make_shared<DataTypeDynamic>();
    }
    else if (isStoredJSONVariantTypeHint(hint))
    {
        type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);
    }
    else
    {
        return {};
    }

    if (hint.starts_with("Nullable("))
        type = std::make_shared<DataTypeNullable>(std::move(type));

    if (primitive_output_nullable && !type->isNullable())
        type = std::make_shared<DataTypeNullable>(std::move(type));

    return type;
}

DataTypePtr makeExplicitParsedObjectTypeFromTypeHint(const DataTypePtr & type_hint, bool primitive_output_nullable)
{
    if (!type_hint)
        return {};

    bool nullable = false;
    DataTypePtr unwrapped = type_hint;
    while (unwrapped)
    {
        if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(unwrapped.get()))
        {
            unwrapped = low_cardinality->getDictionaryType();
            continue;
        }

        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(unwrapped.get()))
        {
            nullable = true;
            unwrapped = nullable_type->getNestedType();
            continue;
        }

        break;
    }

    DataTypePtr type;
    if (typeid_cast<const DataTypeDynamic *>(unwrapped.get()))
    {
        type = std::make_shared<DataTypeDynamic>();
    }
    else if (const auto * object_type = typeid_cast<const DataTypeObject *>(unwrapped.get()))
    {
        if (object_type->getSchemaFormat() != DataTypeObject::SchemaFormat::JSON)
            return {};

        type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);
    }
    else
    {
        return {};
    }

    if (nullable || primitive_output_nullable)
        type = std::make_shared<DataTypeNullable>(std::move(type));

    return type;
}

DataTypePtr getDefaultVariantInferredType(const ReadOptions & options, bool nullable = false)
{
    DataTypePtr type = options.format.parquet.enable_json_parsing
        ? DataTypePtr(std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON))
        : DataTypePtr(std::make_shared<DataTypeDynamic>());

    if (nullable)
        return makeNullableSafe(type);

    return type;
}

}

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

    for (const auto & kv : file_metadata.key_value_metadata)
    {
        if (kv.key == CLICKHOUSE_VARIANT_TYPE_HINTS_METADATA_KEY)
        {
            try
            {
                Poco::JSON::Parser parser;
                auto parsed = parser.parse(kv.value);
                const auto & object = *parsed.extract<Poco::JSON::Object::Ptr>();
                for (const auto & [column_name, raw_hint] : object)
                {
                    String hint = raw_hint.convert<String>();
                    if (hint.empty())
                        continue;

                    clickhouse_variant_type_hints.emplace(column_name, std::move(hint));
                }
            }
            catch (const Poco::Exception & e)
            {
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Malformed `Parquet` footer metadata key {}: {}",
                    kv.key,
                    e.displayText());
            }
        }
        else if (kv.key == CLICKHOUSE_VARIANT_WRAPPER_PATHS_METADATA_KEY)
        {
            has_clickhouse_variant_wrapper_paths_metadata = true;
            try
            {
                Poco::JSON::Parser parser;
                auto parsed = parser.parse(kv.value);
                const auto & array = *parsed.extract<Poco::JSON::Array::Ptr>();
                for (size_t i = 0; i < array.size(); ++i)
                    clickhouse_variant_wrapper_paths.emplace(array.getElement<String>(static_cast<UInt32>(i)));
            }
            catch (const Poco::Exception & e)
            {
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Malformed `Parquet` footer metadata key {}: {}",
                    kv.key,
                    e.displayText());
            }
        }
    }
}

void SchemaConverter::checkSchemaReadDepth(size_t depth) const
{
    if (depth > options.format.max_parser_depth)
    {
        throw Exception(
            ErrorCodes::TOO_DEEP_RECURSION,
            "Maximum parse depth ({}) exceeded while traversing `Parquet` schema. Consider raising `max_parser_depth` setting.",
            options.format.max_parser_depth);
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
        processSubtree(node, 1);
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

        if (col.primitive_dependencies.empty())
            continue;

        for (size_t primitive_idx : col.primitive_dependencies)
        {
            PrimitiveColumnInfo & primitive = primitive_columns.at(primitive_idx);
            primitive.dependent_output_idxs_in_block.push_back(idx);
            if (primitive.idx_in_output_block == UINT64_MAX)
                primitive.idx_in_output_block = idx;
        }
    }
    for (const String & name : external_columns)
    {
        size_t idx = sample_block->getPositionByName(name, /* case_insensitive= */ false);
        /// Note: it may already be true, if PREWHERE expression passes a column through.
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
        missing_output.input_type = sample_block->getByPosition(i).type;
        missing_output.output_type = missing_output.input_type;
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
        processSubtree(node, 1);
        if (node.output_idx.has_value())
        {
            const OutputColumnInfo & col = output_columns.at(node.output_idx.value());
            res.emplace_back(col.name, col.output_type);
        }
    }
    return res;
}

bool SchemaConverter::hasRequestedDescendantColumn(const String & prefix) const
{
    if (!sample_block || prefix.empty())
        return false;

    String full_prefix = prefix + ".";
    for (const auto & column : *sample_block)
    {
        if (options.format.parquet.case_insensitive_column_matching)
        {
            if (startsWithCaseInsensitiveASCII(column.name, full_prefix))
                return true;
        }
        else if (startsWith(column.name, full_prefix))
        {
            return true;
        }
    }

    return false;
}

std::vector<std::pair<size_t, String>> SchemaConverter::collectRequestedSubcolumns(const String & prefix) const
{
    if (!sample_block || prefix.empty())
        return {};

    std::vector<std::pair<size_t, String>> requested_subcolumns;
    String full_prefix = prefix + ".";
    for (size_t i = 0; i < sample_block->columns(); ++i)
    {
        const auto & requested_column = sample_block->getByPosition(i);
        bool matches = options.format.parquet.case_insensitive_column_matching
            ? startsWithCaseInsensitiveASCII(requested_column.name, full_prefix)
            : startsWith(requested_column.name, full_prefix);

        if (!matches)
            continue;

        requested_subcolumns.emplace_back(i, requested_column.name.substr(full_prefix.size()));
    }

    return requested_subcolumns;
}

size_t SchemaConverter::schemaIdxAfterSubtree(size_t idx, size_t depth) const
{
    checkSchemaReadDepth(depth);

    if (idx >= file_metadata.schema.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parquet schema tree");

    const auto & element = file_metadata.schema.at(idx);
    size_t next_idx = idx + 1;

    if (!element.__isset.num_children || element.num_children <= 0)
        return next_idx;

    for (Int32 i = 0; i < element.num_children; ++i)
        next_idx = schemaIdxAfterSubtree(next_idx, depth + 1);

    return next_idx;
}

std::string_view SchemaConverter::useColumnMapperIfNeeded(const parq::SchemaElement & element, const String & current_path) const
{
    if (!column_mapper)
        return element.name;
    const auto & map = column_mapper->getFieldIdToClickHouseName();
    if (!element.__isset.field_id)
    {
        /// Does iceberg require that parquet files have field ids?
        /// Our iceberg writer currently doesn't write them.
        //throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Missing field_id for column {}", element.name);
        return element.name;
    }
    auto it = map.find(element.field_id);
    if (it == map.end())
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Parquet file has column {} with field_id {} that is not in datalake metadata", element.name, element.field_id);

    /// At top level (empty path), return the full mapped name. For nested
    /// elements, strip the parent path prefix to get the child name.
    if (current_path.empty())
        return it->second;

    /// Strip "current_path." prefix to get the child name (preserves dots in child names)
    std::string_view mapped = it->second;
    if (mapped.starts_with(current_path) && mapped.size() > current_path.size()
        && mapped[current_path.size()] == '.')
        return mapped.substr(current_path.size() + 1);

    return it->second;
}

void SchemaConverter::processSubtree(TraversalNode & node, size_t depth)
{
    checkSchemaReadDepth(depth);
    /// A deeply nested schema (e.g. a long chain of REQUIRED groups) recurses here per level.
    /// The definition-level cap below only counts OPTIONAL/REPEATED nesting, so without this an
    /// untrusted file could overflow the stack instead of throwing.
    checkStackSize();

    if (node.type_hint)
        chassert(node.requested);
    if (schema_idx >= file_metadata.schema.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parquet schema tree");
    node.element = &file_metadata.schema.at(schema_idx);
    schema_idx += 1;
    node.appendSchemaPathComponent(node.element->name);

    std::optional<size_t> idx_in_output_block;
    size_t wrap_in_arrays = 0;
    DataTypePtr outer_type_hint;
    auto applyTypeHintFromMetadata = [&]()
    {
        if (!sample_block && !node.type_hint && !node.type_hint_path.empty())
        {
            auto it = clickhouse_variant_type_hints.find(node.type_hint_path);
            if (it != clickhouse_variant_type_hints.end())
                node.type_hint = DB::Parquet::getVariantTypeHintFromMetadata(it->second, options.format.parquet.enable_json_parsing);
        }
    };

    if (node.schema_context == SchemaContext::None)
    {
        if (!node.variant || !node.variant->suppress_name_component)
            node.appendNameComponent(node.element->name, useColumnMapperIfNeeded(*node.element, node.name));
        else
            node.variant->suppress_name_component = false;

        applyTypeHintFromMetadata();

        outer_type_hint = node.type_hint;

        if (sample_block && (!node.variant || !node.variant->skip_requested_lookup))
        {
            /// Doing this lookup on each schema element to support reading individual tuple elements.
            /// E.g.:
            ///   insert into function file('t.parquet') select [(10,20,30)] as x;
            ///   select * from file('t.parquet', Parquet, '`x.2` Array(UInt8)'); -- outputs [20]
            std::optional<size_t> pos = sample_block->findPositionByName(node.name, options.format.parquet.case_insensitive_column_matching);
            if (pos.has_value())
            {
                const auto & sample_column = sample_block->getByPosition(pos.value());
                if (node.requested
                    && (!sample_column.name.contains('.') || !node.type_hint || !node.type_hint->equals(*sample_column.type)))
                {
                    throw Exception(
                        ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE,
                        "Requested column {} is part of another requested column",
                        node.getNameForLogging());
                }

                node.requested = true;
                node.name = sample_column.name; // match case
                node.type_hint = sample_column.type;
                outer_type_hint = node.type_hint; // before unwrapping arrays

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
        else
        {
            if (node.variant)
                node.variant->skip_requested_lookup = false;
        }
    }
    else
    {
        if (node.schema_context == SchemaContext::ListElement)
            node.appendTypeHintPathComponent("element");

        outer_type_hint = node.type_hint;
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

    if (node.schema_context == SchemaContext::ListElement)
        applyTypeHintFromMetadata();

    /// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md

    if (!processSubtreePrimitive(node) &&
        !processSubtreeVariant(node, depth) &&
        !processSubtreeVariantTypedWrapper(node, depth) &&
        !processSubtreeMap(node, depth) &&
        !processSubtreeArrayOuter(node, depth) &&
        !processSubtreeArrayInner(node, depth))
    {
        processSubtreeTuple(node, depth);
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
        array.input_type = std::make_shared<DataTypeArray>(array_element.output_type);
        array.output_type = array.input_type;
        array.nested_columns = {*node.output_idx};
        array.primitive_dependencies = array_element.primitive_dependencies;
        array.rep = rep;
        array.variant_preserve_empty_typed_fields = array_element.variant_preserve_empty_typed_fields;
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
            make_array(static_cast<UInt8>(levels[prev_levels_size - 1].rep - i));

        output_columns[node.output_idx.value()].idx_in_output_block = idx_in_output_block;
    }

    if (outer_type_hint)
    {
        OutputColumnInfo & output = output_columns.at(node.output_idx.value());
        output.output_type = outer_type_hint;
        output.needs_cast = !output.output_type->equals(*output.input_type);
    }
}

static bool isPrimitiveNode(const parq::SchemaElement & elem)
{
    /// `parquet.thrift` says "[num_children] is not set when the element is a primitive type".
    /// If it's set but has value 0, logically it should be an empty tuple/struct.
    /// But in practice some writers are sloppy about it and set this field to 0 (rather than unset)
    /// for primitive columns. E.g.
    /// tests/queries/0_stateless/data_hive/partitioning/non_existing_column=Elizabeth/sample.parquet
    return !elem.__isset.num_children || (elem.num_children == 0 && elem.__isset.type);
}

void SchemaConverter::skipSchemaSubtree(size_t depth)
{
    checkSchemaReadDepth(depth);

    if (schema_idx >= file_metadata.schema.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parquet schema tree");

    const auto & element = file_metadata.schema.at(schema_idx);
    ++schema_idx;

    if (isPrimitiveNode(element))
    {
        ++primitive_column_idx;
        return;
    }

    if (!element.__isset.num_children || element.num_children < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parquet schema tree");

    for (Int32 i = 0; i < element.num_children; ++i)
        skipSchemaSubtree(depth + 1);
}

size_t SchemaConverter::addVariantPrimitiveColumnAt(
    const parq::SchemaElement & element,
    size_t parquet_column_idx,
    size_t parquet_schema_idx,
    const std::vector<LevelInfo> & parent_levels,
    const String & name,
    bool output_nullable,
    bool preserve_unexpanded_nullable)
{
    chassert(isPrimitiveNode(element));
    chassert(element.__isset.type && element.type == parq::Type::BYTE_ARRAY);

    /// If any ancestor (including the VARIANT group itself) is OPTIONAL, the leaf
    /// must also be nullable — rows where the whole VARIANT is absent need to
    /// produce NULL.  The root level starts with is_array=true, so
    /// !levels.back().is_array detects at least one OPTIONAL ancestor.
    if (!output_nullable && !parent_levels.back().is_array)
        output_nullable = true;

    size_t primitive_idx = primitive_columns.size();
    PrimitiveColumnInfo & primitive = primitive_columns.emplace_back();
    primitive.column_idx = parquet_column_idx;
    primitive.schema_idx = parquet_schema_idx;
    primitive.name = name;
    primitive.levels = parent_levels;
    primitive.output_nullable = output_nullable;
    primitive.preserve_unexpanded_nullable = preserve_unexpanded_nullable;
    primitive.decoder.physical_type = element.type;
    primitive.decoder.allow_stats = true;
    primitive.decoder.string_converter = std::make_shared<TrivialStringConverter>();
    primitive.decoded_type = std::make_shared<DataTypeString>();
    primitive.output_type = primitive.output_nullable ? makeNullable(primitive.decoded_type) : primitive.decoded_type;

    for (const auto & level : parent_levels)
        if (level.is_array)
            primitive.max_array_def = level.def;

    if (element.repetition_type != parq::FieldRepetitionType::REQUIRED)
    {
        LevelInfo level = parent_levels.back();
        ++level.def;
        level.is_array = false;
        primitive.levels.push_back(level);
    }

    if (!primitive.levels.empty())
    {
        primitive.max_array_def = 0;
        for (const auto & level : primitive.levels)
            if (level.is_array)
                primitive.max_array_def = level.def;
    }

    return primitive_idx;
}

size_t SchemaConverter::addVariantPrimitiveColumn(
    const parq::SchemaElement & element,
    const String & name,
    bool output_nullable,
    bool preserve_unexpanded_nullable)
{
    size_t primitive_idx = addVariantPrimitiveColumnAt(
        element,
        primitive_column_idx,
        schema_idx,
        levels,
        name,
        output_nullable,
        preserve_unexpanded_nullable);

    ++primitive_column_idx;
    ++schema_idx;
    return primitive_idx;
}

size_t SchemaConverter::addParsedObjectSource(size_t primitive_idx, String storage_name, DataTypePtr parsed_object_type)
{
    size_t source_idx = parsed_object_sources.size();
    auto & source = parsed_object_sources.emplace_back();
    source.primitive_idx = primitive_idx;
    source.storage_name = std::move(storage_name);
    source.parsed_object_type = std::move(parsed_object_type);
    return source_idx;
}

size_t SchemaConverter::addVariantSource(
    size_t metadata_primitive_idx,
    size_t value_primitive_idx,
    size_t typed_value_output_idx,
    bool string_output_uses_json)
{
    std::optional<size_t> state_slot_idx;
    std::optional<size_t> metadata_state_slot_idx;

    for (size_t source_idx = 0; source_idx < variant_sources.size(); ++source_idx)
    {
        const auto & source = variant_sources[source_idx];
        if (source.metadata_primitive_idx != metadata_primitive_idx)
            continue;

        if (!metadata_state_slot_idx.has_value())
            metadata_state_slot_idx = source.metadata_state_slot_idx;

        if (source.value_primitive_idx != value_primitive_idx)
            continue;

        if (!state_slot_idx.has_value())
            state_slot_idx = source.state_slot_idx;

        if (source.typed_value_output_idx == typed_value_output_idx
            && source.string_output_uses_json == string_output_uses_json)
        {
            return source_idx;
        }
    }

    if (!metadata_state_slot_idx.has_value())
        metadata_state_slot_idx = variant_metadata_state_slots++;
    if (!state_slot_idx.has_value())
        state_slot_idx = variant_source_state_slots++;

    size_t source_idx = variant_sources.size();
    auto & source = variant_sources.emplace_back();
    source.metadata_primitive_idx = metadata_primitive_idx;
    source.value_primitive_idx = value_primitive_idx;
    source.typed_value_output_idx = typed_value_output_idx;
    source.state_slot_idx = *state_slot_idx;
    source.metadata_state_slot_idx = *metadata_state_slot_idx;
    source.string_output_uses_json = string_output_uses_json;
    return source_idx;
}

void SchemaConverter::addPrimitiveDependency(OutputColumnInfo & output, size_t primitive_idx)
{
    if (primitive_idx == UINT64_MAX)
        return;

    if (std::find(output.primitive_dependencies.begin(), output.primitive_dependencies.end(), primitive_idx) == output.primitive_dependencies.end())
        output.primitive_dependencies.push_back(primitive_idx);
}

void SchemaConverter::addOutputDependencies(OutputColumnInfo & output, const OutputColumnInfo & dependency_output)
{
    for (size_t primitive_idx : dependency_output.primitive_dependencies)
        addPrimitiveDependency(output, primitive_idx);
}

size_t SchemaConverter::getOrAddVariantMetadataPrimitive(const TraversalNode & node, const String & name)
{
    if (!node.variant || !node.variant->shared_metadata_primitive || node.variant->metadata_schema_idx == UINT64_MAX || node.variant->metadata_column_idx == UINT64_MAX)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing shared `Parquet` `VARIANT` metadata dependency for column {}", node.getNameForLogging());

    if (node.variant->shared_metadata_primitive->has_value())
        return node.variant->shared_metadata_primitive->value();

    const auto & element = file_metadata.schema.at(node.variant->metadata_schema_idx);
    chassert(isPrimitiveNode(element));
    chassert(element.__isset.type && element.type == parq::Type::BYTE_ARRAY);

    size_t primitive_idx = primitive_columns.size();
    PrimitiveColumnInfo & primitive = primitive_columns.emplace_back();
    primitive.column_idx = node.variant->metadata_column_idx;
    primitive.schema_idx = node.variant->metadata_schema_idx;
    primitive.name = name;
    primitive.levels = node.variant->metadata_levels;
    primitive.output_nullable = element.repetition_type != parq::FieldRepetitionType::REQUIRED
        || !primitive.levels.back().is_array;
    primitive.decoder.physical_type = element.type;
    primitive.decoder.allow_stats = true;
    primitive.decoder.string_converter = std::make_shared<TrivialStringConverter>();
    primitive.decoded_type = std::make_shared<DataTypeString>();
    primitive.output_type = primitive.output_nullable ? makeNullable(primitive.decoded_type) : primitive.decoded_type;

    for (const auto & level : primitive.levels)
        if (level.is_array)
            primitive.max_array_def = level.def;

    if (element.repetition_type != parq::FieldRepetitionType::REQUIRED)
    {
        LevelInfo level = primitive.levels.back();
        ++level.def;
        level.is_array = false;
        primitive.levels.push_back(level);
    }

    if (!primitive.levels.empty())
    {
        primitive.max_array_def = 0;
        for (const auto & level : primitive.levels)
            if (level.is_array)
                primitive.max_array_def = level.def;
    }

    *node.variant->shared_metadata_primitive = primitive_idx;
    return primitive_idx;
}

bool SchemaConverter::processSubtreePrimitive(TraversalNode & node)
{
    if (!isPrimitiveNode(*node.element))
        return false;

    primitive_column_idx += 1;
    if (!node.element->__isset.type)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet metadata is missing physical type for column {}", node.getNameForLogging());

    std::vector<std::pair<size_t, String>> requested_object_subcolumns;
    if (!node.requested && sample_block && node.element->type == parq::Type::BYTE_ARRAY && levels.back().rep == 0)
        requested_object_subcolumns = collectRequestedSubcolumns(node.name);

    if (!node.requested && requested_object_subcolumns.empty())
        return true;

    DataTypePtr primitive_type_hint = node.type_hint;
    DataTypePtr low_cardinality_dictionary_type_hint;
    bool output_nullable = false;
    bool output_nullable_if_not_json = false;
    if (primitive_type_hint)
    {
        if (primitive_type_hint->lowCardinality())
        {
            low_cardinality_dictionary_type_hint = assert_cast<const DataTypeLowCardinality &>(*primitive_type_hint).getDictionaryType();
            primitive_type_hint = low_cardinality_dictionary_type_hint;
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
    DataTypePtr decoded_type;
    PageDecoderInfo decoder;
    try
    {
        processPrimitiveColumn(*node.element, primitive_type_hint, decoder, decoded_type, inferred_type, geo_metadata);
    }
    catch (Exception & e)
    {
        if (options.format.parquet.skip_columns_with_unsupported_types_in_schema_inference &&
            (e.code() == ErrorCodes::INCORRECT_DATA || e.code() == ErrorCodes::NOT_IMPLEMENTED))
        {
            if (node.variant && node.variant->inside_typed_value)
            {
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot skip unsupported `typed_value` branch while reading `Parquet` `VARIANT` column {}: skipping it would lose shredded data",
                    node.getNameForLogging());
            }
            return true;
        }
        else
        {
            e.addMessage("column '" + node.getNameForLogging() + "'");
            throw;
        }
    }

    /// GeoParquet types like Point or Polygon can't be inside Nullable.
    /// Geometry (Variant) is also not Nullable-compatible.
    if (typeid_cast<const DataTypeArray *>(inferred_type.get())
        || typeid_cast<const DataTypeTuple *>(inferred_type.get())
        || typeid_cast<const DataTypeVariant *>(inferred_type.get()))
    {
        output_nullable = false;
        output_nullable_if_not_json = false;
    }

    const bool primitive_output_nullable = output_nullable || (output_nullable_if_not_json && !typeid_cast<const DataTypeObject *>(inferred_type.get()));
    const DataTypePtr primitive_decoded_type = decoded_type ? decoded_type : inferred_type;
    DataTypePtr primitive_column_type = primitive_decoded_type;
    DataTypePtr primitive_output_type = primitive_decoded_type;
    DataTypePtr low_cardinality_dictionary_type;
    if (low_cardinality_dictionary_type_hint && low_cardinality_dictionary_type_hint->equals(*primitive_decoded_type))
    {
        low_cardinality_dictionary_type = primitive_decoded_type;
        if (primitive_output_nullable)
            low_cardinality_dictionary_type = std::make_shared<DataTypeNullable>(low_cardinality_dictionary_type);

        primitive_column_type = std::make_shared<DataTypeLowCardinality>(low_cardinality_dictionary_type);
        primitive_output_type = primitive_column_type;
    }
    else if (primitive_output_nullable)
    {
        primitive_output_type = std::make_shared<DataTypeNullable>(primitive_output_type);
    }

    auto add_primitive = [&](const String & output_name) -> size_t
    {
        size_t primitive_idx = primitive_columns.size();
        PrimitiveColumnInfo & primitive = primitive_columns.emplace_back();
        primitive.column_idx = primitive_column_idx - 1;
        primitive.schema_idx = schema_idx - 1;
        primitive.name = output_name;
        primitive.levels = levels;
        primitive.output_nullable = primitive_output_nullable;
        primitive.decoder = decoder;
        primitive.decoded_type = primitive_column_type;
        primitive.output_type = primitive_output_type;
        primitive.low_cardinality_dictionary_type = low_cardinality_dictionary_type;
        for (const auto & level : levels)
            if (level.is_array)
                primitive.max_array_def = level.def;
        return primitive_idx;
    };

    if (!requested_object_subcolumns.empty())
    {
        auto get_parsed_object_type = [&]() -> DataTypePtr
        {
            if (auto it = clickhouse_variant_type_hints.find(node.name); it != clickhouse_variant_type_hints.end())
                return makeExplicitParsedObjectTypeFromMetadataHint(it->second, primitive_output_nullable);

            if (node.type_hint)
                return makeExplicitParsedObjectTypeFromTypeHint(node.type_hint, primitive_output_nullable);

            /// Compatibility fallback for projected subcolumn reads from opaque `BYTE_ARRAY`.
            /// When the query only requests `j.a`, the parent `j JSON(...)` type hint is not
            /// available on this path, but historically the reader still treated the payload as
            /// parsed `JSON`.
            DataTypePtr type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);
            if (primitive_output_nullable)
                type = std::make_shared<DataTypeNullable>(std::move(type));
            return type;
        };

        DataTypePtr parsed_object_type = get_parsed_object_type();
        if (!parsed_object_type)
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Cannot read subcolumns of `Parquet` column {} because explicit object-like metadata or type hint is incompatible. "
                "Expected `{}` metadata or an explicit `Dynamic`/`JSON` type hint.",
                node.getNameForLogging(),
                CLICKHOUSE_VARIANT_TYPE_HINTS_METADATA_KEY);
        }

        size_t parsed_object_primitive_idx = add_primitive(node.name);
        size_t parsed_object_source_idx = addParsedObjectSource(parsed_object_primitive_idx, node.name, parsed_object_type);

        for (const auto & [idx_in_output_block, subcolumn_name] : requested_object_subcolumns)
        {
            const auto & requested_column = sample_block->getByPosition(idx_in_output_block);

            OutputColumnInfo & output = output_columns.emplace_back();
            output.idx_in_output_block = idx_in_output_block;
            output.name = requested_column.name;
            output.primitive_start = parsed_object_primitive_idx;
            output.primitive_end = parsed_object_primitive_idx + 1;
            output.input_type = requested_column.type;
            output.output_type = requested_column.type;
            output.source_kind = OutputColumnInfo::SourceKind::ParsedObject;
            output.source_idx = parsed_object_source_idx;
            output.source_subcolumn_name = subcolumn_name;
            addPrimitiveDependency(output, parsed_object_primitive_idx);
        }

        return true;
    }

    size_t primitive_idx = add_primitive(node.name);

    node.output_idx = output_columns.size();
    OutputColumnInfo & output = output_columns.emplace_back();
    output.name = node.name;
    output.primitive_start = primitive_idx;
    output.primitive_end = primitive_idx + 1;
    output.is_primitive = true;
    output.input_type = primitive_output_type;
    output.output_type = node.type_hint
        ? node.type_hint
        : (primitive_output_nullable ? std::make_shared<DataTypeNullable>(inferred_type) : inferred_type);
    output.needs_cast = !output.output_type->equals(*output.input_type);
    addPrimitiveDependency(output, primitive_idx);

    return true;
}

bool SchemaConverter::processSubtreeVariant(TraversalNode & node, size_t depth)
{
    if (isPrimitiveNode(*node.element))
        return false;

    /// Only recognize VARIANT columns with an explicit logical type annotation.
    /// Without it, a struct with children named "metadata"/"value" would be
    /// incorrectly hijacked from the normal tuple path.
    if (!node.element->logicalType.__isset.VARIANT)
        return false;

    const std::vector<std::pair<size_t, String>> requested_variant_subcolumns
        = levels.back().rep == 0 ? collectRequestedSubcolumns(node.name) : std::vector<std::pair<size_t, String>> {};

    if (!node.element->__isset.num_children || node.element->num_children < 2)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT` column {}: expected at least `metadata` and `value` children", node.getNameForLogging());

    bool has_metadata = false;
    bool has_value = false;
    size_t metadata_primitive_idx = UINT64_MAX;
    size_t value_primitive_idx = UINT64_MAX;
    size_t value_column_idx = UINT64_MAX;
    size_t value_schema_idx = UINT64_MAX;
    std::vector<LevelInfo> value_levels;
    bool value_output_nullable = false;
    size_t typed_value_output_idx = UINT64_MAX;
    size_t primitive_start = primitive_columns.size();
    size_t output_start = output_columns.size();

    for (Int32 i = 0; i < node.element->num_children; ++i)
    {
        if (schema_idx >= file_metadata.schema.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parquet schema tree");

        const auto & child = file_metadata.schema.at(schema_idx);

        if (child.name == "metadata")
        {
            if (has_metadata)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT` column {}: duplicate `metadata` child", node.getNameForLogging());
            if (!isPrimitiveNode(child) || !child.__isset.type || child.type != parq::Type::BYTE_ARRAY)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT` column {}: `metadata` must be `BYTE_ARRAY`", node.getNameForLogging());

            if (!node.variant)
                node.variant.emplace();
            if (!node.variant->shared_metadata_primitive)
                node.variant->shared_metadata_primitive = std::make_shared<std::optional<size_t>>();
            node.variant->metadata_column_idx = primitive_column_idx;
            node.variant->metadata_schema_idx = schema_idx;
            node.variant->metadata_levels = levels;
            if (node.requested)
            {
                metadata_primitive_idx = addVariantPrimitiveColumn(child, node.name + ".metadata", /*output_nullable=*/ false);
                *node.variant->shared_metadata_primitive = metadata_primitive_idx;
            }
            else
                skipSchemaSubtree(depth + 1);
            has_metadata = true;
        }
        else if (child.name == "value")
        {
            if (has_value)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT` column {}: duplicate `value` child", node.getNameForLogging());
            if (!isPrimitiveNode(child) || !child.__isset.type || child.type != parq::Type::BYTE_ARRAY)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT` column {}: `value` must be `BYTE_ARRAY`", node.getNameForLogging());

            value_column_idx = primitive_column_idx;
            value_schema_idx = schema_idx;
            value_levels = levels;
            value_output_nullable = child.repetition_type != parq::FieldRepetitionType::REQUIRED;
            if (node.requested)
                value_primitive_idx = addVariantPrimitiveColumn(child, node.name + ".value", value_output_nullable);
            else
                skipSchemaSubtree(depth + 1);
            has_value = true;
        }
        else if (child.name == "typed_value")
        {
            if (node.requested || !requested_variant_subcolumns.empty())
            {
                auto typed_node = node.prepareToRecurse(SchemaContext::None, nullptr);
                if (!typed_node.variant)
                    typed_node.variant.emplace();
                typed_node.variant->inside_typed_value = true;
                typed_node.variant->suppress_name_component = !node.requested && !requested_variant_subcolumns.empty();
                if (node.variant)
                {
                    typed_node.variant->metadata_column_idx = node.variant->metadata_column_idx;
                    typed_node.variant->metadata_schema_idx = node.variant->metadata_schema_idx;
                    typed_node.variant->metadata_levels = node.variant->metadata_levels;
                    typed_node.variant->shared_metadata_primitive = node.variant->shared_metadata_primitive;
                }
                processSubtree(typed_node, depth + 1);
                if (typed_node.output_idx.has_value())
                    typed_value_output_idx = *typed_node.output_idx;
            }
            else
            {
                skipSchemaSubtree(depth + 1);
            }
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT` column {}: unexpected child `{}`", node.getNameForLogging(), child.name);
        }
    }

    if (!has_metadata || !has_value)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT` column {}: missing `metadata` or `value` child", node.getNameForLogging());

    std::vector<std::pair<size_t, String>> residual_variant_subcolumns;
    for (const auto & [idx_in_output_block, subcolumn_name] : requested_variant_subcolumns)
    {
        bool covered_by_typed_value = false;
        if (!node.requested)
        {
            for (size_t output_idx = output_start; output_idx < output_columns.size(); ++output_idx)
            {
                if (output_columns[output_idx].idx_in_output_block == idx_in_output_block)
                {
                    covered_by_typed_value = true;
                    break;
                }
            }
        }

        if (!covered_by_typed_value)
            residual_variant_subcolumns.emplace_back(idx_in_output_block, subcolumn_name);
    }

    const bool need_variant_subcolumn_outputs = !residual_variant_subcolumns.empty();

    if (!node.requested && !need_variant_subcolumn_outputs)
        return true;

    if (need_variant_subcolumn_outputs)
    {
        if (metadata_primitive_idx == UINT64_MAX)
            metadata_primitive_idx = getOrAddVariantMetadataPrimitive(node, node.name + ".metadata");

        if (value_primitive_idx == UINT64_MAX)
        {
            if (value_schema_idx == UINT64_MAX || value_column_idx == UINT64_MAX)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing shared `Parquet` `VARIANT` value dependency for column {}", node.getNameForLogging());

            value_primitive_idx = addVariantPrimitiveColumnAt(
                file_metadata.schema.at(value_schema_idx),
                value_column_idx,
                value_schema_idx,
                value_levels,
                node.name + ".value",
                value_output_nullable);
        }
    }

    if (node.requested)
    {
        bool variant_output_nullable = false;
        if (!options.schema_inference_force_not_nullable)
        {
            if (!levels.back().is_array || value_output_nullable)
                variant_output_nullable = true;
            else if (options.schema_inference_force_nullable)
                variant_output_nullable = !options.format.parquet.enable_json_parsing || options.format.schema_inference_make_json_columns_nullable;
        }

        DataTypePtr inferred_type = getDefaultVariantInferredType(options, variant_output_nullable);
        DataTypePtr output_type = node.type_hint ? node.type_hint : inferred_type;
        if (!isVariantLikeType(output_type.get()))
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Requested type of column {} doesn't match parquet schema: parquet type is `VARIANT`, requested type is {}", node.getNameForLogging(), output_type->getName());

        node.output_idx = output_columns.size();
        OutputColumnInfo & output = output_columns.emplace_back();
        output.name = node.name;
        output.primitive_start = primitive_start;
        output.primitive_end = primitive_columns.size();
        output.input_type = output_type;
        output.output_type = output_type;
        output.source_kind = OutputColumnInfo::SourceKind::Variant;
        output.source_idx = addVariantSource(metadata_primitive_idx, value_primitive_idx, typed_value_output_idx, /*string_output_uses_json=*/ true);
        addPrimitiveDependency(output, metadata_primitive_idx);
        addPrimitiveDependency(output, value_primitive_idx);
        if (typed_value_output_idx != UINT64_MAX)
            addOutputDependencies(output, output_columns.at(typed_value_output_idx));
        if (typed_value_output_idx != UINT64_MAX)
            output.variant_preserve_empty_typed_fields = output_columns.at(typed_value_output_idx).variant_preserve_empty_typed_fields;
    }

    if (need_variant_subcolumn_outputs)
    {
        size_t source_idx = addVariantSource(metadata_primitive_idx, value_primitive_idx, typed_value_output_idx, /*string_output_uses_json=*/ false);
        for (const auto & [idx_in_output_block, subcolumn_name] : residual_variant_subcolumns)
        {
            const auto & requested_column = sample_block->getByPosition(idx_in_output_block);

            OutputColumnInfo & output = output_columns.emplace_back();
            output.idx_in_output_block = idx_in_output_block;
            output.name = requested_column.name;
            output.primitive_start = primitive_start;
            output.primitive_end = primitive_columns.size();
            output.input_type = requested_column.type;
            output.output_type = requested_column.type;
            output.source_kind = OutputColumnInfo::SourceKind::Variant;
            output.source_idx = source_idx;
            output.source_subcolumn_name = subcolumn_name;
            addPrimitiveDependency(output, metadata_primitive_idx);
            addPrimitiveDependency(output, value_primitive_idx);
            if (typed_value_output_idx != UINT64_MAX)
            {
                addOutputDependencies(output, output_columns.at(typed_value_output_idx));
                output.variant_preserve_empty_typed_fields = output_columns.at(typed_value_output_idx).variant_preserve_empty_typed_fields;
            }
        }
    }

    return true;
}

bool SchemaConverter::processSubtreeVariantTypedWrapper(TraversalNode & node, size_t depth)
{
    if (isPrimitiveNode(*node.element) || !node.variant || !node.variant->inside_typed_value)
        return false;

    if (!node.element->__isset.num_children || node.element->num_children < 1 || node.element->num_children > 2)
        return false;

    bool has_value_child = false;
    bool has_typed_value_child = false;
    size_t child_idx = schema_idx;
    for (Int32 i = 0; i < node.element->num_children; ++i)
    {
        if (child_idx >= file_metadata.schema.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parquet schema tree");

        const auto & child = file_metadata.schema.at(child_idx);
        if (child.name == "value")
        {
            if (has_value_child)
                return false;
            has_value_child = true;
        }
        else if (child.name == "typed_value")
        {
            if (has_typed_value_child)
                return false;
            has_typed_value_child = true;
        }
        else
        {
            return false;
        }

        child_idx = schemaIdxAfterSubtree(child_idx, depth + 1);
    }

    if (!has_typed_value_child)
        return false;

    /// Wrapper-shaped `typed_value` groups are part of the standard shredded
    /// `VARIANT` encoding, so files written by other implementations may not
    /// carry ClickHouse-specific footer metadata. Use
    /// `ClickHouse.variant_wrapper_paths` only as an optional narrowing hint
    /// when it is present.
    if (has_clickhouse_variant_wrapper_paths_metadata
        && !clickhouse_variant_wrapper_paths.contains(node.schema_path))
    {
        return false;
    }

    const bool has_requested_descendants = !node.requested && hasRequestedDescendantColumn(node.name);
    const IDataType * unwrapped_type_hint = unwrapVariantTypeHint(node.type_hint).get();
    const bool direct_exact_request
        = node.requested
        && node.type_hint
        && !typeid_cast<const DataTypeObject *>(unwrapped_type_hint)
        && !typeid_cast<const DataTypeDynamic *>(unwrapped_type_hint);
    const bool dynamic_or_json_request
        = node.requested
        && node.type_hint
        && (typeid_cast<const DataTypeObject *>(unwrapped_type_hint)
            || typeid_cast<const DataTypeDynamic *>(unwrapped_type_hint));
    const bool implicit_dynamic_request
        = node.requested
        && !node.type_hint;
    const bool can_read_wrapper_directly = !has_value_child;
    const bool can_use_exact_typed_value_output
        = direct_exact_request
        && node.type_hint
        && !isComplexVariantExactOutputType(node.type_hint);
    const bool need_wrapper_variant_merge
        = (direct_exact_request || dynamic_or_json_request || implicit_dynamic_request)
        && !can_use_exact_typed_value_output
        && !can_read_wrapper_directly
        && ((node.variant->shared_metadata_primitive && node.variant->shared_metadata_primitive->has_value())
            || node.variant->metadata_schema_idx != UINT64_MAX);

    if (!has_requested_descendants && !direct_exact_request && !dynamic_or_json_request && !implicit_dynamic_request && !can_read_wrapper_directly)
        return false;

    size_t primitive_start = primitive_columns.size();
    size_t value_primitive_idx = UINT64_MAX;
    size_t typed_value_output_idx = UINT64_MAX;

    for (Int32 i = 0; i < node.element->num_children; ++i)
    {
        if (schema_idx >= file_metadata.schema.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parquet schema tree");

        const auto & child = file_metadata.schema.at(schema_idx);
        if (child.name == "typed_value")
        {
            auto typed_node = node.prepareToRecurse(SchemaContext::None, node.type_hint);
            if (!typed_node.variant)
                typed_node.variant.emplace();
            typed_node.variant->suppress_name_component = true;
            typed_node.variant->skip_requested_lookup = direct_exact_request || need_wrapper_variant_merge;
            processSubtree(typed_node, depth + 1);
            typed_value_output_idx = typed_node.output_idx.value_or(UINT64_MAX);
            if (!need_wrapper_variant_merge)
            {
                if ((can_read_wrapper_directly || can_use_exact_typed_value_output) && typed_node.output_idx.has_value())
                    output_columns.at(*typed_node.output_idx).variant_preserve_empty_typed_fields = true;
                node.output_idx = typed_node.output_idx;
            }
        }
        else if (child.name == "value")
        {
            if (need_wrapper_variant_merge)
            {
                bool output_nullable = child.repetition_type != parq::FieldRepetitionType::REQUIRED;
                bool preserve_unexpanded_nullable = node.type_hint && node.type_hint->lowCardinality();
                value_primitive_idx = addVariantPrimitiveColumn(child, node.name + ".value", output_nullable, preserve_unexpanded_nullable);
            }
            else
            {
                skipSchemaSubtree(depth + 1);
            }
        }
        else
        {
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Malformed nested `Parquet` `VARIANT` field {}: unexpected child `{}`",
                node.getNameForLogging(),
                child.name);
        }
    }

    if (need_wrapper_variant_merge && (value_primitive_idx != UINT64_MAX || typed_value_output_idx != UINT64_MAX))
    {
        size_t metadata_primitive_idx = UINT64_MAX;
        if (node.variant->shared_metadata_primitive && node.variant->shared_metadata_primitive->has_value())
            metadata_primitive_idx = node.variant->shared_metadata_primitive->value();
        else
            metadata_primitive_idx = getOrAddVariantMetadataPrimitive(node, node.name + ".__variant_metadata");

        node.output_idx = output_columns.size();
        OutputColumnInfo & output = output_columns.emplace_back();
        DataTypePtr output_type = node.type_hint ? node.type_hint : std::make_shared<DataTypeDynamic>();
        output.name = node.name;
        output.primitive_start = primitive_start;
        output.primitive_end = primitive_columns.size();
        output.input_type = output_type;
        output.output_type = output_type;
        output.source_kind = OutputColumnInfo::SourceKind::Variant;
        output.source_idx = addVariantSource(metadata_primitive_idx, value_primitive_idx, typed_value_output_idx, /*string_output_uses_json=*/ false);
        addPrimitiveDependency(output, metadata_primitive_idx);
        addPrimitiveDependency(output, value_primitive_idx);
        if (typed_value_output_idx != UINT64_MAX)
        {
            addOutputDependencies(output, output_columns.at(typed_value_output_idx));
            output.variant_preserve_empty_typed_fields = true;
        }
    }

    return true;
}

bool SchemaConverter::processSubtreeMap(TraversalNode & node, size_t depth)
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
            /// Support explicitly requesting Array(Tuple) type for map columns. Useful e.g. if the map
            /// key type is something that's not allowed as Map key in clickhouse.
            array_type_hint = node.type_hint;
            no_map = true;
        }
        else if (typeid_cast<const DataTypeObject *>(node.type_hint.get()))
        {
            // (We'll produce Map, then do castColumn to JSON.)
        }
        else
        {
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Requested type of column {} doesn't match parquet schema: parquet type is Map, requested type is {}", node.getNameForLogging(), node.type_hint->getName());
        }
    }
    /// (MapTupleAsPlainTuple is needed to skip a level in the column name: it changes
    /// `my_map.key_value.key` to `my_map.key`.
    TraversalNode subnode = node.prepareToRecurse(no_map ? SchemaContext::MapTupleAsPlainTuple : SchemaContext::MapTuple, array_type_hint);
    processSubtree(subnode, depth + 1);

    if (!node.requested || !subnode.output_idx.has_value())
        return true;
    size_t array_idx = subnode.output_idx.value();

    if (no_map)
    {
        node.output_idx = array_idx;
    }
    else
    {
        /// Add an OutputColumnInfo to wrap the Array in a Map.
        /// (Why not just use `needs_cast` to cast Array to Map? Because we may need two cast steps:
        //   Array -> Map -> JSON, if node.type_hint is DataTypeObject.)

        node.output_idx = output_columns.size();
        OutputColumnInfo & output = output_columns.emplace_back();
        const OutputColumnInfo & array = output_columns.at(array_idx);

        output.name = node.name;
        output.primitive_start = array.primitive_start;
        output.primitive_end = array.primitive_end;
        output.input_type = std::make_shared<DataTypeMap>(array.output_type);
        output.output_type = output.input_type;
        output.nested_columns = {array_idx};
        output.primitive_dependencies = array.primitive_dependencies;
        output.rep = array.rep;
        output.variant_preserve_empty_typed_fields = array.variant_preserve_empty_typed_fields;
    }

    return true;
}

bool SchemaConverter::processSubtreeArrayOuter(TraversalNode & node, size_t depth)
{
    /// Array:
    ///   required/optional group `name` (List):
    ///     repeated group "list":
    ///       <recurse> "element"
    ///
    /// I.e. it's a double-wrapped burrito. To unwrap it into one Array, we have to coordinate
    /// across two levels of recursion: processSubtreeArrayOuter for the outer wrapper,
    /// processSubtreeArrayInner for the inner wrapper.
    ///
    /// But hudi writes arrays differently, without the inner group:
    ///   required/optional group `name` (List):
    ///     repeated <recurse> "array"
    /// This probably makes it indinsinguishable from a single-element tuple.

    if (node.element->converted_type != parq::ConvertedType::LIST && !node.element->logicalType.__isset.LIST)
        return false;
    if (node.schema_context != SchemaContext::None && node.schema_context != SchemaContext::ListElement)
        return false;
    if (node.element->num_children != 1)
        return false;
    const parq::SchemaElement & child = file_metadata.schema.at(schema_idx);
    if (child.repetition_type != parq::FieldRepetitionType::REPEATED)
        return false;

    bool has_inner_group = child.num_children == 1;

    TraversalNode subnode = node.prepareToRecurse(has_inner_group ? SchemaContext::ListTuple : SchemaContext::ListElement, node.type_hint);
    processSubtree(subnode, depth + 1);

    if (!node.requested || !subnode.output_idx.has_value())
        return true;

    node.output_idx = subnode.output_idx;

    return true;
}

bool SchemaConverter::processSubtreeArrayInner(TraversalNode & node, size_t depth)
{
    if (node.schema_context != SchemaContext::ListTuple)
        return false;

    /// Array (middle schema element).
    chassert(node.element->repetition_type == parq::FieldRepetitionType::REPEATED &&
             node.element->num_children == 1); // caller checked this
    /// (type_hint is already unwrapped to be element type, because of REPEATED)
    TraversalNode subnode = node.prepareToRecurse(SchemaContext::ListElement, node.type_hint);

    if (column_mapper && schema_idx < file_metadata.schema.size())
    {
        const auto & elem_schema = file_metadata.schema.at(schema_idx);
        if (elem_schema.__isset.field_id)
        {
            const auto & field_id_map = column_mapper->getFieldIdToClickHouseName();
            if (auto it = field_id_map.find(elem_schema.field_id); it != field_id_map.end())
                subnode.name = std::string(it->second);
        }
    }

    processSubtree(subnode, depth + 1);

    if (!node.requested || !subnode.output_idx.has_value())
        return true;

    node.output_idx = subnode.output_idx;

    return true;
}

void SchemaConverter::processSubtreeTuple(TraversalNode & node, size_t depth)
{
    /// Tuple (possibly a Map key_value tuple):
    ///   (required|optional) group `name`:
    ///     <recurse> `name1`
    ///     <recurse> `name2`
    ///     ...

    const DataTypeTuple * tuple_type_hint = typeid_cast<const DataTypeTuple *>(node.type_hint.get());
    if (node.type_hint && !tuple_type_hint && !typeid_cast<const DataTypeObject *>(node.type_hint.get()))
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
    bool infer_tuple_structure = false;
    std::vector<size_t> elements;
    if (tuple_type_hint)
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
            elements.resize(size_t(node.element->num_children), UINT64_MAX);
        }
    }
    else if (node.requested)
    {
        infer_tuple_structure = true;
        elements.reserve(size_t(node.element->num_children));
    }

    Strings names;
    DataTypes types;
    if (infer_tuple_structure)
    {
        names.reserve(size_t(node.element->num_children));
        types.reserve(size_t(node.element->num_children));
    }

    size_t primitive_start = primitive_columns.size();
    size_t output_start = output_columns.size();
    std::vector<String> element_names_in_file;
    for (size_t i = 0; i < size_t(node.element->num_children); ++i)
    {
        const String & element_name = element_names_in_file.emplace_back(useColumnMapperIfNeeded(file_metadata.schema.at(schema_idx), node.name));
        std::optional<size_t> idx_in_output_tuple = tuple_type_hint ? std::make_optional(i) : std::nullopt;
        if (lookup_by_name)
        {
            idx_in_output_tuple = tuple_type_hint->tryGetPositionByName(element_name, options.format.parquet.case_insensitive_column_matching);

            if (idx_in_output_tuple.has_value() && idx_in_output_tuple.value() >= elements.size())
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Internal error while matching tuple {} by name: child `{}` resolved to position {}, but tuple hint has only {} elements",
                    node.getNameForLogging(),
                    element_name,
                    idx_in_output_tuple.value(),
                    elements.size());
            }

            if (idx_in_output_tuple.has_value() && elements.at(idx_in_output_tuple.value()) != UINT64_MAX)
                throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Parquet tuple {} has multiple elements with name `{}`", node.getNameForLogging(), element_name);
        }

        DataTypePtr element_type_hint;
        if (tuple_type_hint && idx_in_output_tuple.has_value())
            element_type_hint = tuple_type_hint->getElement(idx_in_output_tuple.value());

        const bool element_requested = node.requested && (infer_tuple_structure || idx_in_output_tuple.has_value());

        TraversalNode subnode = node.prepareToRecurse(SchemaContext::None, element_type_hint);
        subnode.requested = element_requested;
        if (node.schema_context == SchemaContext::MapTuple && idx_in_output_tuple == 0)
            subnode.schema_context = SchemaContext::MapKey;

        processSubtree(subnode, depth + 1);
        auto element_idx = subnode.output_idx;

        if (element_requested)
        {
            if (!element_idx.has_value())
            {
                if (tuple_type_hint || node.schema_context == SchemaContext::MapTuple)
                {
                    /// If one of the elements is skipped, skip the whole tuple.
                    /// Remove previous elements.
                    primitive_columns.resize(primitive_start);
                    output_columns.resize(output_start);
                    return;
                }
                else
                {
                    continue;
                }
            }

            const auto & type = output_columns.at(element_idx.value()).output_type;
            if (tuple_type_hint)
            {
                if (idx_in_output_tuple.value() >= elements.size())
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Internal error while assembling tuple {}: child `{}` mapped to position {}, but tuple has only {} positions",
                        node.getNameForLogging(),
                        element_name,
                        idx_in_output_tuple.value(),
                        elements.size());
                }

                elements.at(idx_in_output_tuple.value()) = element_idx.value();
                chassert(type->equals(*element_type_hint));
            }
            else
            {
                elements.push_back(element_idx.value());
                names.push_back(element_name);
                types.push_back(type);
            }
        }
    }

    if (node.schema_context == SchemaContext::MapTuple && !elements.empty() && elements[0] != UINT64_MAX)
    {
        auto & key_output = output_columns.at(elements[0]);
        auto normalized_key_type = removeNullableOrLowCardinalityNullable(key_output.output_type);
        if (!normalized_key_type->equals(*key_output.output_type))
        {
            key_output.output_type = normalized_key_type;
            key_output.needs_cast = !key_output.output_type->equals(*key_output.input_type);

            if (infer_tuple_structure)
                types[0] = key_output.output_type;
        }
    }

    if (!node.requested)
        return;

    /// Map tuple in parquet has elements: {"key" , "value" },
    /// but DataTypeMap requires:          {"keys", "values"}.
    if (node.schema_context == SchemaContext::MapTuple)
        names = {"keys", "values"};

    DataTypePtr output_type;
    if (tuple_type_hint)
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
            missing_output.input_type = tuple_type_hint->getElement(i);
            missing_output.output_type = missing_output.input_type;
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
    output.input_type = std::move(output_type);
    output.output_type = output.input_type;
    output.nested_columns = elements;
    for (size_t element_idx : elements)
    {
        if (element_idx != UINT64_MAX)
            addOutputDependencies(output, output_columns.at(element_idx));

        if (element_idx != UINT64_MAX && output_columns.at(element_idx).variant_preserve_empty_typed_fields)
            output.variant_preserve_empty_typed_fields = true;
    }
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

    if (type_hint && type_hint->getName() == "Geometry" && type == parq::Type::BYTE_ARRAY)
    {
        GeoColumnMetadata iceberg_geo{GeoEncoding::WKB, GeoType::Mixed};
        out_inferred_type = getGeoDataType(GeoType::Mixed);
        out_decoder.string_converter = std::make_shared<GeoConverter>(iceberg_geo);
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

        size_t physical_bits = 0;
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

        UInt32 scale = 0;
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

        out_inferred_type = std::make_shared<DataTypeUUID>();
        out_decoder.allow_stats = true; // UUIDs support min/max stats
        out_decoder.fixed_size_converter = std::make_shared<UUIDConverter>();
        return;
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
                WhichDataType which(type_hint->getTypeId());

                /// Handle explicit UUID type hint (e.g. SELECT x::UUID)
                if (which.isUUID() && element.type_length == 16)
                {
                    out_inferred_type = type_hint;
                    out_decoder.fixed_size_converter = std::make_shared<UUIDConverter>();
                    out_decoder.allow_stats = true;
                    return;
                }

                /// Legacy ClickHouse binary formats for [U]Int128 and [U]Int256.
                /// These are written as FIXED_LEN_BYTE_ARRAY(16/32) but without logical types.
                if (which.isInteger() && !which.isNativeInteger() &&
                    type_hint->getSizeOfValueInMemory() == size_t(element.type_length))
                {
                    out_inferred_type = type_hint;
                }
            }

            /// Automatic Inference: If no hint is provided, but the Parquet
            /// file metadata explicitly flags this column as a UUID.
            if (logical.__isset.UUID && element.type_length == 16)
            {
                out_inferred_type = std::make_shared<DataTypeUUID>();
                out_decoder.fixed_size_converter = std::make_shared<UUIDConverter>();
                out_decoder.allow_stats = true;
                return;
            }

            /// Default Fallback: If it's not a UUID or a BigInt hint, treat it as FixedString.
            if (!out_inferred_type)
                out_inferred_type = std::make_shared<DataTypeFixedString>(size_t(element.type_length));

            auto converter = std::make_shared<FixedStringConverter>();
            converter->input_size = size_t(element.type_length);
            out_decoder.fixed_size_converter = std::move(converter);

            /// Stats are only allowed for FixedString if the output is actually a string.
            out_decoder.allow_stats = WhichDataType(get_output_type_index()).isString();
            return;
        }
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type: {}", thriftToString(element));
}

}
