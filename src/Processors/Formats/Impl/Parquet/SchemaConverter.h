#pragma once

#include <Processors/Formats/Impl/Parquet/Reader.h>
#include <Processors/Formats/Impl/Parquet/VariantUtils.h>

#include <memory>
#include <unordered_set>

namespace DB
{

class ColumnMapper;

}

namespace DB::Parquet
{

/// Converting parquet schema to clickhouse schema + information for decoding.
/// Used both for schema inference and for reading.
struct SchemaConverter
{
    using PrimitiveColumnInfo = Reader::PrimitiveColumnInfo;
    using OutputColumnInfo = Reader::OutputColumnInfo;
    using ParsedObjectSourceInfo = Reader::ParsedObjectSourceInfo;
    using VariantSourceInfo = Reader::VariantSourceInfo;
    using LevelInfo = Reader::LevelInfo;

    const parq::FileMetaData & file_metadata;
    const ReadOptions & options;
    const Block * sample_block;
    const ColumnMapper * column_mapper = nullptr;
    std::vector<String> external_columns;

    std::vector<PrimitiveColumnInfo> primitive_columns;
    std::vector<ParsedObjectSourceInfo> parsed_object_sources;
    std::vector<VariantSourceInfo> variant_sources;
    std::vector<OutputColumnInfo> output_columns;
    size_t variant_source_state_slots = 0;
    size_t variant_metadata_state_slots = 0;

    size_t schema_idx = 1;
    size_t primitive_column_idx = 0;
    std::vector<LevelInfo> levels;

    /// The key is the parquet column name, without ColumnMapper.
    std::unordered_map<String, GeoColumnMetadata> geo_columns;
    std::unordered_map<String, String> clickhouse_variant_type_hints;
    std::unordered_set<String> clickhouse_variant_wrapper_paths;
    bool has_clickhouse_variant_wrapper_paths_metadata = false;

    SchemaConverter(const parq::FileMetaData &, const ReadOptions &, const Block *);

    void prepareForReading();
    NamesAndTypesList inferSchema();

private:
    /// If we interpret the parquet schema tree in a straightforward way, ignoring List/Map type
    /// annotations, we get some extra layers of tuples:
    /// Instead of `foo Array(Int64)` we'd get `foo Tuple(list Array(Tuple(element Int64)))`.
    /// Instead of `bar Map(String, Int64)` we'd get `bar Tuple(key_value Array(Tuple(key String, value Int64)))`.
    /// To avoid adding these extra Tuple layers, we have this enum that tells a recursive call
    /// where it is located in the group of SchemaElement-s representing a List/Map.
    enum class SchemaContext
    {
        None,
        MapTuple,
        MapTupleAsPlainTuple,
        MapKey,
        ListTuple,
        ListElement,
    };

    struct VariantTraversalContext
    {
        bool inside_typed_value = false;
        bool suppress_name_component = false;
        bool skip_requested_lookup = false;
        size_t metadata_column_idx = UINT64_MAX;
        size_t metadata_schema_idx = UINT64_MAX;
        std::vector<LevelInfo> metadata_levels;
        std::shared_ptr<std::optional<size_t>> shared_metadata_primitive;
    };

    /// Parameters of a recursive call that traverses a subtree, corresponding to a parquet SchemaElement.
    struct TraversalNode
    {
        /// Assigned by the caller.
        SchemaContext schema_context = SchemaContext::None;

        /// These fields are assigned by the caller, then updated by the callee.
        /// E.g. name is initially the parent element's name, then the callee appends a path
        /// component to it.
        ///
        /// If there's ColumnMapper, `name` is the mapped name (clickhouse column name), while
        /// `parquet_name` is the name according to the parquet schema.
        /// If `parquet_name` is nullopt, the clickhouse and parquet names are equal.
        String name;
        std::optional<String> parquet_name;
        String schema_path;
        String type_hint_path;
        DataTypePtr type_hint;
        bool requested = false;
        std::optional<VariantTraversalContext> variant;

        /// These are assigned by the callee.
        const parq::SchemaElement * element = nullptr;
        std::optional<size_t> output_idx; // index in output_columns

        const String & getParquetName() const
        {
            return parquet_name.has_value() ? *parquet_name : name;
        }

        String getNameForLogging() const
        {
            if (parquet_name.has_value() && *parquet_name != name)
                return fmt::format("{} (mapped to {})", *parquet_name, name);
            return name;
        }

        void appendNameComponent(const String & parquet_field_name, std::string_view mapped_field_name)
        {
            if (!name.empty())
                name += ".";
            name += mapped_field_name;
            appendTypeHintPathComponent(mapped_field_name);
            if (parquet_name.has_value() || mapped_field_name != parquet_field_name)
            {
                if (parquet_name.has_value())
                    *parquet_name += ".";
                else
                    parquet_name.emplace();
                *parquet_name += parquet_field_name;
            }
        }

        void appendSchemaPathComponent(const String & parquet_field_name)
        {
            schema_path = appendVariantMetadataPath(schema_path, parquet_field_name);
        }

        void appendTypeHintPathComponent(std::string_view field_name)
        {
            type_hint_path = appendVariantMetadataPath(type_hint_path, field_name);
        }

        TraversalNode prepareToRecurse(SchemaContext schema_context_, DataTypePtr type_hint_)
        {
            TraversalNode res = *this;
            res.schema_context = schema_context_;
            res.type_hint = std::move(type_hint_);
            res.element = nullptr;
            res.output_idx.reset();
            return res;
        }
    };

    void checkHasColumns();
    bool hasRequestedDescendantColumn(const String & prefix) const;
    std::vector<std::pair<size_t, String>> collectRequestedSubcolumns(const String & prefix) const;
    void checkSchemaReadDepth(size_t depth) const;
    size_t schemaIdxAfterSubtree(size_t idx, size_t depth) const;
    size_t getOrAddVariantMetadataPrimitive(const TraversalNode & node, const String & name);
    size_t addParsedObjectSource(size_t primitive_idx, String storage_name, DataTypePtr parsed_object_type);
    size_t addVariantSource(
        size_t metadata_primitive_idx,
        size_t value_primitive_idx,
        size_t typed_value_output_idx,
        bool string_output_uses_json,
        bool typed_value_requires_parent_metadata_mapping);
    static void addPrimitiveDependency(OutputColumnInfo & output, size_t primitive_idx);
    void addOutputDependencies(OutputColumnInfo & output, const OutputColumnInfo & dependency_output);
    size_t addVariantPrimitiveColumnAt(
        const parq::SchemaElement & element,
        size_t parquet_column_idx,
        size_t parquet_schema_idx,
        const std::vector<LevelInfo> & parent_levels,
        const String & name,
        bool output_nullable,
        bool preserve_unexpanded_nullable = false);

    void processSubtree(TraversalNode & node, size_t depth);

    /// These functions are used by processSubtree for different kinds of SchemaElement.
    /// Return true if the schema element was recognized as the corresponding kind,
    /// even if no output column needs to be produced.
    bool processSubtreePrimitive(TraversalNode & node);
    bool processSubtreeVariant(TraversalNode & node, size_t depth);
    bool processSubtreeVariantTypedWrapper(TraversalNode & node, size_t depth);
    bool processSubtreeMap(TraversalNode & node, size_t depth);
    bool processSubtreeArrayOuter(TraversalNode & node, size_t depth);
    bool processSubtreeArrayInner(TraversalNode & node, size_t depth);
    void processSubtreeTuple(TraversalNode & node, size_t depth);

    void skipSchemaSubtree(size_t depth);
    size_t addVariantPrimitiveColumn(
        const parq::SchemaElement & element,
        const String & name,
        bool output_nullable,
        bool preserve_unexpanded_nullable = false);

    void processPrimitiveColumn(
        const parq::SchemaElement & element, DataTypePtr type_hint,
        PageDecoderInfo & out_decoder, DataTypePtr & out_decoded_type,
        DataTypePtr & out_inferred_type, std::optional<GeoColumnMetadata> geo_metadata) const;

    /// Returns element.name or a corresponding name from ColumnMapper.
    /// For nested tuple elements, returns just the element name like `x`, not the whole path like `t.x`.
    /// For top-level columns (when current_path is empty), returns the full mapped name to support
    /// column names with dots (e.g. `integer.col` in Iceberg).
    std::string_view useColumnMapperIfNeeded(const parq::SchemaElement & element, const String & current_path) const;
};

}
