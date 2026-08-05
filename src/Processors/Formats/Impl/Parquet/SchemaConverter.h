#pragma once

#include <Processors/Formats/Impl/Parquet/Reader.h>

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
    using LevelInfo = Reader::LevelInfo;

    const parq::FileMetaData & file_metadata;
    const ReadOptions & options;
    Block * sample_block;
    const ColumnMapper * column_mapper = nullptr;
    std::vector<String> external_columns;

    std::vector<PrimitiveColumnInfo> primitive_columns;
    std::vector<OutputColumnInfo> output_columns;

    size_t schema_idx = 1;
    size_t primitive_column_idx = 0;
    std::vector<LevelInfo> levels;
    /// Actual recursion depth of processSubtree. Tracked unconditionally because the def-level
    /// counter only advances for OPTIONAL/REPEATED nodes, so REQUIRED-group nesting would bypass it.
    size_t recursion_depth = 0;
    /// >0 while recursing inside a physically-nullable Tuple group (OPTIONAL group requested as
    /// Nullable(Tuple(...)) and eligible for lossless reading). Leaves under it get
    /// PrimitiveColumnInfo::group_nullable set: their definition-level null map equals the group
    /// null map, so we keep it and later wrap the assembled ColumnTuple in ColumnNullable.
    size_t nullable_tuple_group_depth = 0;

    /// The key is the parquet column name, without ColumnMapper.
    std::unordered_map<String, GeoColumnMetadata> geo_columns;

    /// If precomputed_geo_columns has a value it is used directly (including the empty-map case)
    /// and the constructor skips parsing the "geo" key-value metadata. This ensures that a
    /// failed parse caught by the caller (which leaves an empty map) does not cause
    /// SchemaConverter to re-parse and rethrow. Pass std::nullopt to let SchemaConverter
    /// parse according to its own settings.
    SchemaConverter(const parq::FileMetaData &, const ReadOptions &, Block *,
                    std::optional<std::unordered_map<String, GeoColumnMetadata>> precomputed_geo_columns = std::nullopt);

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
        /// If set, overrides the mapped name of this schema element (used instead of
        /// useColumnMapperIfNeeded). Used to expose fully-shredded Variant fields under their
        /// logical variant path (e.g. `payload.event_type`) instead of the physical shredded
        /// path (e.g. `payload.typed_value.event_type.typed_value`).
        std::optional<String> name_override;
        DataTypePtr type_hint;
        bool requested = false;

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
            if (parquet_name.has_value() || mapped_field_name != parquet_field_name)
            {
                if (parquet_name.has_value())
                    *parquet_name += ".";
                else
                    parquet_name.emplace();
                *parquet_name += parquet_field_name;
            }
        }

        TraversalNode prepareToRecurse(SchemaContext schema_context_, DataTypePtr type_hint_)
        {
            TraversalNode res = *this;
            res.schema_context = schema_context_;
            res.type_hint = std::move(type_hint_);
            res.element = nullptr;
            res.output_idx.reset();
            res.name_override.reset();
            return res;
        }
    };

    void checkHasColumns();

    void processSubtree(TraversalNode & node);

    /// These functions are used by processSubtree for different kinds of SchemaElement.
    /// Return true if the schema element was recognized as the corresponding kind,
    /// even if no output column needs to be produced.
    bool processSubtreePrimitive(TraversalNode & node);
    bool processSubtreeMap(TraversalNode & node);
    bool processSubtreeArrayOuter(TraversalNode & node);
    bool processSubtreeArrayInner(TraversalNode & node);
    bool processSubtreeVariant(TraversalNode & node);
    /// Expose fully-shredded Variant fields (shredded field groups that have no `value` child)
    /// under their logical variant path, so subcolumn requests like `payload.event_type` match
    /// the shredded leaf directly (and only that leaf is read).
    void processSubtreeVariantShreddedAliases(TraversalNode & node, size_t num_field_groups);
    /// Request the whole column of `node` as JSON (appended to extended_sample_block) and
    /// synthesize its requested Object subcolumns as json_subcolumn outputs extracting from it.
    void processSubcolumnFallback(TraversalNode & node, const std::vector<size_t> & subcolumn_positions);
    void synthesizeSubcolumnOutputs(size_t source_idx, const String & column_name);
    void consumeVariantSubtreeUnrequested(TraversalNode & node);
    /// Field names of fully-shredded scalar leaves (shredded field groups without a `value`
    /// child and with a primitive typed_value) in this variant group's typed_value subtree.
    std::unordered_set<String> collectValueLessShreddedPaths(const TraversalNode & node) const;

    /// Read only the leaves on the requested Object subcolumn paths of a Variant column (used
    /// when the paths don't all map to fully-shredded leaves): for each path, address the
    /// shredded field group covering it (or the Variant root) and extract just that value
    /// (OutputColumnInfo::variant_select_path), skipping all other leaves of the Variant subtree.
    void processSubtreeVariantSelective(TraversalNode & node);
    /// Where a requested subcolumn path is addressed within the Variant subtree.
    struct VariantSelectiveTarget
    {
        /// Schema index of the field group element whose value/typed_value children address the
        /// value, or UINT64_MAX when navigation starts at the Variant root.
        size_t group_schema_idx = UINT64_MAX;
        /// Read the root `value` leaf (the path lives in the unshredded root value binary).
        bool need_root_value = false;
        /// Read the root `typed_value` leaf (DuckDB-style primitive typed_value: a JSON document).
        bool need_root_typed = false;
        /// Read the addressed field group's `value` child / `typed_value` child.
        bool need_group_value = false;
        bool need_group_typed = false;
        /// Index into the request's path segments where navigation of the addressed subtree starts.
        size_t remainder_start = 0;
        /// Ancestor field groups descended through, shallowest first: (schema idx, path segment
        /// idx at which the group was entered). Used to retarget the request to an ancestor when
        /// the ancestor's whole typed subtree is materialized for another request.
        std::vector<std::pair<size_t, size_t>> ancestors;
    };
    VariantSelectiveTarget navigateVariantSelectivePath(
        const parq::SchemaElement * typed_group, size_t typed_group_idx, const std::vector<String> & segments) const;
    /// Process one group of variant field groups during selective Variant processing
    /// (schema_idx points at the group element): materialize the addressed field groups'
    /// children, recursing into groups that only have deeper targets and skipping everything
    /// else. `requests` are indexed into the caller's request list; direct_requests maps field
    /// group schema idx to the requests addressing that group exactly.
    struct VariantSelectiveRequest;
    void processSelectiveTypedGroup(
        TraversalNode & node,
        const parq::SchemaElement & group,
        const std::vector<VariantSelectiveRequest> & requests,
        const std::unordered_map<size_t, std::vector<size_t>> & direct_requests,
        std::unordered_map<size_t, std::pair<std::optional<size_t>, std::optional<size_t>>> & group_pieces);
    /// Process one child element during selective Variant processing (schema_idx points at it):
    /// produce its output when materialize, otherwise consume the subtree without outputs
    /// (keeping the physical parquet column index aligned). Returns the output idx if produced.
    std::optional<size_t> processSelectiveChild(TraversalNode & node, bool materialize);
    /// Consume the subtree at schema_idx without producing outputs, keeping the physical
    /// parquet column index aligned.
    void skipSelectiveSubtree();
    /// Push the definition/repetition level of a consumed group element (mirrors processSubtree's
    /// level handling for group elements processed manually during selective Variant reads).
    void pushVariantGroupLevel(const TraversalNode & node, const parq::SchemaElement & group);
    void processSubtreeTuple(TraversalNode & node);

    std::vector<size_t> fallback_subcolumn_positions;
    std::vector<size_t> variant_subcolumn_positions;
    std::optional<size_t> fallback_whole_idx;
    String fallback_node_name;

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
