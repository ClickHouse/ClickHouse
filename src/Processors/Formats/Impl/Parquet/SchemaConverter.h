#pragma once

#include <Processors/Formats/Impl/Parquet/Reader.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

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

    /// Its column chunks are parsed only where needed, see getGeoTypeFromGeospatialStatistics.
    const FileFooter & file_footer;
    const parq::FileMetaData & file_metadata;
    const ReadOptions & options;
    const Block * sample_block;
    const ColumnMapper * column_mapper = nullptr;
    VectorWithMemoryTracking<String> external_columns;

    VectorWithMemoryTracking<PrimitiveColumnInfo> primitive_columns;
    VectorWithMemoryTracking<OutputColumnInfo> output_columns;

    size_t schema_idx = 1;
    size_t primitive_column_idx = 0;
    VectorWithMemoryTracking<LevelInfo> levels;
    /// Actual recursion depth of processSubtree. Tracked unconditionally because the def-level
    /// counter only advances for OPTIONAL/REPEATED nodes, so REQUIRED-group nesting would bypass it.
    size_t recursion_depth = 0;
    /// >0 while recursing inside a physically-nullable Tuple group (OPTIONAL group requested as
    /// Nullable(Tuple(...)) and eligible for lossless reading). Leaves under it get
    /// PrimitiveColumnInfo::group_nullable set: their definition-level null map equals the group
    /// null map, so we keep it and later wrap the assembled ColumnTuple in ColumnNullable.
    size_t nullable_tuple_group_depth = 0;

    /// The key is the parquet column name, without ColumnMapper.
    UnorderedMapWithMemoryTracking<String, GeoColumnMetadata> geo_columns;

    /// If precomputed_geo_columns has a value it is used directly (including the empty-map case)
    /// and the constructor skips parsing the "geo" key-value metadata. This ensures that a
    /// failed parse caught by the caller (which leaves an empty map) does not cause
    /// SchemaConverter to re-parse and rethrow. Pass std::nullopt to let SchemaConverter
    /// parse according to its own settings.
    SchemaConverter(const FileFooter &, const ReadOptions &, const Block *,
                    std::optional<UnorderedMapWithMemoryTracking<String, GeoColumnMetadata>> precomputed_geo_columns = std::nullopt);

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
        DataTypePtr type_hint;
        bool requested = false;
        /// The element or one of its ancestors is not in the data lake schema (see useColumnMapperIfNeeded).
        bool not_in_datalake_schema = false;

        /// These are assigned by the callee.
        const parq::SchemaElement * element = nullptr;
        std::optional<size_t> output_idx; // index in output_columns

        [[nodiscard]] const String & getParquetName() const
        {
            return parquet_name.has_value() ? *parquet_name : name;
        }

        [[nodiscard]] String getNameForLogging() const
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
            return res;
        }
    };

    /// Every leaf under an array, in schema order, so the leaves of a subtree are a contiguous range.
    struct LeafInArray
    {
        size_t schema_idx = 0;
        size_t column_idx = 0;
        VectorWithMemoryTracking<LevelInfo> levels;
    };
    VectorWithMemoryTracking<LeafInArray> leaves_in_arrays;

    /// A tuple inside an array that is not requested as a whole. A requested subcolumn of an element
    /// it does not have (`arr.missing`) takes the array sizes from it.
    struct TupleInArray
    {
        String name;
        size_t schema_idx = 0;
        size_t leaves_start = 0;
        size_t leaves_end = 0;
        VectorWithMemoryTracking<LevelInfo> levels;
    };
    VectorWithMemoryTracking<TupleInArray> tuples_in_arrays;

    void checkHasColumns();

    void processSubtree(TraversalNode & node);

    /// These functions are used by processSubtree for different kinds of SchemaElement.
    /// Return true if the schema element was recognized as the corresponding kind,
    /// even if no output column needs to be produced.
    bool processSubtreePrimitive(TraversalNode & node);
    bool processSubtreeMap(TraversalNode & node);
    bool processSubtreeArrayOuter(TraversalNode & node);
    bool processSubtreeArrayInner(TraversalNode & node);
    void processSubtreeTuple(TraversalNode & node);
    bool processSubtreeDynamic(TraversalNode & node);

    /// The sizes of an array are stored only in the leaves under it. Adds a primitive column with
    /// PrimitiveColumnInfo::levels_only_arrays = `arrays` and no output, for one of the leaves
    /// leaves_in_arrays[leaves_start, leaves_end). Returns its index in primitive_columns, or nullopt if no leaf is read.
    std::optional<size_t> addLevelsOnlyPrimitive(size_t leaves_start, size_t leaves_end, UInt8 arrays, const String & name);

    /// If the requested column at `pos` is `arr.missing`, where `arr` is in tuples_in_arrays and has
    /// no element `missing`, outputs it as arrays of defaults sized like `arr` and returns true.
    bool addMissingElementOfTupleInArray(size_t pos);

    void processPrimitiveColumn(
        const parq::SchemaElement & element, DataTypePtr type_hint, bool time_as_stored_integer,
        PageDecoderInfo & out_decoder, DataTypePtr & out_decoded_type,
        DataTypePtr & out_inferred_type, std::optional<GeoColumnMetadata> geo_metadata, size_t column_idx) const;

    /// For a column with the `GEOMETRY` or `GEOGRAPHY` logical type: if `GeospatialStatistics` of
    /// all its column chunks list the same single 2D geometry type, returns that type. If any of
    /// them lists a geometry type with Z and/or M coordinates (which cannot be parsed), returns
    /// `std::nullopt`. Otherwise returns `GeoType::Mixed`.
    [[nodiscard]] std::optional<GeoType> getGeoTypeFromGeospatialStatistics(size_t column_idx) const;

    /// Returns element.name or a corresponding name from ColumnMapper.
    /// For nested tuple elements, returns just the element name like `x`, not the whole path like `t.x`.
    /// For top-level columns (when current_path is empty), returns the full mapped name to support
    /// column names with dots (e.g. `integer.col` in Iceberg).
    std::string_view useColumnMapperIfNeeded(
        const parq::SchemaElement & element, const String & current_path, bool & out_not_in_schema) const;
};

}
