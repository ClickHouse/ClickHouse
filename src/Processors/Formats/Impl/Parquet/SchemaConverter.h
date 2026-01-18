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
    const Block * sample_block;
    const ColumnMapper * column_mapper = nullptr;
    std::vector<String> external_columns;

    std::vector<PrimitiveColumnInfo> primitive_columns;
    std::vector<OutputColumnInfo> output_columns;

    size_t schema_idx = 1;
    size_t primitive_column_idx = 0;
    std::vector<LevelInfo> levels;

    /// The key is the parquet column name, without ColumnMapper.
    std::unordered_map<String, GeoColumnMetadata> geo_columns;

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
    void processSubtreeTuple(TraversalNode & node);

    void processPrimitiveColumn(
        const parq::SchemaElement & element, DataTypePtr type_hint,
        PageDecoderInfo & out_decoder, DataTypePtr & out_decoded_type,
        DataTypePtr & out_inferred_type, std::optional<GeoColumnMetadata> geo_metadata) const;

    /// Returns element.name or a corresponding name from ColumnMapper.
    /// For tuple elements, that's just the element name like `x`, not the whole path like `t.x`.
    std::string_view useColumnMapperIfNeeded(const parq::SchemaElement & element) const;
};

}
