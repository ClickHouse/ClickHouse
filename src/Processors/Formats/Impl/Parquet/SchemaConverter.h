#pragma once

#include <Processors/Formats/Impl/Parquet/Reader.h>

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
    std::vector<String> external_columns;

    std::vector<PrimitiveColumnInfo> primitive_columns;
    std::vector<OutputColumnInfo> output_columns;

    size_t schema_idx = 1;
    size_t primitive_column_idx = 0;
    std::vector<LevelInfo> levels;

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

    void checkHasColumns();

    std::optional<size_t> processSubtree(String name, bool requested, DataTypePtr type_hint, SchemaContext);

    /// These functions are used by processSubtree for different kinds of SchemaElement.
    /// Return true if the schema element was recognized as the corresponding kind,
    /// even if no output column needs to be produced.
    bool processSubtreePrimitive(const String & name, bool requested, DataTypePtr type_hint, SchemaContext schema_context, const parq::SchemaElement & element, std::optional<size_t> & output_idx);
    bool processSubtreeMap(const String & name, bool requested, DataTypePtr type_hint, SchemaContext schema_context, const parq::SchemaElement & element, std::optional<size_t> & output_idx);
    bool processSubtreeArrayOuter(const String & name, bool requested, DataTypePtr type_hint, SchemaContext schema_context, const parq::SchemaElement & element, std::optional<size_t> & output_idx);
    bool processSubtreeArrayInner(const String & name, bool requested, DataTypePtr type_hint, SchemaContext schema_context, const parq::SchemaElement & element, std::optional<size_t> & output_idx);
    void processSubtreeTuple(const String & name, bool requested, DataTypePtr type_hint, SchemaContext schema_context, const parq::SchemaElement & element, std::optional<size_t> & output_idx);

    void processPrimitiveColumn(
        const parq::SchemaElement & element, DataTypePtr type_hint,
        PageDecoderInfo & out_decoder, DataTypePtr & out_decoded_type,
        DataTypePtr & out_inferred_type, std::optional<GeoColumnMetadata> geo_metadata) const;
};

}
