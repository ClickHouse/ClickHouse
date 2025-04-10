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
    std::vector<PrimitiveColumnInfo> primitive_columns;
    std::vector<OutputColumnInfo> output_columns;

    size_t schema_idx = 1;
    size_t primitive_column_idx = 0;
    std::vector<LevelInfo> levels;

    SchemaConverter(const parq::FileMetaData &, const ReadOptions &, const Block *);

    void prepareForReading();
    NamesAndTypesList inferSchema();

private:
    /// If we interpret the parquet schema tree in a straightforward way, ignoring List/Map type
    /// annotations, we get some extra layers of tuples:
    /// Instead of `foo Array(Int64)` we'd get `foo Tuple(list Array(Tuple(element Int64)))`.
    /// Instead of `bar Map(String, Int64)` we'd get `bar Tuple(key_value Array(Tuple(key String, value Int64)))`.
    /// To avoid adding these extra Tuple types, we have this enum that tells a recursive call where it
    /// is located in the group of SchemaElement-s representing a List/Map.
    enum class SchemaContext
    {
        None,
        MapTuple,
        ListTuple,
        ListElement,
    };

    void checkHasColumns();

    std::optional<size_t> processSubtree(String name, bool requested, DataTypePtr type_hint, SchemaContext);

    void processPrimitiveColumn(
        const parq::SchemaElement & element, const IDataType * type_hint,
        std::unique_ptr<ValueDecoder> & out_decoder, DataTypePtr & out_decoded_type,
        DataTypePtr & out_inferred_type);
};

}
