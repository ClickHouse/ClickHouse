#pragma once

#include <cstdint>
#include <unordered_map>

#include "config.h"

#if USE_ARROW || USE_ORC || USE_PARQUET

#include <DataTypes/IDataType.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Block.h>
#include <arrow/table.h>
#include <Formats/FormatSettings.h>
#include <Core/BlockMissingValues.h>

namespace DB
{

class Chunk;

class ArrowColumnToCHColumn
{
public:
    ArrowColumnToCHColumn(
        const Block & header_,
        const std::string & format_name_,
        const FormatSettings & format_settings_,
        const std::optional<std::unordered_map<String, String>> & parquet_columns_to_clickhouse_,
        const std::optional<std::unordered_map<String, String>> & clickhouse_columns_to_parquet_,
        bool allow_missing_columns_,
        bool null_as_default_,
        FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior_,
        bool allow_geoparquet_parser_,
        bool case_insensitive_matching_ = false,
        bool is_stream_ = false,
        bool enable_json_parsing_ = true);

    Chunk arrowTableToCHChunk(
        const std::shared_ptr<arrow::Table> & table,
        size_t num_rows,
        std::shared_ptr<const arrow::KeyValueMetadata> metadata,
        BlockMissingValues * block_missing_values = nullptr);

    /// Transform arrow schema to ClickHouse header
    static Block arrowSchemaToCHHeader(
        const arrow::Schema & schema,
        std::shared_ptr<const arrow::KeyValueMetadata> metadata,
        const std::string & format_name,
        const FormatSettings & format_settings,
        bool skip_columns_with_unsupported_types = false,
        bool allow_inferring_nullable_columns = true,
        bool case_insensitive_matching = false,
        bool allow_geoparquet_parser = true,
        bool enable_json_parsing = true,
        const std::optional<std::unordered_map<String, String>> & parquet_columns_to_clickhouse = std::nullopt,
        const std::optional<std::unordered_map<String, String>> & clickhouse_columns_to_parquet = std::nullopt);

    struct DictionaryInfo
    {
        std::shared_ptr<ColumnWithTypeAndName> values;
        Int64 default_value_index = -1;
        UInt64 dictionary_size;
    };

private:
    struct ArrowColumn
    {
        std::shared_ptr<arrow::ChunkedArray> column;
        std::shared_ptr<arrow::Field> field;
    };

    using NameToArrowColumn = std::unordered_map<std::string, ArrowColumn>;

    Chunk arrowColumnsToCHChunk(
        const NameToArrowColumn & name_to_arrow_column,
        size_t num_rows,
        std::shared_ptr<const arrow::KeyValueMetadata> metadata,
        BlockMissingValues * block_missing_values);

    const Block & header;
    const std::string format_name;

    FormatSettings format_settings;
    /// If false, throw exception if some columns in header not exists in arrow table.
    bool allow_missing_columns;
    bool null_as_default;
    FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior;
    bool allow_geoparquet_parser;
    bool case_insensitive_matching;
    bool is_stream;
    bool enable_json_parsing;

    /// Map {column name : dictionary column}.
    /// To avoid converting dictionary from Arrow Dictionary
    /// to LowCardinality every chunk we save it and reuse.
    std::unordered_map<std::string, DictionaryInfo> dictionary_infos;

    std::optional<std::unordered_map<String, String>> parquet_columns_to_clickhouse;
    std::optional<std::unordered_map<String, String>> clickhouse_columns_to_parquet;
};

}

#endif
