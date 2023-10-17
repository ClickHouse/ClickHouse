#pragma once

#include "config.h"

#if USE_ARROW || USE_ORC || USE_PARQUET

#include <DataTypes/IDataType.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Block.h>
#include <arrow/table.h>


namespace DB
{

class Block;
class Chunk;

class ArrowColumnToCHColumn
{
public:
    using NameToColumnPtr = std::unordered_map<std::string, std::shared_ptr<arrow::ChunkedArray>>;

    ArrowColumnToCHColumn(
        const Block & header_,
        const std::string & format_name_,
        bool allow_missing_columns_,
        bool null_as_default_,
        bool case_insensitive_matching_ = false);

    void arrowTableToCHChunk(Chunk & res, std::shared_ptr<arrow::Table> & table, size_t num_rows, BlockMissingValues * block_missing_values = nullptr);

    void arrowColumnsToCHChunk(Chunk & res, NameToColumnPtr & name_to_column_ptr, size_t num_rows, BlockMissingValues * block_missing_values = nullptr);

    /// Transform arrow schema to ClickHouse header. If hint_header is provided,
    /// we will skip columns in schema that are not in hint_header.
    static Block arrowSchemaToCHHeader(
        const arrow::Schema & schema,
        const std::string & format_name,
        bool skip_columns_with_unsupported_types = false,
        const Block * hint_header = nullptr,
        bool ignore_case = false);

    struct DictionaryInfo
    {
        std::shared_ptr<ColumnWithTypeAndName> values;
        Int64 default_value_index = -1;
        UInt64 dictionary_size;
    };


private:
    const Block & header;
    const std::string format_name;
    /// If false, throw exception if some columns in header not exists in arrow table.
    bool allow_missing_columns;
    bool null_as_default;
    bool case_insensitive_matching;

    /// Map {column name : dictionary column}.
    /// To avoid converting dictionary from Arrow Dictionary
    /// to LowCardinality every chunk we save it and reuse.
    std::unordered_map<std::string, DictionaryInfo> dictionary_infos;
};

}

#endif
