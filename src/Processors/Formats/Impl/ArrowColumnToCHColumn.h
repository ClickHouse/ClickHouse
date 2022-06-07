#pragma once

#include "config_formats.h"

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

    ArrowColumnToCHColumn(const Block & header_, std::shared_ptr<arrow::Schema> schema_, const std::string & format_name_, const FormatSettings & settings_);

    void arrowTableToCHChunk(Chunk & res, std::shared_ptr<arrow::Table> & table, BlockMissingValues & block_missing_values);

    void arrowColumnsToCHChunk(Chunk & res, NameToColumnPtr & name_to_column_ptr, BlockMissingValues & block_missing_values);

    /// Transform arrow schema to ClickHouse header. If hint_header is provided,
    /// we will skip columns in schema that are not in hint_header.
    static Block arrowSchemaToCHHeader(
        const arrow::Schema & schema,
        const std::string & format_name,
        bool skip_columns_with_unsupported_types = false,
        const Block * hint_header = nullptr,
        bool ignore_case = false);

private:
    /// Update missing columns that exists in header but not in arrow::Schema
    void updateMissingColumns();

    const Block & header;
    std::shared_ptr<arrow::Schema> schema;
    const std::string format_name;
    bool import_nested;
    /// If false, throw exception if some columns in header not exists in arrow table.
    bool allow_missing_columns;
    bool case_insensitive_matching;
    // bool null_as_default;
    bool defaults_for_omitted_fields;

    std::vector<size_t> missing_columns;

    /// Map {column name : dictionary column}.
    /// To avoid converting dictionary from Arrow Dictionary
    /// to LowCardinality every chunk we save it and reuse.
    std::unordered_map<std::string, std::shared_ptr<ColumnWithTypeAndName>> dictionary_values;
};

}

#endif
