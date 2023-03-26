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

    ArrowColumnToCHColumn(
        const Block & header_,
        const std::string & format_name_,
        bool import_nested_,
        bool allow_missing_columns_);

    void arrowTableToCHChunk(Chunk & res, std::shared_ptr<arrow::Table> & table);

    void arrowColumnsToCHChunk(Chunk & res, NameToColumnPtr & name_to_column_ptr);

    /// Get missing columns that exists in header but not in arrow::Schema
    std::vector<size_t> getMissingColumns(const arrow::Schema & schema) const;

    static Block arrowSchemaToCHHeader(const arrow::Schema & schema, const std::string & format_name);

private:
    const Block & header;
    const std::string format_name;
    bool import_nested;
    /// If false, throw exception if some columns in header not exists in arrow table.
    bool allow_missing_columns;

    /// Map {column name : dictionary column}.
    /// To avoid converting dictionary from Arrow Dictionary
    /// to LowCardinality every chunk we save it and reuse.
    std::unordered_map<std::string, std::shared_ptr<ColumnWithTypeAndName>> dictionary_values;
};

}

#endif
