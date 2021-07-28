#pragma once

#include "config_formats.h"

#if USE_ARROW || USE_ORC || USE_PARQUET

#include <DataTypes/IDataType.h>
#include <arrow/table.h>


namespace DB
{

class Block;
class Chunk;

class ArrowColumnToCHColumn
{
public:
    ArrowColumnToCHColumn(const Block & header_, std::shared_ptr<arrow::Schema> schema_, const std::string & format_name_);

    void arrowTableToCHChunk(Chunk & res, std::shared_ptr<arrow::Table> & table);

private:
    const Block & header;
    std::unordered_map<std::string, DataTypePtr> name_to_internal_type;
    const std::string format_name;

    /// Map {column name : dictionary column}.
    /// To avoid converting dictionary from Arrow Dictionary
    /// to LowCardinality every chunk we save it and reuse.
    std::unordered_map<std::string, ColumnPtr> dictionary_values;
};

}

#endif
