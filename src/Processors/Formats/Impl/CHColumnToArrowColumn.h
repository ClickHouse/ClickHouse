#pragma once
#include "config_formats.h"

#if USE_ARROW || USE_PARQUET

#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <arrow/table.h>


namespace DB
{

class CHColumnToArrowColumn
{
public:
    CHColumnToArrowColumn(const Block & header, const std::string & format_name_, bool low_cardinality_as_dictionary_, bool output_string_as_string_);

    void chChunkToArrowTable(std::shared_ptr<arrow::Table> & res, const Chunk & chunk, size_t columns_num);

private:
    ColumnsWithTypeAndName header_columns;
    std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
    const std::string format_name;
    bool low_cardinality_as_dictionary;
    /// Map {column name : arrow dictionary}.
    /// To avoid converting dictionary from LowCardinality to Arrow
    /// Dictionary every chunk we save it and reuse.
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> dictionary_values;

    /// We should initialize arrow fields on first call of chChunkToArrowTable, not in constructor
    /// because LowCardinality column from header always has indexes type UInt8, so, we should get
    /// proper indexes type from first chunk of data.
    bool is_arrow_fields_initialized = false;

    /// Output columns with String data type as Arrow::String type.
    /// By default Arrow::Binary is used.
    bool output_string_as_string = false;
};

}

#endif
