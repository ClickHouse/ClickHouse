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
    CHColumnToArrowColumn(const Block & header, const std::string & format_name_, bool low_cardinality_as_dictionary_);

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
};

}

#endif
