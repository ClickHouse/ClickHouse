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
    CHColumnToArrowColumn(const Block & header, const std::string & format_name_, bool low_cardinality_as_dictionary_ = false);

    void chChunkToArrowTable(std::shared_ptr<arrow::Table> & res, const Chunk & chunk, size_t columns_num);
private:

#define FOR_INTERNAL_NUMERIC_TYPES(M) \
        M(UInt8, arrow::UInt8Builder) \
        M(Int8, arrow::Int8Builder) \
        M(UInt16, arrow::UInt16Builder) \
        M(Int16, arrow::Int16Builder) \
        M(UInt32, arrow::UInt32Builder) \
        M(Int32, arrow::Int32Builder) \
        M(UInt64, arrow::UInt64Builder) \
        M(Int64, arrow::Int64Builder) \
        M(Float32, arrow::FloatBuilder) \
        M(Float64, arrow::DoubleBuilder)

#define FOR_ARROW_TYPES(M) \
        M(UINT8, arrow::UInt8Type) \
        M(INT8, arrow::Int8Type) \
        M(UINT16, arrow::UInt16Type) \
        M(INT16, arrow::Int16Type) \
        M(UINT32, arrow::UInt32Type) \
        M(INT32, arrow::Int32Type) \
        M(UINT64, arrow::UInt64Type) \
        M(INT64, arrow::Int64Type) \
        M(FLOAT, arrow::FloatType) \
        M(DOUBLE, arrow::DoubleType)  \
        M(STRING, arrow::StringType)

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
