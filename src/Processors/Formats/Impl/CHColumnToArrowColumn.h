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


public:
    static void chChunkToArrowTable(std::shared_ptr<arrow::Table> & res, const Block & header, const Chunk & chunk,
                                    size_t columns_num, String format_name);

    static void fillArrowArray(
        const String & column_name,
        ColumnPtr & nested_column,
        const std::shared_ptr<const IDataType> & column_nested_type,
        const String column_nested_type_name,
        std::shared_ptr<arrow::Array> arrow_array,
        const PaddedPODArray<UInt8> * null_bytemap,
        arrow::ArrayBuilder * array_builder,
        String format_name);
};
}
#endif
