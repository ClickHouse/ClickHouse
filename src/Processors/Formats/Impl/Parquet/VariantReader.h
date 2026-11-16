#pragma once

#include <Processors/Formats/Impl/Parquet/VariantBinaryDecoder.h>
#include <Processors/Formats/Impl/Parquet/Reader.h>

namespace DB::Parquet::VariantReader
{

SourceState & getOrPrepareSourceState(
    Reader & reader,
    Reader::RowSubgroup & row_subgroup,
    const Reader::OutputColumnInfo & output_info,
    const Reader::VariantSourceInfo & source_info,
    size_t num_rows);

MutableColumnPtr formOutputColumn(
    Reader & reader,
    Reader::RowSubgroup & row_subgroup,
    const Reader::OutputColumnInfo & output_info,
    const Reader::VariantSourceInfo & source_info,
    size_t num_rows);

}
