#pragma once

#include <Columns/ColumnDynamic.h>
#include <Columns/IColumn.h>

namespace DB::Parquet
{

void decodeVariantColumn(const IColumn & metadata, const IColumn & value, ColumnDynamic & output, size_t num_rows, size_t max_parser_depth);

}
