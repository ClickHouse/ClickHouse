#pragma once

#include <Columns/ColumnDynamic.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

namespace DB::Parquet
{

void decodeVariantColumn(
    const IColumn & metadata,
    const IColumn * value,
    const IColumn * typed_value,
    const DataTypePtr & typed_value_type,
    ColumnDynamic & output,
    size_t num_rows,
    size_t max_parser_depth);

}
