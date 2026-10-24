#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

namespace DB
{
struct FormatSettings;
}

namespace DB::Parquet
{

/// `output` must be a column of `output_type`, which is either `Dynamic` or `JSON`.
void decodeVariantColumn(
    const IColumn & metadata,
    const IColumn & value,
    IColumn & output,
    const DataTypePtr & output_type,
    const String & column_name,
    size_t num_rows,
    const FormatSettings & format_settings);

}
