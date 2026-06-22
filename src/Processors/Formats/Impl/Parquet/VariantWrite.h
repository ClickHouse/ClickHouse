#pragma once

#include <Columns/IColumn_fwd.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>

#include <map>
#include <memory>

namespace DB::Parquet
{

struct VariantWriteAnalysisNode
{
    size_t value_count = 0;
    size_t object_count = 0;
    size_t array_count = 0;

    std::map<String, std::pair<size_t, DataTypePtr>, std::less<>> scalar_types;
    std::map<String, VariantWriteAnalysisNode, std::less<>> object_fields;
    std::unique_ptr<VariantWriteAnalysisNode> array_child;
};

struct VariantWriteAnalysisEntry
{
    DataTypePtr source_type;
    VariantWriteAnalysisNode analysis;
};

struct PreparedVariantColumns
{
    ColumnPtr metadata_column;
    DataTypePtr metadata_type;
    ColumnPtr value_column;
    DataTypePtr value_type;
    ColumnPtr typed_value_column;
    DataTypePtr typed_value_type;
};

/// Prepare Variant-encoded columns for Parquet output.
///
/// If `shredded_type` is null and `out_shredded_type` is non-null, the function
/// infers a shredded type from the data and writes it to `*out_shredded_type`.
/// The inferred type is also used for encoding in the same call, so inference
/// and encoding happen in a single pass over the data.
///
/// The metadata dictionary is built once from the union of all object keys across
/// all rows in the column chunk, then shared by every row.
PreparedVariantColumns prepareVariantColumnsForWrite(
    const ColumnPtr & column,
    const DataTypePtr & type,
    const FormatSettings & format_settings,
    DataTypePtr shredded_type,
    DataTypePtr * out_shredded_type = nullptr);

void analyzeVariantColumnForWrite(
    const IColumn & column,
    const DataTypePtr & type,
    const FormatSettings & format_settings,
    VariantWriteAnalysisEntry & out_analysis);

DataTypePtr inferVariantShreddedTypeForWrite(const VariantWriteAnalysisEntry & analysis);

}
