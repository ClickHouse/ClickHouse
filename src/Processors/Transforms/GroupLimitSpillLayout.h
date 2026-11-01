#pragma once

#include <Core/Block_fwd.h>
#include <Core/ColumnNumbers.h>
#include <Core/SortDescription.h>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>
#include <Processors/Transforms/SeenGroupExtractor.h>

#include <optional>

namespace DB
{

/// Describes the comparison keys and output payload used when a group limit (`DISTINCT` or `LIMIT BY`)
/// spills. Input runs carry non-constant input columns and optional arrival numbers. Seen runs carry only
/// comparison keys. Both carry the number of input rows seen per group before spilling: zero for input rows, at
/// least one for seen rows. Generic keys use the fingerprints retained by the in-memory filter.
/// The layout owns these conversions and their metadata; its caller sorts and schedules the runs.
class GroupLimitSpillLayout
{
public:
    /// `seen_rows_type_` is an unsigned integer type wide enough for the seen counts of the operator.
    GroupLimitSpillLayout(
        SharedHeader input_header_, const ColumnNumbers & input_key_columns_pos,
        SpillKeyRepresentation key_representation_, bool preserve_input_order, DataTypePtr seen_rows_type_);

    const SharedHeader & getInputRunHeader() const { return input_run_header; }
    const SharedHeader & getSeenRunHeader() const { return seen_run_header; }
    const SharedHeader & getMergedHeader() const { return merged_header; }
    const SortDescription & getKeySortDescription() const { return key_sort_description; }
    const SortDescription & getRunSortDescription() const { return run_sort_description; }
    const String & getSeenRowsColumnName() const { return seen_rows_column_name; }
    const SortDescription & getArrivalNumberSortDescription() const { return arrival_number_sort_description; }
    bool preservesInputOrder() const { return arrival_number_column_pos.has_value(); }

    /// Estimates the dense service columns added to input rows: fingerprints and arrival numbers.
    /// Available before constructing the layout so hashing can reserve memory to start spilling.
    static size_t estimateServiceColumnsMemory(
        size_t num_rows, SpillKeyRepresentation key_representation, bool preserve_input_order);

    /// Normalizes input rows, adding fingerprints for generic keys and optional arrival numbers.
    Chunk prepareInputChunk(Chunk chunk, UInt64 first_arrival_number) const;

    /// Combines the owning comparison-key columns returned by a `SeenGroupExtractor` with their seen counts.
    Chunk prepareSeenChunk(SeenGroups seen) const;

    /// Restores constant columns and removes arrival numbers after merging and optional order restoration.
    Chunk restoreOutputChunk(Chunk chunk) const;

private:
    const SharedHeader input_header;
    const SpillKeyRepresentation key_representation;
    const DataTypePtr seen_rows_type;
    /// Stores input-header positions of non-constant columns in their original order.
    const ColumnNumbers spill_columns_pos;
    /// Positions of the original key columns within the non-constant payload.
    const ColumnNumbers key_columns_pos;
    const std::optional<size_t> arrival_number_column_pos;
    SharedHeader input_run_header;
    SharedHeader seen_run_header;
    SharedHeader merged_header;
    SortDescription key_sort_description;
    /// Run ordering puts seen rows before input rows with equal keys; key comparisons ignore the seen counts.
    SortDescription run_sort_description;
    /// The number of input rows seen per group before spilling, the last column of `run_sort_description`.
    String seen_rows_column_name;
    SortDescription arrival_number_sort_description;
};

}
