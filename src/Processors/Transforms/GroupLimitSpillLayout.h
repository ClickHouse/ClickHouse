#pragma once

#include <Core/Block_fwd.h>
#include <Core/ColumnNumbers.h>
#include <Core/NamesAndTypes.h>
#include <Core/SortDescription.h>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>
#include <Processors/Transforms/SeenGroupExtractor.h>

#include <optional>

namespace DB
{

/// Describes the comparison keys and output payload used when a group limit (`DISTINCT` or `LIMIT BY`)
/// spills. Input runs carry non-constant input columns, optional arrival numbers, and the fingerprints of
/// generic keys. Seen runs carry only comparison keys and the number of input rows seen per group before
/// spilling, at least one. Input runs carry no seen counts: the final merge counts their rows as unseen (see
/// `GroupLimitSortedAlgorithm`), so the files store no constant column. Generic keys use the fingerprints
/// retained by the in-memory filter. The layout owns these conversions and their metadata; its caller sorts
/// and schedules the runs.
class GroupLimitSpillLayout
{
public:
    /// Seen counts are clamped to `group_limit_end_`, the exclusive end of the kept window, and stored in the
    /// narrowest unsigned type that holds it: every count at or past the window end suppresses the rest of
    /// its group alike. With `attach_arrival_numbers`, input rows record their arrival numbers, which restore
    /// the input order after merging.
    GroupLimitSpillLayout(
        SharedHeader input_header_, const ColumnNumbers & input_key_columns_pos,
        SpillKeyRepresentation key_representation_, bool attach_arrival_numbers, UInt64 group_limit_end_);

    const SharedHeader & getInputRunHeader() const { return input_run_header; }
    const SharedHeader & getSeenRunHeader() const { return seen_run_header; }
    const SharedHeader & getMergedHeader() const { return merged_header; }
    const SortDescription & getKeySortDescription() const { return key_sort_description; }
    const SortDescription & getRunSortDescription() const { return run_sort_description; }
    NameAndTypePair getSeenRowsColumn() const { return {seen_rows_column_name, seen_rows_type}; }
    size_t getSeenRowsBytes() const { return seen_rows_type->getSizeOfValueInMemory(); }
    const SortDescription & getArrivalNumberSortDescription() const { return arrival_number_sort_description; }

    /// Whether spilled input rows carry every column of `description`, so that they can be sorted by it. They
    /// carry the non-constant columns of `input_header`. Available before constructing the layout.
    static bool spillsSortColumns(const Block & input_header, const SortDescription & description);

    /// Estimates the dense service columns added to input rows: fingerprints and arrival numbers.
    /// Available before constructing the layout so hashing can reserve memory to start spilling.
    static size_t estimateServiceColumnsMemory(
        size_t num_rows, SpillKeyRepresentation key_representation, bool attach_arrival_numbers);

    /// Normalizes input rows, adding fingerprints for generic keys and optional arrival numbers.
    Chunk prepareInputChunk(Chunk chunk, UInt64 first_arrival_number) const;

    /// Combines the owning comparison-key columns returned by a `SeenGroupExtractor` with their seen counts,
    /// clamped and narrowed to the seen-count type of the layout.
    Chunk prepareSeenChunk(SeenGroups seen) const;

    /// Restores constant columns and removes arrival numbers after merging and optional order restoration.
    Chunk restoreOutputChunk(Chunk chunk) const;

private:
    const SharedHeader input_header;
    const SpillKeyRepresentation key_representation;
    const UInt64 group_limit_end;
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
    /// The order of the seen runs, the order of the final merge's inputs (see
    /// `GroupLimitSortedAlgorithm::inputSortDescription`); key comparisons ignore the seen counts.
    SortDescription run_sort_description;
    /// The number of input rows seen per group before spilling, the last column of `run_sort_description`.
    /// The name differs from the names of the input-run columns, which lack it.
    String seen_rows_column_name;
    SortDescription arrival_number_sort_description;
};

}
