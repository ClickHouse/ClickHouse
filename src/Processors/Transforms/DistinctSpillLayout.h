#pragma once

#include <Core/Block_fwd.h>
#include <Core/ColumnNumbers.h>
#include <Core/SortDescription.h>
#include <Processors/Chunk.h>

#include <optional>

namespace DB
{

enum class DistinctKeyRepresentation;

/// Describes the comparison keys and output payload used by external `DISTINCT`. Ordinary runs carry
/// non-constant input columns, while suppression runs carry keys already emitted before spilling.
/// Both carry an already-emitted flag. When input order must be preserved, ordinary rows carry their
/// arrival numbers and suppression rows carry a constant zero for the shared comparator. Generic keys
/// use fingerprints from the hash set.
/// The layout owns these conversions and their metadata; its caller sorts and schedules the runs.
class DistinctSpillLayout
{
public:
    DistinctSpillLayout(
        SharedHeader input_header_, const ColumnNumbers & input_key_columns_pos,
        DistinctKeyRepresentation key_representation_, bool preserve_input_order);

    const SharedHeader & getInputRunHeader() const { return input_run_header; }
    const SharedHeader & getSuppressionRunHeader() const { return suppression_run_header; }
    const SharedHeader & getMergedHeader() const { return merged_header; }
    const SortDescription & getKeySortDescription() const { return key_sort_description; }
    const SortDescription & getRunSortDescription() const { return run_sort_description; }
    const SortDescription & getArrivalNumberSortDescription() const { return arrival_number_sort_description; }
    bool preservesInputOrder() const { return arrival_number_column_pos.has_value(); }

    /// Normalizes ordinary rows, adding fingerprints for generic keys and optional arrival numbers.
    Chunk prepareInputChunk(Chunk chunk, UInt64 first_arrival_number) const;

    /// Adds the emitted flag and an optional constant arrival number to keys returned by the set's extractor.
    Chunk prepareSuppressionChunk(MutableColumns key_columns) const;

    /// Restores constant columns and removes arrival numbers after merging and optional order restoration.
    Chunk restoreOutputChunk(Chunk chunk) const;

private:
    const SharedHeader input_header;
    const DistinctKeyRepresentation key_representation;

    /// Stores input-header positions of non-constant columns in their original order.
    const ColumnNumbers spill_columns_pos;

    /// Positions of the original key columns within the non-constant payload.
    const ColumnNumbers key_columns_pos;
    const std::optional<size_t> arrival_number_column_pos;
    SharedHeader input_run_header;
    SharedHeader suppression_run_header;
    SharedHeader merged_header;
    SortDescription key_sort_description;

    /// Run ordering gives suppression rows precedence. When preserving input order, arrival numbers
    /// select the earliest ordinary row. Key equality ignores the emitted flag and arrival numbers.
    SortDescription run_sort_description;
    SortDescription arrival_number_sort_description;
};

}
