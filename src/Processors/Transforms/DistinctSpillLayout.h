#pragma once

#include <Core/Block_fwd.h>
#include <Core/ColumnNumbers.h>
#include <Core/SortDescription.h>
#include <Processors/Chunk.h>

#include <optional>

namespace DB
{

/// Describes the column representations used by external `DISTINCT`. Temporary runs omit constant input
/// columns, serialize non-comparable keys, and carry an already-emitted flag and optional arrival numbers.
/// The layout owns these conversions and their metadata; its caller sorts and schedules the runs.
class DistinctSpillLayout
{
public:
    DistinctSpillLayout(
        SharedHeader input_header_, const ColumnNumbers & input_key_columns_pos, bool preserve_input_order);

    const SharedHeader & getSpillHeader() const { return spill_header; }
    const SharedHeader & getMergedHeader() const { return merged_header; }
    const SortDescription & getKeySortDescription() const { return key_sort_description; }
    const SortDescription & getRunSortDescription() const { return run_sort_description; }
    const SortDescription & getArrivalNumberSortDescription() const { return arrival_number_sort_description; }
    size_t getFlagColumnPosition() const { return flag_column_pos; }
    bool preservesInputOrder() const { return arrival_number_column_pos.has_value(); }

    /// Converts an input chunk to the spill layout, with unflagged rows and their arrival numbers.
    Chunk prepareInputChunk(Chunk chunk, UInt64 first_arrival_number) const;

    /// Converts extracted keys to flagged suppression rows. Non-key payload columns contain defaults
    /// because these rows suppress previously emitted keys and are never returned to the caller.
    Chunk prepareSuppressionChunk(MutableColumns key_columns) const;

    /// Converts a merged chunk, whose flag has already been removed, to the original input layout.
    Chunk restoreOutputChunk(Chunk chunk) const;

private:
    Chunk serializeKeysAndAddServiceColumns(Chunk chunk, bool already_emitted, UInt64 first_arrival_number) const;

    const SharedHeader input_header;
    /// Stores input-header positions of non-constant columns in their original order.
    const ColumnNumbers spill_columns_pos;
    /// Key positions and serialized-key positions are relative to the spill header.
    const ColumnNumbers key_columns_pos;
    const ColumnNumbers serialized_key_columns_pos;
    const std::optional<size_t> arrival_number_column_pos;
    const size_t flag_column_pos;
    const SharedHeader spill_header;
    const SharedHeader merged_header;
    const SortDescription key_sort_description;
    /// Run ordering gives suppression rows precedence; deduplication compares only the keys.
    const SortDescription run_sort_description;
    const SortDescription arrival_number_sort_description;
};

}
