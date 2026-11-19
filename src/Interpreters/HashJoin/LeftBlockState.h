#pragma once

#include <Interpreters/HashJoin/AddedColumns.h>
#include <Common/PODArray.h>

#include <memory>
#include <optional>
#include <vector>

namespace DB::JoinProbe
{

/// What the key search found for one left row in one ON disjunct: 0 if the key is not there, is NULL, or the ON section
/// masks the row out. Otherwise the `RowRef` word (`MapsOne`), the `RowRefList` word (`MapsAll`), the address of the key's
/// `SortedLookupVectorBase` (`MapsAsof`), or 1 (`MapsSet`).
using MatchWord = UInt64;

/// What the probe of one left block keeps for one ON disjunct: its key columns, and what the key searches build from them.
struct DisjunctState
{
    /// The key columns of the disjunct, made from the whole source block.
    JoinOnKeyColumns columns;
    /// See `keptKeyGetter`. A kept getter may cache cells of the table that the disjunct probes, as the `LowCardinality`
    /// getter does per dictionary index. That is safe for the whole block. The probe never changes the table: its build has
    /// finished, or the probe holds the `Join` engine's read lock. And a key always has its cell in that table.
    std::shared_ptr<void> key_getter;
    /// See `skipBytesOf`.
    std::optional<const UInt8 *> skip;
    IColumn::Filter skip_buffer;
};

/// Everything the probe of one left block keeps or works in, except the block and the outputs that go to
/// `HashJoinResult`. The row limit can cut the probe of a block into several calls of `joinBlockImpl`, and all of them
/// share this state. The first call fills it, and the call that finishes the block clears it before the output is built.
/// One thread at a time probes a block, so the state needs no lock.
struct LeftBlockState
{
    std::vector<DisjunctState> disjuncts;
    /// The key type of the first call. The types of the kept key getters follow from it.
    HashJoin::Type key_type{};

    /// The work of the two-step paths for the current batch, over all disjuncts. Their contents are dead between batches.
    /// Only their capacity lasts across the calls of the block.
    /// The match words of the batch (`JoinRightColumnsData::match_words`).
    PaddedPODArray<MatchWord> match_words;
    /// For a join with an additional filter and a used flag per key: the cell offset of the key of each row of the batch.
    PODArray<size_t> key_offsets;
    /// For a join that stops at the first match: the rows of the batch that no disjunct before the current one matched.
    PaddedPODArray<UInt64> unmatched_rows;
    /// With an additional filter: the pairs that match on the keys, and the end offset of the pairs of each left row.
    PODArray<UInt64> selected_rows;
    IColumn::Offsets row_replicate_offset;
    /// With several disjuncts: the right rows that one disjunct of a left row added. `addFoundRowAll` empties it for each
    /// disjunct of each row, and the later disjuncts of the row skip these rows.
    std::vector<UInt64> new_known_rows;
};

}
