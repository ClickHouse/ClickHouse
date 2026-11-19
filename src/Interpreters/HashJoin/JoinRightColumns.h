#pragma once

#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoinTable.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/LookupMethod.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Common/PODArray.h>

#include <memory>
#include <span>

namespace DB::JoinProbe
{

/** The probe of a hash join with several ON disjuncts or an additional filter runs in two steps per batch of left rows:
  *  - The key search looks up the key of every row in every ON disjunct and records what it found. It depends on the key type
  *    and the map, never on the join kind. It only looks up: it sets no used flag.
  *  - The match step does everything that the join kind does with what was found, the used flags included. It depends on the
  *    join kind, never on the key type.
  * `JoinRightColumns` calls both through function pointers that `selectFunctions` picks for each block. A call of `run` may
  * stop at the row limit (`max_joined_block_size_rows`, counted by `added_rows`); the next call goes on with the rest of the
  * block. The key search is in `FindKeys.h`, and what a lookup method implements is in `LookupMethod.h`. The match step and
  * `selectFunctions` are in `JoinRightColumnsImpl.h`.
  */

class JoinRightColumns;

/// With several disjuncts, the maximum number of rows in a batch of `JoinRightColumns::run` (see `Functions::max_batch_rows`).
/// Measured: the time of a batched probe stays flat for batches from 256 to 65536 rows, so this value is not a tuning knob.
constexpr size_t max_disjunct_batch_rows = 8192;

/// Adds the matches of the current batch to the result and returns how many rows of the batch it finished.
using AddMatchesFn = size_t (*)(JoinRightColumns &);

/// The probe of one left block. Built per block, because several threads probe one `HashJoin` at once.
class JoinRightColumns
{
public:
    using Tables = std::span<const std::unique_ptr<HashJoinTable>>;

    struct Functions
    {
        FindKeysFn find_keys[2][2]{}; /// [later disjunct][fast_path]
        AddMatchesFn add_matches[2]{}; /// [need_filter]
        /// The most rows of a batch. The first batch of a call has at most as many rows as the row limit, and each later batch
        /// twice as many as the one before, up to this value. So a call that stops early at the limit wastes few lookups.
        size_t max_batch_rows = 0;
        /// For a join with one disjunct and a used flag per key: the flag is indexed by the key's cell offset, which a match
        /// word does not carry.
        bool records_key_offsets = false;
        bool stops_at_first_match = false;
    };

    JoinRightColumns(
        const Functions & functions_,
        Tables tables_,
        AddedColumns & added_columns_,
        JoinStuff::JoinUsedFlags & used_flags_,
        const ScatteredBlock::Selector & selector);

    /// Returns how many rows of the block it finished: fewer than all when the row limit stopped it, but at least one for a
    /// non-empty block. An empty block still runs one empty batch, so that its result is set up as for any other block.
    size_t run()
    {
        size_t done = 0;
        do
        {
            startBatch(done);
            for (size_t k = 0; k < tables.size(); ++k)
            {
                /// The first disjunct may stop at the row limit. The later ones look up only the rows it reached.
                const size_t visited = findKeys(k);
                if (k == 0)
                    batch_rows = visited;
                keepUnmatchedRows(k);
            }
            /// The keys of every disjunct are found before any match is added: the output of a left row must be contiguous
            /// and in row order, and a right row that two disjuncts reach is added once.
            const size_t finished = add_matches_fn(*this);
            done += finished;
            if (finished < batch_rows)
                break;
        } while (done < rows);
        return done;
    }

    [[nodiscard]] MatchWord * matchWordsOf(size_t k) const { return match_words + k * batch_rows; }

    /// The rows of the batch, or for a join that stops at the first match, the rows that no disjunct before `k` matched.
    [[nodiscard]] ProbeRows rowsToProbe(size_t k) const;

    const Functions & functions;
    const Tables tables;
    AddedColumns & added_columns;
    JoinStuff::JoinUsedFlags & used_flags;
    /// The block rows `[first_row, first_row + rows)`.
    const size_t first_row;
    const size_t rows;
    const size_t max_joined_rows;
    const size_t max_batch_rows;
    size_t next_batch_rows;

    /// The current batch: the block rows `[batch_first_row, batch_first_row + batch_rows)`.
    size_t batch_first_row = 0;
    size_t batch_rows = 0;
    /// `[k * batch_rows + j]` is the match word of disjunct `k` for row `batch_first_row + j`.
    MatchWord * match_words = nullptr;
    /// With `records_key_offsets`: for each row of the batch, the cell offset of its key.
    PODArray<size_t> key_offsets;
    /// What the row limit counts so far in this call: output rows, or with an additional filter, the pairs that match on the keys.
    IColumn::Offset added_rows = 0;

private:
    void startBatch(size_t done);

    size_t findKeys(size_t k)
    {
        return find_keys_fns[k != 0](
            {*tables[k],
             added_columns.join_on_keys[k],
             rowsToProbe(k),
             added_columns.enable_prefetch,
             batch_first_row,
             matchWordsOf(k),
             functions.records_key_offsets ? key_offsets.data() : nullptr,
             max_joined_rows - std::min<size_t>(added_rows, max_joined_rows)});
    }

    /// For a join that stops at the first match: removes the rows that disjunct `k` matched from `unmatched_rows`. The match
    /// step never reads the match words of a later disjunct for a row that an earlier disjunct matched, so those words need
    /// no zeroing.
    void keepUnmatchedRows(size_t k);

    /// What `run` calls: [later disjunct].
    FindKeysFn find_keys_fns[2]{};
    AddMatchesFn add_matches_fn = nullptr;

    PaddedPODArray<MatchWord> disjunct_match_words;
    PaddedPODArray<UInt32> unmatched_rows;
};

/// Picks the functions of a join for one block. Per block, it always sees the current key type and strictness: both may change
/// after the build filled the tables. Instantiated with the methods of each join kind in its own file, such as `InnerHashJoinAll.cpp`.
template <JoinKind KIND, JoinStrictness STRICTNESS, typename Maps>
JoinRightColumns::Functions selectFunctions(HashJoin::Type key_type, size_t num_disjuncts, bool has_additional_filter);

}
