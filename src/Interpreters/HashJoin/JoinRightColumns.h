#pragma once

#include <Interpreters/HashJoin/FindKeys.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Common/PODArray.h>

#include <memory>
#include <span>

namespace DB::JoinProbe
{

/** The probe of a hash join runs in two steps per batch of left rows:
  *  - Finding keys looks up the key of every row in every ON disjunct and records what it found. It depends on the key type
  *    and the map, never on the join kind. It only looks up: it sets no used flag and picks no ASOF row.
  *  - Adding matches does everything that the join kind does with what was found, the used flags included. It depends on the
  *    join kind, never on the key type.
  * `JoinRightColumns` calls both through function pointers that `selectFunctions` picks for each block. Where two steps are
  * slower, one fused function per key type and join kind does both in one loop. The key search is in `FindKeys.h`; adding
  * matches and `selectFunctions` are in `JoinRightColumnsImpl.h`.
  */

class JoinRightColumns;

/// Adds the matches of the current batch to the result and returns the rows of the batch it finished. A fused function has
/// this type too, and finds the keys itself.
using AddMatchesFn = size_t (*)(JoinRightColumns &);

/// The probe of one left block. Built per block, because several threads probe one `HashJoin` at once.
class JoinRightColumns
{
public:
    using Tables = std::span<const std::unique_ptr<HashJoinTable>>;

    struct Functions
    {
        FindKeysFn find_keys[2][2]{}; /// [later disjunct][fast_path]; empty for a fused probe
        AddMatchesFn add_matches[2]{}; /// [need_filter]
        AddMatchesFn fused[2][2]{}; /// [need_filter][fast_path]; set instead of the two above where splitting is slower
        /// 0 for the whole block. Otherwise the row limit applies: the first batch of a call has at most as many rows as the
        /// limit, and each next one twice as many, so a call that stops early at the limit wastes few lookups.
        size_t max_batch_rows = 0;
        /// One disjunct without an additional filter keeps its match words at the end of the row refs.
        bool words_in_row_refs = false;
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

    /// Returns the rows it finished: fewer than the block when the row limit stopped it. An empty block still runs one empty
    /// batch, so that its result is set up as for any other block.
    size_t run()
    {
        size_t done = 0;
        do
        {
            startBatch(done);
            for (size_t k = 0; k < tables.size() && find_keys_fns[0]; ++k)
            {
                /// The first disjunct may stop at the row limit. The later ones look up only the rows it reached.
                const size_t visited = findKeys(k);
                if (k == 0)
                    batch_rows = visited;
                keepUnmatchedRows(k);
            }
            /// All disjuncts first: the output of a left row must be contiguous and in row order, and a right row that two
            /// disjuncts reach is added once.
            const size_t finished = add_matches_fn(*this);
            done += finished;
            if (finished < batch_rows)
                break;
        } while (done < rows);
        return done;
    }

    MatchWord * matchWordsOf(size_t k) { return match_words + k * batch_rows; }

    /// The rows of the batch, or for a join that stops at the first match, the rows that no disjunct before `k` matched.
    ProbeRows rowsToProbe(size_t k) const;

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
    /// With `words_in_row_refs`: where the match words start in the row refs.
    size_t row_refs_base = 0;
    /// With `records_key_offsets`: the key cell of each row of the batch.
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
             max_joined_rows - std::min(added_rows, max_joined_rows)});
    }

    /// For a join that stops at the first match: removes the rows that disjunct `k` matched from `unmatched_rows`. A row that
    /// an earlier disjunct matched never reads the match words of the later ones, so those need no zeroing.
    void keepUnmatchedRows(size_t k);

    FindKeysFn find_keys_fns[2]{};
    AddMatchesFn add_matches_fn = nullptr;

    PaddedPODArray<MatchWord> disjunct_match_words;
    PaddedPODArray<UInt32> unmatched_rows;
};

/// Picks the functions of a join for one block. Per block, it always sees the current key type and strictness: both may change
/// after the build filled the tables. Instantiated with the methods of each join kind in its own file, such as `InnerHashJoinAll.cpp`.
template <JoinKind KIND, JoinStrictness STRICTNESS, typename Maps>
JoinRightColumns::Functions
selectFunctions(HashJoin::Type key_type, size_t num_disjuncts, bool has_additional_filter, bool record_row_refs);

}
