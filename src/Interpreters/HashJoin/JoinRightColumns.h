#pragma once

#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/AmacRing.h>
#include <Interpreters/HashJoin/HashJoinTable.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/LookupMethod.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Common/PODArray.h>

#include <memory>
#include <span>

namespace DB::JoinProbe
{

/** The probe of a hash join, through function pointers that `selectFunctions` picks for each block:
  *  - With one disjunct and no additional filter, one pass finds the key of each left row and adds its output right away.
  *    It searches the block in search batches (`max_search_batch_rows`). The pass depends on the key type and on the match
  *    handling (`JoinProbe::MatchHandling`): what the join kind does with a row whose key was found and with one whose key
  *    was not. Join kinds with the same handling share one pass per key type.
  *  - With several disjuncts or an additional filter, each batch of left rows takes two steps. The key search looks up the
  *    keys of the rows in each ON disjunct and records what it found. It depends on the key type and the map, never on the
  *    join kind. The match step then does what the join kind does with what was found, the used flags included. A call of
  *    `run` may stop at the row limit (`max_joined_block_size_rows`, counted by `added_rows`); the next call goes on with the
  *    rest of the block.
  * The key search is in `FindKeys.h`, and what a lookup method implements is in `LookupMethod.h`. The pass, the match steps
  * and `selectFunctions` are in `JoinRightColumnsImpl.h`.
  * The AMAC ring of `AmacLookup.h` does the key search of a disjunct whose table passes `ringTakes`, and in the pass of one
  * disjunct it searches the keys in row order. The ring does not build keys.
  * For a key type whose key is built from the key columns, a prepare step (`PrepareKeys.h`) builds the keys of the rows
  * before the ring searches them.
  */

class JoinRightColumns;

/// With one disjunct and no additional filter, the pass searches a block in search batches: consecutive ranges of at most
/// this many rows, of about equal size. The lookup method sets up each search batch: it builds the skip bytes of its rows,
/// and for a prepared key getter the first chunk of their keys (`PreparedJoinKeys::prepareRange`). It also creates a
/// prefetcher, which measures its look-ahead on the first rows and prefetches no row past the batch. A short batch would pay
/// all of this for a few rows, so the batches of a block have about equal size.
constexpr size_t max_search_batch_rows = 4096;

/// With several disjuncts, the maximum number of rows in a batch of `JoinRightColumns::run` (see `Functions::max_batch_rows`).
/// The probe time stays flat for batches of 256 to 65536 rows, so this value needs no tuning.
constexpr size_t max_disjunct_batch_rows = 8192;

/// Adds the matches of the current batch to the result and returns how many rows of the batch it finished. The pass of a join
/// with one disjunct has this type too, and finds the keys itself.
using AddMatchesFn = size_t (*)(JoinRightColumns &);

/// The probe of one left block. Built per block, because several threads probe one `HashJoin` at once.
class JoinRightColumns
{
public:
    using Tables = std::span<const std::unique_ptr<HashJoinTable>>;

    struct Functions
    {
        /// Several disjuncts or an additional filter: the key search, [later disjunct][fast_path].
        FindKeysFn find_keys[2][2]{};
        /// The same searches by the AMAC ring (`AmacLookup.h`), [buckets in the Robin Hood mode][later disjunct][fast_path].
        /// Null where the ring has none.
        FindKeysFn find_keys_ring[2][2][2]{};
        /// The prepare step before a search by the ring (`PrepareKeys.h`), [later disjunct][fast_path]. Null for a key type
        /// whose key is read from the key column.
        PrepareKeysFn prepare_keys[2][2]{};
        /// Several disjuncts or an additional filter: the match step, [need_filter].
        AddMatchesFn add_matches[2]{};
        /// One disjunct and no additional filter: the pass that finds the keys and adds the matches, [fast_path].
        AddMatchesFn find_keys_and_add_matches[2]{};
        /// The same pass with the AMAC ring in row order, for a block that the ring takes, [buckets in the Robin Hood
        /// mode][fast_path]. Null where the ring has none.
        AddMatchesFn find_keys_and_add_matches_ring[2][2]{};
        /// The most rows of a batch, or 0 for a single batch of the whole block. With a value other than 0, the row limit
        /// applies: the first batch of a call has at most as many rows as the limit, and each later batch twice as many as the
        /// one before, up to this value. So a call that stops early at the limit wastes few lookups.
        size_t max_batch_rows = 0;
        /// One disjunct and no additional filter: the batch is the whole block. The pass writes its row refs in place, into
        /// `lazy_output.row_refs`, which starts empty.
        bool words_in_row_refs = false;
        /// For a join with an additional filter and one disjunct that sets a used flag per key: the flag is indexed by the
        /// key's cell offset. A match word does not carry that offset.
        bool records_key_offsets = false;
        /// Several disjuncts: a later disjunct looks up only the rows that no earlier disjunct matched.
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
            /// With `words_in_row_refs`, `add_matches_fn` finds the keys itself.
            if (!functions.words_in_row_refs)
            {
                for (size_t k = 0; k < tables.size(); ++k)
                {
                    /// The first disjunct may stop at the row limit. The later ones look up only the rows it reached.
                    const size_t visited = findKeys(k);
                    if (k == 0)
                        batch_rows = visited;
                    keepUnmatchedRows(k);
                }
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

    /// Before a search of the first disjunct by the ring: the prepare step of the rows of `args`, if the key type builds its
    /// keys.
    void prepareKeysOfFirstDisjunct(const FindKeysArgs & args) const
    {
        if (prepare_keys_fns[0])
            prepare_keys_fns[0](args);
    }

    const Functions & functions;
    const Tables tables;
    AddedColumns & added_columns;
    JoinStuff::JoinUsedFlags & used_flags;
    /// The block rows `[first_row, first_row + rows)`.
    const size_t first_row;
    const size_t rows;
    const size_t max_joined_rows;

    /// The current batch: the block rows `[batch_first_row, batch_first_row + batch_rows)`.
    size_t batch_first_row = 0;
    size_t batch_rows = 0;
    /// `[k * batch_rows + j]` is the match word of disjunct `k` for row `batch_first_row + j`. With `words_in_row_refs`, it
    /// holds the row refs of the batch instead. The pass writes them in row order.
    MatchWord * match_words = nullptr;
    /// With `records_key_offsets`: for each row of the batch, the cell offset of its key.
    PODArray<size_t> key_offsets;
    /// What the row limit counts so far in this call: output rows, or with an additional filter, the pairs that match on the keys.
    IColumn::Offset added_rows = 0;
    /// With `words_in_row_refs`: the number of rows of every search batch but the last, which may have fewer.
    size_t search_batch_rows = 0;

private:
    const size_t max_batch_rows;
    size_t next_batch_rows;

    void startBatch(size_t done);

    /// The rows of the batch, or for a join that stops at the first match, the rows that no disjunct before `k` matched.
    [[nodiscard]] ProbeRows rowsToProbe(size_t k) const;

    /// The key search of disjunct `k` by the AMAC ring, for the mode of the buckets of its table. Null where the ring has none.
    [[nodiscard]] FindKeysFn ringFn(size_t k) const { return find_keys_ring_fns[tables[k]->bucketsAreRobinHood()][k != 0]; }

    /// Whether the ring may search `probe_rows` rows of disjunct `k`.
    [[nodiscard]] bool ringTakes(size_t k, size_t probe_rows) const
    {
        return probe_rows >= amac_min_rows && added_columns.enable_prefetch && tables[k]->takesAmacLookup();
    }

    size_t findKeys(size_t k)
    {
        const ProbeRows probe_rows = rowsToProbe(k);
        const FindKeysArgs args{
            *tables[k],
            added_columns.join_on_keys[k],
            probe_rows,
            added_columns.enable_prefetch,
            batch_first_row,
            matchWordsOf(k),
            functions.records_key_offsets ? key_offsets.data() : nullptr,
            max_joined_rows - std::min<size_t>(added_rows, max_joined_rows)};
        const FindKeysFn ring = ringTakes(k, probe_rows.size) ? ringFn(k) : nullptr;
        if (!ring)
            return find_keys_fns[k != 0](args);
        if (const PrepareKeysFn prepare_keys = prepare_keys_fns[k != 0])
            prepare_keys(args);
        return ring(args);
    }

    /// For a join that stops at the first match: removes the rows that disjunct `k` matched from `unmatched_rows`. The match
    /// step never reads the match words of a later disjunct for a row that an earlier disjunct matched, so those words need
    /// no zeroing.
    void keepUnmatchedRows(size_t k);

    /// What `run` calls: [later disjunct]. Unused with `words_in_row_refs`.
    FindKeysFn find_keys_fns[2]{};
    FindKeysFn find_keys_ring_fns[2][2]{}; /// [buckets in the Robin Hood mode][later disjunct]
    PrepareKeysFn prepare_keys_fns[2]{}; /// [later disjunct]
    AddMatchesFn add_matches_fn = nullptr;

    PaddedPODArray<MatchWord> disjunct_match_words;
    PaddedPODArray<UInt64> unmatched_rows;
};

/// Picks the functions of a join for one block. Per block, it always sees the current key type and strictness: both may change
/// after the build filled the tables. Instantiated with the methods of each join kind in its own file, such as `InnerHashJoinAll.cpp`.
template <JoinKind KIND, JoinStrictness STRICTNESS, typename Maps>
JoinRightColumns::Functions
selectFunctions(HashJoin::Type key_type, size_t num_disjuncts, bool has_additional_filter);

}
