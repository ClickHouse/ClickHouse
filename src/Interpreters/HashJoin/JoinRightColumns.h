#pragma once

#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoinTable.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/LookupMethod.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>

#include <memory>
#include <span>

namespace DB::JoinProbe
{

/// With one disjunct and no residual filter, the lookup handles each match. AMAC can write output
/// in completion order or leave one word per left row to build the ordered gather inputs.
/// With multiple disjuncts or a residual filter, each disjunct records its hits first. A separate
/// match step checks the full condition, updates used flags and enforces the output row limit.
struct JoinRightColumnsData;

/// With one disjunct and no additional filter, the pass searches a block in search batches: consecutive ranges of at most
/// this many rows, of about equal size. The lookup method sets up each search batch: for a prepared key getter it builds
/// the first chunk of the batch's keys (`PreparedJoinKeys::prepareRange`), and it creates a prefetcher, which measures its
/// look-ahead on the first rows and prefetches no row past the batch. A short batch would pay all of this for a few rows,
/// so the batches of a block have about equal size.
constexpr size_t max_search_batch_rows = 4096;

/// With several disjuncts, the maximum number of rows in a batch of `runProbe` (see `Functions::max_batch_rows`).
/// The probe time stays flat for batches of 256 to 65536 rows, so this value needs no tuning.
constexpr size_t max_disjunct_batch_rows = 8192;

/// Adds the matches of the current batch to the result and returns how many rows of the batch it finished. The pass of a join
/// with one disjunct has this type too, and finds the keys itself.
using AddMatchesFn = size_t (*)(JoinRightColumnsData &);

AddMatchesFn selectAmacPostPass(bool groups, bool add_missing, bool keeps_unmatched);
size_t compactOrderedAmacRows(JoinRightColumnsData & data);

using Tables = std::span<const std::unique_ptr<HashJoinTable>>;

/// The functions of a join for one block, from `selectFunctions`.
struct Functions
{
    /// Several disjuncts or an additional filter: the key search of the first disjunct and of the later ones.
    FindKeysFn find_keys_first = nullptr;
    FindKeysFn find_keys_later = nullptr;
    /// The same searches by the AMAC ring (`AmacLookup.h`), [buckets in the Robin Hood mode]. Null where the ring has none.
    FindKeysFn find_keys_ring_first[2]{};
    FindKeysFn find_keys_ring_later[2]{};
    /// The prepare step before a search by the ring (`PrepareKeys.h`), of the first disjunct and of the later ones. Null for
    /// a key type whose key is read from the key column.
    PrepareKeysFn prepare_keys_first = nullptr;
    PrepareKeysFn prepare_keys_later = nullptr;
    /// Several disjuncts or an additional filter: the match step. One disjunct and no additional filter: the pass that
    /// finds the keys and adds the matches.
    AddMatchesFn add_matches = nullptr;
    /// Consumes the indexed words of a single-disjunct AMAC lookup in left input order.
    AddMatchesFn amac_postpass = nullptr;
    /// The most rows of a batch, or 0 for a single batch of the whole block. With a value other than 0, the row limit
    /// applies: the first batch of a call has at most as many rows as the limit, and each later batch twice as many as the
    /// one before, up to this value. So a call that stops early at the limit wastes few lookups.
    size_t max_batch_rows = 0;
    bool uses_amac = false;
    /// For a join with an additional filter and one disjunct that sets a used flag per key: the flag is indexed by the
    /// key's cell offset. A match word does not carry that offset.
    bool records_key_offsets = false;
    /// Several disjuncts: a later disjunct looks up only the rows that no earlier disjunct matched.
    bool stops_at_first_match = false;
};

/// What the functions of one call of `runProbe` share: the inputs of the call and its current batch. `runProbe` sets the
/// batch, and the key search and the match step read it. It lives for one call because several threads probe one
/// `HashJoin` at once. What a block keeps or works in across its calls is in the block's `LeftBlockState`.
struct JoinRightColumnsData
{
    [[nodiscard]] MatchWord * matchWordsOf(size_t k) const { return left_block_state.match_words.data() + k * batch_rows; }

    const Functions & functions;
    const Tables tables;
    AddedColumns & added_columns;
    LeftBlockState & left_block_state;
    JoinStuff::JoinUsedFlags & used_flags;
    /// The block rows `[first_row, first_row + rows)`.
    const size_t first_row;
    const size_t rows;
    const size_t max_joined_rows;

    /// The current batch: the block rows `[batch_first_row, batch_first_row + batch_rows)`.
    size_t batch_first_row = 0;
    size_t batch_rows = 0;
    /// What the row limit counts so far in this call: output rows, or with an additional filter, the pairs that match on the keys.
    IColumn::Offset added_rows = 0;
    /// For a single-disjunct fused pass, the number of rows of every search batch but the last, which may have fewer.
    size_t search_batch_rows = 0;
};

/// Runs `functions` on the rows of `selector`, batch by batch. Returns how many rows of the block it finished: fewer than
/// all when the row limit stopped it, but at least one for a non-empty block. An empty block still runs one empty batch, so
/// that its result is set up as for any other block.
size_t runProbe(
    const Functions & functions,
    Tables tables,
    JoinStuff::JoinUsedFlags & used_flags,
    AddedColumns & added_columns,
    LeftBlockState & left_block_state,
    const ScatteredBlock::Selector & selector);

/// Whether no disjunct of the block has a nullable key or an ON-section condition that may filter out a row. This
/// overwhelmingly common case is compiled without the per-row null-map and join-mask checks.
bool canUseFastPath(const LeftBlockState & left_block_state);
}
