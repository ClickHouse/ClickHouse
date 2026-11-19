#include <Interpreters/HashJoin/JoinRightColumns.h>
#include <base/MemorySanitizer.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace JoinProbe
{

namespace
{

size_t firstRowOf(const ScatteredBlock::Selector & selector)
{
    /// The probe always gets one continuous range of rows: a whole block, or the rest of one that the row limit cut. Only the
    /// build scatters rows by index.
    if (!selector.isContinuousRange())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected hash join probe of a scattered block");
    return selector.getRange().first;
}

}

JoinRightColumns::JoinRightColumns(
    const Functions & functions_,
    Tables tables_,
    AddedColumns & added_columns_,
    LeftBlockState & left_block_state_,
    JoinStuff::JoinUsedFlags & used_flags_,
    const ScatteredBlock::Selector & selector)
    : functions(functions_)
    , tables(tables_)
    , added_columns(added_columns_)
    , left_block_state(left_block_state_)
    , used_flags(used_flags_)
    , first_row(firstRowOf(selector))
    , rows(selector.size())
    , max_joined_rows(added_columns.max_joined_block_rows ? added_columns.max_joined_block_rows : std::numeric_limits<size_t>::max())
    , max_batch_rows(functions.max_batch_rows ? std::min(rows, functions.max_batch_rows) : rows)
    , next_batch_rows(functions.max_batch_rows ? std::min(max_batch_rows, max_joined_rows) : rows)
{
    /// The overwhelmingly common case - non-nullable keys and no condition in the ON section - is compiled without the
    /// per-row null-map and join-mask checks.
    const bool fast_path = std::ranges::all_of(
        left_block_state.disjuncts,
        [](const DisjunctState & disjunct)
        { return !disjunct.columns.null_map && disjunct.columns.join_mask_column.getKind() == JoinCommon::JoinMask::Kind::AllTrue; });
    const bool need_filter = added_columns.need_filter;

    if (functions.words_in_row_refs)
    {
        /// The batch is the whole block, and `add_matches_fn` finds the keys itself, on search batches of about equal size.
        add_matches_fn = functions.find_keys_and_add_matches[fast_path];
        const size_t num_search_batches = (rows + max_search_batch_rows - 1) / max_search_batch_rows;
        search_batch_rows = num_search_batches ? (rows + num_search_batches - 1) / num_search_batches : 0;
    }
    else
    {
        find_keys_fns[0] = functions.find_keys[0][fast_path];
        find_keys_fns[1] = functions.find_keys[1][fast_path];
        /// A lookup method may have no key search for this key type.
        if (!find_keys_fns[0] || !find_keys_fns[1])
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No hash join key search for {} disjuncts", tables.size());
        add_matches_fn = functions.add_matches[need_filter];
        /// The first call of a block has the most rows left, so it sizes these buffers for all the calls of the block.
        auto & key_offsets = left_block_state.key_offsets;
        if (functions.records_key_offsets)
        {
            key_offsets.resize(max_batch_rows);
            /// MemorySanitizer must still report a read of a word that this call did not write, as with a new buffer.
            __msan_poison(key_offsets.data(), key_offsets.size() * sizeof(key_offsets[0]));
        }
        auto & words = left_block_state.match_words;
        words.resize(tables.size() * max_batch_rows);
        __msan_poison(words.data(), words.size() * sizeof(words[0]));
    }
    if (!add_matches_fn)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Unexpected hash join of {} disjuncts with need_filter = {}", tables.size(), need_filter);
}

void JoinRightColumns::startBatch(size_t done)
{
    batch_first_row = first_row + done;
    batch_rows = std::min(next_batch_rows, rows - done);
    next_batch_rows = std::min(max_batch_rows, 2 * next_batch_rows);
    if (functions.words_in_row_refs)
    {
        /// The batch is the whole block, and the row refs of a block start empty.
        auto & row_refs = added_columns.lazy_output.row_refs;
        chassert(row_refs.empty());
        row_refs.resize(batch_rows);
        match_words = row_refs.data();
    }
    else
        match_words = left_block_state.match_words.data();
}

void JoinRightColumns::keepUnmatchedRows(size_t k)
{
    if (!functions.stops_at_first_match || k + 1 == tables.size())
        return;

    const MatchWord * words = matchWordsOf(k);
    auto & unmatched_rows = left_block_state.unmatched_rows;
    size_t kept = 0;
    if (k == 0)
    {
        unmatched_rows.resize(batch_rows);
        for (size_t j = 0; j < batch_rows; ++j)
        {
            unmatched_rows[kept] = batch_first_row + j;
            kept += words[j] == 0;
        }
    }
    else
    {
        for (const UInt64 row : unmatched_rows)
        {
            unmatched_rows[kept] = row;
            kept += words[row - batch_first_row] == 0;
        }
    }
    unmatched_rows.resize(kept);
}

ProbeRows JoinRightColumns::rowsToProbe(size_t k) const
{
    if (functions.stops_at_first_match && k > 0)
        return {.first = 0, .rows = left_block_state.unmatched_rows.data(), .size = left_block_state.unmatched_rows.size()};
    return {.first = batch_first_row, .rows = nullptr, .size = batch_rows};
}

}

}
