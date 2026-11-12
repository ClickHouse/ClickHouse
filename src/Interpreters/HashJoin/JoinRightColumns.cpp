#include <Interpreters/HashJoin/JoinRightColumns.h>

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
    /// The probe always runs on a whole block as one continuous range; only the build scatters rows by index.
    if (!selector.isContinuousRange())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected hash join probe of a scattered block");
    return selector.getRange().first;
}

}

JoinRightColumns::JoinRightColumns(
    const Functions & functions_,
    Tables tables_,
    AddedColumns & added_columns_,
    JoinStuff::JoinUsedFlags & used_flags_,
    const ScatteredBlock::Selector & selector)
    : functions(functions_)
    , tables(tables_)
    , added_columns(added_columns_)
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
        added_columns.join_on_keys,
        [](const auto & keys) { return !keys.null_map && keys.join_mask_column.getKind() == JoinCommon::JoinMask::Kind::AllTrue; });
    const bool need_filter = added_columns.need_filter;

    for (const size_t robin_hood : {0, 1})
        for (const size_t later : {0, 1})
            find_keys_ring_fns[robin_hood][later] = functions.find_keys_ring[robin_hood][later][fast_path];
    for (const size_t later : {0, 1})
        prepare_keys_fns[later] = functions.prepare_keys[later][fast_path];

    if (functions.words_in_row_refs)
    {
        /// The batch is the whole block, and `add_matches_fn` runs the key search itself, on search batches or fused.
        search_batch_find_keys_fn = functions.find_keys[0][fast_path];
        add_matches_fn = search_batch_find_keys_fn ? functions.add_matches[need_filter] : functions.fused[need_filter][fast_path];
        const bool ring_searches = ringTakes(0, rows);
        if (ring_searches)
        {
            search_batch_find_keys_fn = ringFn(0);
            search_batch_prepare_keys_fn = prepare_keys_fns[0];
            add_matches_fn = functions.ring_batches[need_filter];
        }
        if (search_batch_find_keys_fn)
        {
            /// Search batches of about equal size. A short last one would pay the cost of starting a key search for only a few
            /// rows: `PrefetchingLookup` spends its first rows measuring how far ahead to prefetch, and the ring has to fill
            /// and drain.
            const size_t num_search_batches = (rows + max_search_batch_rows - 1) / max_search_batch_rows;
            search_batch_rows = num_search_batches ? (rows + num_search_batches - 1) / num_search_batches : 0;
            if (functions.records_key_offsets)
            {
                /// The prefetches of the last rows of a search batch read the key offsets past it.
                if (ring_searches)
                    key_offsets.resize_fill(search_batch_rows + amac_flag_prefetch_rows, 0);
                else
                    key_offsets.resize(search_batch_rows);
            }
        }
    }
    else
    {
        find_keys_fns[0] = functions.find_keys[0][fast_path];
        find_keys_fns[1] = functions.find_keys[1][fast_path];
        add_matches_fn = functions.add_matches[need_filter];
        if (functions.records_key_offsets)
            key_offsets.resize(max_batch_rows);
        disjunct_match_words.resize(tables.size() * max_batch_rows);
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
        auto & row_refs = added_columns.lazy_output.row_refs;
        row_refs_base = row_refs.size();
        row_refs.resize(row_refs_base + batch_rows);
        match_words = row_refs.data() + row_refs_base;
    }
    else
        match_words = disjunct_match_words.data();
}

void JoinRightColumns::keepUnmatchedRows(size_t k)
{
    if (!functions.stops_at_first_match || k + 1 == tables.size())
        return;

    const MatchWord * words = matchWordsOf(k);
    size_t kept = 0;
    if (k == 0)
    {
        chassert(batch_first_row + batch_rows <= std::numeric_limits<UInt32>::max());
        unmatched_rows.resize(batch_rows);
        for (size_t j = 0; j < batch_rows; ++j)
        {
            unmatched_rows[kept] = static_cast<UInt32>(batch_first_row + j);
            kept += words[j] == 0;
        }
    }
    else
    {
        for (const UInt32 row : unmatched_rows)
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
        return {.first = 0, .rows = unmatched_rows.data(), .size = unmatched_rows.size()};
    return {.first = batch_first_row, .rows = nullptr, .size = batch_rows};
}

}

}
