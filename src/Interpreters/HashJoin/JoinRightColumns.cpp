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

/// Makes the batch the `batch_rows` block rows after the first `done` rows of the call.
void startBatch(JoinRightColumnsData & data, size_t done, size_t batch_rows)
{
    data.batch_first_row = data.first_row + done;
    data.batch_rows = batch_rows;
    if (data.functions.words_in_row_refs)
    {
        /// The batch is the whole block, and the row refs of a block start empty.
        auto & row_refs = data.added_columns.lazy_output.row_refs;
        chassert(row_refs.empty());
        row_refs.resize(batch_rows);
        data.match_words = row_refs.data();
    }
    else
        data.match_words = data.left_block_state.match_words.data();
}

/// The rows of the batch, or for a join that stops at the first match, the rows that no disjunct before `k` matched.
ProbeRows rowsToProbe(const JoinRightColumnsData & data, size_t k)
{
    const auto & unmatched_rows = data.left_block_state.unmatched_rows;
    if (data.functions.stops_at_first_match && k > 0)
        return {.first = 0, .rows = unmatched_rows.data(), .size = unmatched_rows.size()};
    return {.first = data.batch_first_row, .rows = nullptr, .size = data.batch_rows};
}

size_t findKeysOfDisjunct(JoinRightColumnsData & data, size_t k)
{
    DisjunctState & disjunct = data.left_block_state.disjuncts[k];
    return (k == 0 ? data.functions.find_keys_first : data.functions.find_keys_later)(
        {.table = *data.tables[k],
         .keys = disjunct.columns,
         .disjunct = disjunct,
         .rows = rowsToProbe(data, k),
         .enable_prefetch = data.added_columns.enable_prefetch,
         .base = data.batch_first_row,
         .words = data.matchWordsOf(k),
         .key_offsets = data.functions.records_key_offsets ? data.left_block_state.key_offsets.data() : nullptr,
         .max_output_rows = data.max_joined_rows - std::min<size_t>(data.added_rows, data.max_joined_rows)});
}

/// For a join that stops at the first match: removes the rows that disjunct `k` matched from `unmatched_rows`. The match
/// step never reads the match words of a later disjunct for a row that an earlier disjunct matched, so those words need
/// no zeroing.
void keepUnmatchedRows(JoinRightColumnsData & data, size_t k)
{
    if (!data.functions.stops_at_first_match || k + 1 == data.tables.size())
        return;

    const MatchWord * words = data.matchWordsOf(k);
    auto & unmatched_rows = data.left_block_state.unmatched_rows;
    size_t kept = 0;
    if (k == 0)
    {
        unmatched_rows.resize(data.batch_rows);
        for (size_t j = 0; j < data.batch_rows; ++j)
        {
            unmatched_rows[kept] = data.batch_first_row + j;
            kept += words[j] == 0;
        }
    }
    else
    {
        for (const UInt64 row : unmatched_rows)
        {
            unmatched_rows[kept] = row;
            kept += words[row - data.batch_first_row] == 0;
        }
    }
    unmatched_rows.resize(kept);
}

}

size_t runProbe(
    const Functions & functions,
    Tables tables,
    JoinStuff::JoinUsedFlags & used_flags,
    AddedColumns & added_columns,
    LeftBlockState & left_block_state,
    const ScatteredBlock::Selector & selector)
{
    const size_t rows = selector.size();
    const size_t max_joined_rows
        = added_columns.max_joined_block_rows ? added_columns.max_joined_block_rows : std::numeric_limits<size_t>::max();
    JoinRightColumnsData data{
        .functions = functions,
        .tables = tables,
        .added_columns = added_columns,
        .left_block_state = left_block_state,
        .used_flags = used_flags,
        .first_row = firstRowOf(selector),
        .rows = rows,
        .max_joined_rows = max_joined_rows,
    };
    const size_t max_batch_rows = functions.max_batch_rows ? std::min(rows, functions.max_batch_rows) : rows;
    size_t next_batch_rows = functions.max_batch_rows ? std::min(max_batch_rows, max_joined_rows) : rows;

    if (functions.words_in_row_refs)
    {
        /// The batch is the whole block, and `add_matches` finds the keys itself, on search batches of about equal size.
        const size_t num_search_batches = (rows + max_search_batch_rows - 1) / max_search_batch_rows;
        data.search_batch_rows = num_search_batches ? (rows + num_search_batches - 1) / num_search_batches : 0;
    }
    else
    {
        /// A lookup method may have no key search for this key type.
        if (!functions.find_keys_first || !functions.find_keys_later)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No hash join key search for {} disjuncts", tables.size());
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
    if (!functions.add_matches)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected hash join of {} disjuncts with need_filter = {}",
            tables.size(),
            added_columns.need_filter);

    size_t done = 0;
    do
    {
        startBatch(data, done, std::min(next_batch_rows, rows - done));
        next_batch_rows = std::min(max_batch_rows, 2 * next_batch_rows);
        if (!functions.words_in_row_refs)
        {
            for (size_t k = 0; k < tables.size(); ++k)
            {
                /// The first disjunct may stop at the row limit. The later ones look up only the rows it reached.
                const size_t visited = findKeysOfDisjunct(data, k);
                if (k == 0)
                    data.batch_rows = visited;
                keepUnmatchedRows(data, k);
            }
        }
        /// The keys of every disjunct are found before any match is added: the output of a left row must be contiguous
        /// and in row order, and a right row that two disjuncts reach is added once.
        const size_t finished = functions.add_matches(data);
        done += finished;
        if (finished < data.batch_rows)
            break;
    } while (done < rows);
    return done;
}

bool canUseFastPath(const LeftBlockState & left_block_state)
{
    return std::ranges::all_of(
        left_block_state.disjuncts,
        [](const DisjunctState & disjunct)
        { return !disjunct.columns.null_map && disjunct.columns.join_mask_column.getKind() == JoinCommon::JoinMask::Kind::AllTrue; });
}

}

}
