#include <Storages/MergeTree/ProjectionIndex/PhraseCursor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

PhraseCursor::PhraseCursor(std::vector<ProjectionPostingListCursorPtr> token_cursors_, std::vector<PositionCursorPtr> pos_cursors_)
    : token_cursors(std::move(token_cursors_))
    , pos_cursors(std::move(pos_cursors_))
{
}

void PhraseCursor::fill(UInt8 * out, size_t row_offset, size_t num_rows)
{
    if (token_cursors.empty() || pos_cursors.empty())
        return;

    if (token_cursors.size() == 1)
    {
        token_cursors[0]->fill(out, row_offset, num_rows);
        return;
    }

    memset(out, 0, num_rows);

    size_t n = token_cursors.size();
    auto row_begin = static_cast<uint32_t>(row_offset);
    auto row_end = static_cast<uint32_t>(row_offset + num_rows);

    /// Seek the lead cursor to row_begin
    token_cursors[0]->seek(row_begin);

    while (token_cursors[0]->valid() && token_cursors[0]->value() < row_end)
    {
        uint32_t candidate = token_cursors[0]->value();

        /// AND check: seek all other cursors to candidate
        bool all_match = true;
        for (size_t t = 1; t < n; ++t)
        {
            token_cursors[t]->seek(candidate);

            if (!token_cursors[t]->valid() || token_cursors[t]->value() != candidate)
            {
                all_match = false;

                /// Galloping: if this cursor jumped ahead, skip lead cursor forward
                if (token_cursors[t]->valid())
                {
                    uint32_t skip_to = token_cursors[t]->value();
                    token_cursors[0]->seek(skip_to);
                }
                else
                {
                    /// This cursor is exhausted — no more matches possible
                    return;
                }
                break;
            }
        }

        if (all_match)
        {
            if (checkPhrase(candidate))
                out[candidate - row_begin] = 1;

            token_cursors[0]->next();
        }
    }
}

bool PhraseCursor::checkPhrase(UInt32 doc_id)
{
    size_t n = pos_cursors.size();

    UInt32 freq0 = pos_cursors[0]->seekDoc(doc_id);
    if (freq0 == 0)
        return false;

    for (size_t t = 1; t < n; ++t)
    {
        if (pos_cursors[t]->seekDoc(doc_id) == 0)
            return false;
    }

    /// Each token's current position
    UInt32 inline_pos[8] = {};  // NOLINT(clang-analyzer-core.uninitialized.Assign)
    std::vector<UInt32> heap_pos;
    UInt32 * cur_pos = nullptr;

    if (n <= 8)
        cur_pos = inline_pos;
    else
    {
        heap_pos.resize(n);
        cur_pos = heap_pos.data();
    }

    for (size_t t = 0; t < n; ++t)
    {
        cur_pos[t] = pos_cursors[t]->nextPosition();
        if (cur_pos[t] == PositionCursor::NO_MORE_POSITIONS)
            return false;
    }

    /// Stream-based phrase matching: token 0 drives, others chase.
    for (;;)
    {
        UInt32 base_pos = cur_pos[0];
        bool match = true;

        for (size_t t = 1; t < n && match; ++t)
        {
            /// Compute `base_pos + t` in UInt64 to detect wrap-around: a corrupted `.pos`
            /// stream can drive `base_pos` close to UINT32_MAX, and then `base_pos + t` in
            /// UInt32 would wrap to a small value and silently mis-match against a real
            /// position in another token. Treat any overflow (including reaching the
            /// `NO_MORE_POSITIONS` sentinel) as `INCORRECT_DATA` so phrase queries fail
            /// closed instead of returning wrong rows.
            const UInt64 expected64 = static_cast<UInt64>(base_pos) + t;
            if (expected64 >= PositionCursor::NO_MORE_POSITIONS) [[unlikely]]
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Corrupted projection text index: phrase position {} + {} overflows UInt32",
                    base_pos, t);
            const UInt32 expected = static_cast<UInt32>(expected64);

            while (cur_pos[t] < expected)
            {
                cur_pos[t] = pos_cursors[t]->nextPosition();
                if (cur_pos[t] == PositionCursor::NO_MORE_POSITIONS)
                    return false;
            }

            if (cur_pos[t] != expected)
                match = false;
        }

        if (match)
            return true;

        cur_pos[0] = pos_cursors[0]->nextPosition();
        if (cur_pos[0] == PositionCursor::NO_MORE_POSITIONS)
            return false;
    }
}

}
