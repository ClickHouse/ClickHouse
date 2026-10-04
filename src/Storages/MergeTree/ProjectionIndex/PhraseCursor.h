#pragma once

#include <Storages/MergeTree/ProjectionIndex/PositionCursor.h>
#include <Storages/MergeTree/ProjectionIndex/PostingCursor.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListCursor.h>

#include <vector>

namespace DB
{

/// Exact phrase query cursor using lazy position data.
///
/// For each doc in the AND intersection of all phrase tokens,
/// verifies that token positions form a consecutive sequence.
///
/// Uses token_cursors (ProjectionPostingListCursor with seek/next/value/valid)
/// for AND intersection via galloping, and PositionCursor for
/// on-demand per-doc position decode — no upfront bulk decode.
class PhraseCursor final : public IPostingCursor
{
public:
    PhraseCursor(std::vector<ProjectionPostingListCursorPtr> token_cursors_, std::vector<PositionCursorPtr> pos_cursors_);

    void fill(UInt8 * out, size_t row_offset, size_t num_rows) override;

private:
    bool checkPhrase(UInt32 doc_id);

    std::vector<ProjectionPostingListCursorPtr> token_cursors;
    std::vector<PositionCursorPtr> pos_cursors;
};

}
