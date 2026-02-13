#pragma once

#include <Storages/MergeTree/MergeTreeReaderTextIndex.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>

namespace DB
{

class MergeTreeReaderProjectionIndex : public MergeTreeReaderTextIndex
{
public:
    MergeTreeReaderProjectionIndex(
        const IMergeTreeReader * main_reader_, MergeTreeIndexWithCondition index_, NamesAndTypesList columns_, bool can_skip_mark_);

    void prefetchBeginOfRange(Priority /* priority */) override { }

    size_t readRows(
        size_t from_mark,
        size_t current_task_last_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        size_t offset,
        Columns & res_columns) override;

private:
    PostingListPtr
    readPostingsBlockForToken(std::string_view token, const TokenPostingsInfo & token_info, size_t block_idx, PostingListCodecPtr) override;

    /// Build PostingListCursorMap from remaining_tokens.
    PostingListCursorMap buildCursorMap();

    /// Ensure cursor map is built (called once, lazy).
    void ensureCursorMap();

    /// Fill column using lazy cursor-based intersection/union.
    void fillColumnLazy(
        IColumn & column,
        const String & column_name,
        size_t column_offset,
        size_t row_offset,
        size_t num_rows);

    /// Whether to use lazy posting list apply mode.
    bool use_lazy_mode = false;

    /// Lazily-built cursor map, shared across all marks in the part.
    PostingListCursorMap cursor_map;
    bool cursor_map_built = false;
};

}
