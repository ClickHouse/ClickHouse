#pragma once

#include <Storages/MergeTree/MergeTreeReaderTextIndex.h>
#include <Storages/MergeTree/ProjectionIndex/PostingCursor.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListCursor.h>

#include <absl/container/flat_hash_set.h>

namespace DB
{

struct MergeTreeProjectionIndexGranuleText;

class MergeTreeReaderTextProjectionIndex : public MergeTreeReaderTextIndex
{
public:
    MergeTreeReaderTextProjectionIndex(
        const IMergeTreeReader * main_reader_,
        IndexReadTask index_read_task_,
        NamesAndTypesList columns_,
        MergeTreeIndexGranulePtr index_granule_);

    size_t readRows(
        size_t from_mark,
        size_t current_range_last_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        MutableColumns & res_columns, Columns & shared_columns) override;

    /// Unlike the base class, `readRows` here walks whole marks: a read cannot start or stop inside a mark.
    bool canReadIncompleteGranules() const override { return false; }

    void setPrecomputedGranule(const IndexGranulesMap & granules) override;
    void setIndexGranule(MergeTreeIndexGranulePtr index_granule) override;

private:
    const TextSearchQueryPtr & getSearchQuery(const String & column_name) const;

    PostingListPtr readPostingsBlockForToken(
        std::string_view token, const ProjectionTokenInfo & token_info, size_t block_idx, const String & index_id);

    void projectionReadGranule();
    void projectionInitializePostingStreams() { }

    LargePostingListReaderStreamPtr createIndependentStream(const char * suffix, bool required) const;
    LargePostingListReaderStreamPtr createIndependentPostingStream() const;
    LargePostingListReaderStreamPtr createIndependentPositionStream() const;
    LargePostingListReaderStreamPtr createIndependentIndexStream() const;

    PostingCursorPtr & getOrBuildCursor(const String & column_name);

    void ensureInitialized();
    void ensureCursorMap();

    using CursorMap = absl::flat_hash_map<std::string_view, PostingCursorPtr>;
    CursorMap buildCursorMap();

    /// Builds a cursor over the posting list of one token, or returns nullptr when the token has no postings.
    PostingCursorPtr buildTokenCursor(
        MergeTreeProjectionIndexGranuleText & granule_text, const String & token, const ProjectionTokenInfo & token_info, DecodedBlockCache * block_cache);

    void fillColumnLazy(IColumn & column, const String & column_name, size_t column_offset, size_t row_offset, size_t num_rows);

    void fillBatch(MutableColumns & res_columns, size_t from_row, size_t batch_rows);

    MergeTreeIndexGranulePtr projection_granule;

    CursorMap cursor_map;
    bool cursor_map_built = false;
    /// The tokens whose cursor from `cursor_map` is already used by the cursor of a virtual column. A cursor
    /// has a mutable position, so another virtual column with the same token gets a cursor of its own:
    /// otherwise filling one column would advance the cursor past the rows the other one has yet to see.
    absl::flat_hash_set<std::string_view> claimed_tokens;
    absl::flat_hash_map<String, PostingCursorPtr> column_cursors;
};

}
