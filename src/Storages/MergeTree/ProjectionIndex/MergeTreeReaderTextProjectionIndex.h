#pragma once

#include <Storages/MergeTree/MergeTreeReaderTextIndex.h>
#include <Storages/MergeTree/ProjectionIndex/PostingCursor.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListCursor.h>

namespace DB
{

class MergeTreeReaderTextProjectionIndex : public MergeTreeReaderTextIndex
{
public:
    MergeTreeReaderTextProjectionIndex(
        const IMergeTreeReader * main_reader_,
        MergeTreeIndexWithCondition index_,
        NamesAndTypesList columns_,
        MergeTreeIndexGranulePtr index_granule_);

    size_t readRows(
        size_t from_mark,
        size_t current_task_last_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        size_t offset,
        Columns & res_columns) override;

    void setPrecomputedGranule(const IndexGranulesMap & granules) override;
    void setIndexGranule(MergeTreeIndexGranulePtr index_granule) override;

private:
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

    void fillColumnLazy(IColumn & column, const String & column_name, size_t column_offset, size_t row_offset, size_t num_rows);

    void fillBatch(Columns & res_columns, size_t from_row, size_t batch_rows);

    MergeTreeIndexGranulePtr projection_granule;

    CursorMap cursor_map;
    bool cursor_map_built = false;
    absl::flat_hash_map<String, PostingCursorPtr> column_cursors;
};

}
