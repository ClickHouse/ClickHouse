#pragma once
#include <Disks/IDisk.h>
#include <Storages/MarkCache.h>

namespace DB
{

struct MergeTreeIndexGranularityInfo;

class MergeTreeMarksLoader
{
public:
    MergeTreeMarksLoader(
        DiskPtr disk_,
        MarkCache * mark_cache_,
        const String & mrk_path,
        size_t marks_count_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        bool save_marks_in_cache_,
        size_t columns_in_mark_ = 1);

    const MarkInCompressedFile & getMark(size_t row_index, size_t column_index = 0);

    bool initialized() const { return is_initialized; }

private:
    DiskPtr disk;
    MarkCache * mark_cache = nullptr;
    String mrk_path;
    size_t marks_count;
    const MergeTreeIndexGranularityInfo & index_granularity_info;
    bool save_marks_in_cache = false;
    bool is_initialized = false;
    size_t columns_in_mark;
    MarkCache::HolderPtr marks_cache;
    MarksInCompressedFile marks_non_cache;

    void loadMarks();
    void readIntoMarks(MarksInCompressedFile& marks_to_load);
};

}
