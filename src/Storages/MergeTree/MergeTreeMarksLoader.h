#include <cstddef>
#include <memory>
#include <Disks/IDisk.h>
#include <Storages/MarkCache.h>
#include <DataStreams/MarkInCompressedFile.h>

namespace DB
{
struct MergeTreeIndexGranularityInfo;

class MergeTreeMarksLoader
{
public:
    using MarksPtr = MarkCache::ValuePtr;

    MergeTreeMarksLoader(
        DiskPtr disk_,
        MarkCache * mark_cache_,
        const String & mrk_path_,
        size_t marks_count_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        bool save_new_marks_in_cache_,
        size_t columns_in_mark_ = 1)
    : disk(std::move(disk_))
    , mark_cache(mark_cache_)
    , mrk_path(mrk_path_)
    , marks_count(marks_count_)
    , index_granularity_info(index_granularity_info_)
    , save_new_marks_in_cache(save_new_marks_in_cache_)
    , columns_in_mark(columns_in_mark_) {}

    const inline MarkInCompressedFile & getMark(size_t row_index, size_t column_index = 0)
    {
        if (cached_marks() ? marks_cache == nullptr : marks_non_cache == nullptr)
            loadMarks();

#ifndef NDEBUG
        if (column_index >= columns_in_mark)
            throw Exception("Column index: " + toString(column_index)
                + " is out of range [0, " + toString(columns_in_mark) + ")",
                ErrorCodes::LOGICAL_ERROR);
#endif

        if (cached_marks())
            return (*marks_cache)[row_index * columns_in_mark + column_index];
        else
            return (*marks_non_cache)[row_index * columns_in_mark + column_index];
    }

    bool initialized() const noexcept {
        return cached_marks() ? marks_cache != nullptr : marks_non_cache != nullptr;
    }

    constexpr bool cached_marks() const noexcept
    {
        return mark_cache && marks_cache != nullptr;
    }

private:
    DiskPtr disk;
    MarkCache * mark_cache = nullptr;
    String mrk_path;

    size_t marks_count;

    const MergeTreeIndexGranularityInfo & index_granularity_info;

    bool save_new_marks_in_cache;
    size_t columns_in_mark;

    /// Contains something if the cache is active and contains a value for given key.
    MarkCache::ValuePtr marks_cache;

    /// Contains something if the cache is disabled, or the value for the key was not found.
    std::unique_ptr<MarksInCompressedFile> marks_non_cache;

    void loadMarks();
};
}

