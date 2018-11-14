#pragma once
#include <DataStreams/MarkInCompressedFile.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{
class MarksData
{
public:
    MarksData(const std::string & path_prefix,
        const MergeTreeData & storage,
        size_t marks_count_ = 0,
        bool save_marks_in_cache_ = true,
        bool lazy_load = true);

    const MarkInCompressedFile & getMark(size_t index);
    const size_t & getMarksCount() const
    {
        return marks_count;
    }

private:
    void loadMarks();

private:
    const std::string path;
    const size_t one_mark_bytes_size;
    const size_t marks_count;
    const size_t storage_index_granularity;
    const bool save_marks_in_cache;

    MarkCachePtr mark_cache;
    MarkCache::MappedPtr marks;
};
}
