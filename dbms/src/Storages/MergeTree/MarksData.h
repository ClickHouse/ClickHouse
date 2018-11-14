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

    MarksData() = default;

    const MarkInCompressedFile & getMark(size_t index) const;
    size_t getNumRowsBetweenMarks(size_t mark_from, size_t mark_to) const;
    const size_t & getMarksCount() const
    {
        return marks_count;
    }

private:
    void loadMarks() const;

private:
    std::string path = "";
    size_t one_mark_bytes_size = 0;
    size_t marks_count = 0;
    size_t storage_index_granularity = 0;
    bool save_marks_in_cache = false;

    MarkCachePtr mark_cache = nullptr;
    mutable MarkCache::MappedPtr marks = nullptr;
};
}
