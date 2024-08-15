#pragma once

#include <Storages/MarkCache.h>
#include <IO/ReadSettings.h>
#include <Common/ThreadPool_fwd.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>


namespace DB
{

struct MergeTreeIndexGranularityInfo;
class Threadpool;

class MergeTreeMarksLoader
{
public:
    using MarksPtr = MarkCache::MappedPtr;

    MergeTreeMarksLoader(
        MergeTreeDataPartInfoForReaderPtr data_part_reader_,
        MarkCache * mark_cache_,
        const String & mrk_path,
        size_t marks_count_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        bool save_marks_in_cache_,
        const ReadSettings & read_settings_,
        ThreadPool * load_marks_threadpool_,
        size_t columns_in_mark_ = 1);

    ~MergeTreeMarksLoader();

    MarkInCompressedFile getMark(size_t row_index, size_t column_index = 0);

private:
    MergeTreeDataPartInfoForReaderPtr data_part_reader;
    MarkCache * mark_cache = nullptr;
    String mrk_path;
    size_t marks_count;
    const MergeTreeIndexGranularityInfo & index_granularity_info;
    bool save_marks_in_cache = false;
    size_t columns_in_mark;
    MarkCache::MappedPtr marks;
    ReadSettings read_settings;

    MarkCache::MappedPtr loadMarks();
    std::future<MarkCache::MappedPtr> loadMarksAsync();
    MarkCache::MappedPtr loadMarksImpl();

    std::future<MarkCache::MappedPtr> future;
    ThreadPool * load_marks_threadpool;
};

}
