#pragma once

#include <Storages/MarkCache.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Common/ThreadPool_fwd.h>


namespace DB
{

struct MergeTreeIndexGranularityInfo;
using MarksPtr = MarkCache::MappedPtr;
struct ReadSettings;
class Threadpool;

/// Class that helps to get marks by indexes.
/// Always immutable and thread safe.
/// Marks can be shared between several threads
/// that read columns from the same file.
class MergeTreeMarksGetter
{
public:
    MergeTreeMarksGetter(MarkCache::MappedPtr marks_, size_t num_columns_in_mark_);

    MarkInCompressedFile getMark(size_t row_index, size_t column_index) const;
    size_t getNumColumns() const { return num_columns_in_mark; }

private:
    const MarkCache::MappedPtr marks;
    const size_t num_columns_in_mark;
};

using MergeTreeMarksGetterPtr = std::unique_ptr<const MergeTreeMarksGetter>;

/// Class that helps to load marks on demand.
/// Thread safe, but locks while loading marks.
class MergeTreeMarksLoader
{
public:
    MergeTreeMarksLoader(
        MergeTreeDataPartInfoForReaderPtr data_part_reader_,
        MarkCache * mark_cache_,
        const String & mrk_path,
        size_t marks_count_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        bool save_marks_in_cache_,
        const ReadSettings & read_settings_,
        ThreadPool * load_marks_threadpool_,
        size_t num_columns_in_mark_);

    ~MergeTreeMarksLoader();

    void startAsyncLoad();
    MergeTreeMarksGetterPtr loadMarks();
    size_t getNumColumns() const { return num_columns_in_mark; }

private:
    const MergeTreeDataPartInfoForReaderPtr data_part_reader;
    MarkCache * const mark_cache;
    const String mrk_path;
    const size_t marks_count;
    const MergeTreeIndexGranularityInfo & index_granularity_info;
    const bool save_marks_in_cache;
    const ReadSettings read_settings;
    const size_t num_columns_in_mark;

    std::mutex load_mutex;
    MarkCache::MappedPtr marks;

    MarkCache::MappedPtr loadMarksSync();
    std::future<MarkCache::MappedPtr> loadMarksAsync();
    MarkCache::MappedPtr loadMarksImpl();

    std::future<MarkCache::MappedPtr> future;
    ThreadPool * load_marks_threadpool;
};

using MergeTreeMarksLoaderPtr = std::shared_ptr<MergeTreeMarksLoader>;

class IMergeTreeDataPart;
struct MergeTreeSettings;

/// Adds computed marks for part to the marks cache.
void addMarksToCache(const IMergeTreeDataPart & part, const PlainMarksByName & cached_marks, MarkCache * mark_cache);

/// Returns the list of columns suitable for prewarming of mark cache according to settings.
Names getColumnsToPrewarmMarks(const MergeTreeSettings & settings, const NamesAndTypesList & columns_list);

}
