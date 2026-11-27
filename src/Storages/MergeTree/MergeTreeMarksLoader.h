#pragma once

#include <IO/ReadSettings.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Common/ThreadPool_fwd.h>

#include <atomic>
#include <future>
#include <optional>

namespace DB
{

struct MergeTreeIndexGranularityInfo;
using MarksPtr = MarkCache::MappedPtr;
class Threadpool;
class ReadBuffer;

/// Class that helps to get marks by indexes.
/// Always immutable and thread safe.
/// Marks can be shared between several threads
/// that read columns from the same file.
class MergeTreeMarksGetter
{
public:
    MergeTreeMarksGetter(MarkCache::MappedPtr marks_, size_t num_columns_in_mark_);

    [[nodiscard]] MarkInCompressedFile getMark(size_t row_index, size_t column_index) const;
    [[nodiscard]] size_t getNumColumns() const { return num_columns_in_mark; }

private:
    const MarkCache::MappedPtr marks;
    const size_t num_columns_in_mark;
};

using MergeTreeMarksGetterPtr = std::unique_ptr<const MergeTreeMarksGetter>;

/// Marks stored as a byte range of a file rather than as a marks file of their own: compressed blocks of non-adaptive
/// marks, whatever the part's mark type.
struct MergeTreeMarksSlice
{
    size_t begin = 0;
    size_t end = 0;
};

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
        size_t num_columns_in_mark_,
        bool use_streaming_compression_,
        std::optional<MergeTreeMarksSlice> slice_ = {});

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
    const bool use_streaming_compression;
    const std::optional<MergeTreeMarksSlice> slice;

    std::mutex load_mutex;
    MarkCache::MappedPtr marks;

    /// The decompressed marks and their format.
    struct OpenedMarks
    {
        std::unique_ptr<ReadBuffer> reader;
        bool adaptive = false;
        size_t mark_size = 0;
        String path_for_errors;
    };
    /// Opens the marks file, or the slice of it.
    OpenedMarks openMarksReader();
    MarkCache::Key getCacheKey() const;

    MarkCache::MappedPtr loadMarksImpl();
    MarkCache::MappedPtr loadMarksSync();
    std::future<MarkCache::MappedPtr> loadMarksAsync();

    std::future<MarkCache::MappedPtr> future;
    ThreadPool * load_marks_threadpool;
    std::atomic<bool> is_canceled{false};
};

using MergeTreeMarksLoaderPtr = std::shared_ptr<MergeTreeMarksLoader>;

class IMergeTreeDataPart;
struct MergeTreeSettings;

/// Adds computed marks for part to the marks cache.
void addMarksToCache(const IMergeTreeDataPart & part, const PlainMarksByName & cached_marks, MarkCache * mark_cache);

/// Removes cached marks for all columns from part.
void removeMarksFromCache(const IMergeTreeDataPart & part, MarkCache * mark_cache);

/// Returns the list of columns suitable for prewarming of mark cache according to settings.
Names getColumnsToPrewarmMarks(const MergeTreeSettings & settings, const NamesAndTypesList & columns_list);

}
