#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>


namespace DB
{

/// Class for reading a single column (or index).
class MergeTreeReaderStream
{
public:
    using GranularityInfo = MergeTreeData::IndexGranularityInfo;
    MergeTreeReaderStream(
            const String & path_prefix_, const String & data_file_extension_, size_t marks_count_,
            const MarkRanges & all_mark_ranges,
            MarkCache * mark_cache, bool save_marks_in_cache,
            UncompressedCache * uncompressed_cache,
            size_t file_size, size_t aio_threshold, size_t max_read_buffer_size,
            const GranularityInfo * index_granularity_info_,
            const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type);

    void seekToMark(size_t index);

    void seekToStart();

    ReadBuffer * data_buffer;

private:
    /// NOTE: lazily loads marks from the marks cache.
    const MarkInCompressedFile & getMark(size_t index);

    void loadMarks();

    std::string path_prefix;
    std::string data_file_extension;

    size_t marks_count;

    MarkCache * mark_cache;
    bool save_marks_in_cache;
    MarkCache::MappedPtr marks;

    const GranularityInfo * index_granularity_info;

    std::unique_ptr<CachedCompressedReadBuffer> cached_buffer;
    std::unique_ptr<CompressedReadBufferFromFile> non_cached_buffer;
};
}
