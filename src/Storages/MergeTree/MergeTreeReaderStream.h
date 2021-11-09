#pragma once
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>


namespace DB
{

/// Class for reading a single column (or index).
class MergeTreeReaderStream
{
public:
    MergeTreeReaderStream(
        DiskPtr disk_,
        const String & path_prefix_, const String & data_file_extension_, size_t marks_count_,
        const MarkRanges & all_mark_ranges,
        const MergeTreeReaderSettings & settings_,
        MarkCache * mark_cache, UncompressedCache * uncompressed_cache,
        size_t file_size_, const MergeTreeIndexGranularityInfo * index_granularity_info_,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type);

    void seekToMark(size_t index);

    void seekToStart();

    /**
     * Does buffer need to know something about mark ranges bounds it is going to read?
     * (In case of MergeTree* tables). Mostly needed for reading from remote fs.
     */
    void adjustForRange(MarkRange range);

    ReadBuffer * data_buffer;

private:
    std::pair<size_t, size_t> getRightOffsetAndBytesRange(size_t left_mark, size_t right_mark);

    DiskPtr disk;
    std::string path_prefix;
    std::string data_file_extension;

    size_t marks_count;
    size_t file_size;

    MarkCache * mark_cache;
    bool save_marks_in_cache;

    size_t last_right_offset = 0;

    const MergeTreeIndexGranularityInfo * index_granularity_info;

    std::unique_ptr<CachedCompressedReadBuffer> cached_buffer;
    std::unique_ptr<CompressedReadBufferFromFile> non_cached_buffer;

    MergeTreeMarksLoader marks_loader;
};
}
