#pragma once
#include <tuple>
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
        const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type,
        bool is_low_cardinality_dictionary_);

    void seekToMark(size_t index);

    void seekToStart();

    /**
     * Does buffer need to know something about mark ranges bounds it is going to read?
     * (In case of MergeTree* tables). Mostly needed for reading from remote fs.
     */
    void adjustRightMark(size_t right_mark);

    ReadBuffer * getDataBuffer();
    CompressedReadBufferBase * getCompressedDataBuffer();

private:
    void init();
    size_t getRightOffset(size_t right_mark_non_included);

    const MergeTreeReaderSettings settings;
    const ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type;
    const MarkRanges all_mark_ranges;
    size_t file_size;
    UncompressedCache * uncompressed_cache;

    DiskPtr disk;
    std::string path_prefix;
    std::string data_file_extension;

    bool is_low_cardinality_dictionary = false;

    size_t marks_count;


    ReadBuffer * data_buffer;
    CompressedReadBufferBase * compressed_data_buffer;
    MarkCache * mark_cache;
    bool save_marks_in_cache;
    bool initialized = false;

    std::optional<size_t> last_right_offset;

    const MergeTreeIndexGranularityInfo * index_granularity_info;

    std::unique_ptr<CachedCompressedReadBuffer> cached_buffer;
    std::unique_ptr<CompressedReadBufferFromFile> non_cached_buffer;

    MergeTreeMarksLoader marks_loader;
};
}
