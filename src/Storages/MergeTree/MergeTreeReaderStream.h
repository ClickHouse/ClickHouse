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
/**
 * @brief Loads a single column or index from #disk.
 *
 * - Marks are loaded via #marks_loader (paseed a DB::MarkCache ptr).
 * - Uncompressed data is loaded via #cached_buffer (passed a DB::UncompressedCache ptr).
 *
 * To understand how data is stored:
 *
 * @see DB::MarkCache
 * @see DB::UncompressedCache
 *
 * To understand how data is loaded:
 *
 * @see DB::MergeTreeMarksLoader
 * @see DB::CachedCompressedReadBuffer
 *
 * To understand where this class is used:
 *
 * @see DB::MergeTreeReaderWide
 */
class MergeTreeReaderStream
{
public:
    MergeTreeReaderStream(
        DiskPtr disk_,
        const String & path_prefix_,
        const String & data_file_extension_,
        size_t marks_count_,
        const MarkRanges & all_mark_ranges,
        const MergeTreeReaderSettings & settings_,
        MarkCache * mark_cache,
        UncompressedCache * uncompressed_cache,
        size_t file_size,
        const MergeTreeIndexGranularityInfo * index_granularity_info_,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback,
        clockid_t clock_type);

    void seekToMark(size_t index);

    void seekToStart();

    ReadBuffer * data_buffer;

private:
    DiskPtr disk;
    std::string path_prefix;
    std::string data_file_extension;

    size_t marks_count;

    MarkCache * mark_cache;
    bool save_marks_in_cache;

    const MergeTreeIndexGranularityInfo * index_granularity_info;

    std::unique_ptr<CachedCompressedReadBuffer> cached_buffer;
    std::unique_ptr<CompressedReadBufferFromFile> non_cached_buffer;

    MergeTreeMarksLoader marks_loader;
};
}
