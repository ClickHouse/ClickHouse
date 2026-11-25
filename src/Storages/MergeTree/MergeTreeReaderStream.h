#pragma once
#include <Compression/CompressedReadBufferFromFile.h>
#include <IO/ReadBuffer.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <base/defines.h>
#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/HashTable/Hash.h>
#include <absl/container/flat_hash_map.h>

#include <atomic>
#include <mutex>


namespace DB
{

class IDataPartStorage;
using DataPartStoragePtr = std::shared_ptr<const IDataPartStorage>;

class UncompressedCache;
class CachedCompressedReadBuffer;

struct RuntimeDataflowStatisticsBlock;

/// The ranges of the files the readers of a read load, counted into its statistics, each range once however many readers
/// load it. The memory the ranges hold is charged to `automatic_parallel_replicas_max_counted_ranges` as it grows; past
/// it, the counting stops, the ranges are freed, and the statistics record that the input was not measured. The ranges
/// still kept unmerged are counted when the registry is destroyed, before the statistics are, since it holds them.
class ReadRangesRegistry
{
public:
    /// `duplicated` is that of the `RuntimeDataflowStatisticsCacheUpdater` of the read.
    ReadRangesRegistry(std::shared_ptr<RuntimeDataflowStatisticsBlock> block_, bool duplicated_);
    ~ReadRangesRegistry();

    /// The counted ranges of a file. The readers of several threads count the same file.
    /// The ranges are appended and merged in bulk, which is several times faster than keeping them in a tree and takes
    /// 16 bytes a range.
    class ReadRanges
    {
    public:
        /// The bytes of the ranges not counted yet that become counted: those merged, and those extending the last
        /// merged range. Clears the ranges. `capacity_grown` is how many ranges the memory kept for them grew by.
        size_t add(CompressedBlockRanges & ranges, size_t & capacity_grown);
        /// Merges the appended ranges into the merged ones and returns the bytes they add.
        size_t merge() noexcept;
        /// Frees the ranges, once the counting stopped.
        void release() noexcept;

    private:
        size_t mergeLocked() noexcept TSA_REQUIRES(mutex);

        std::mutex mutex;
        /// Up to `merged`: sorted by their begin, neither overlapping nor touching. After it: appended since the last merge.
        CompressedBlockRanges united_ranges TSA_GUARDED_BY(mutex);
        size_t merged TSA_GUARDED_BY(mutex) = 0;
        /// The bytes of the merged ranges.
        size_t merged_bytes TSA_GUARDED_BY(mutex) = 0;
    };

    /// The counted ranges of the file at the path, valid as long as this object. A reader looks it up once by stream.
    ReadRanges & getReadRanges(const String & path);

    /// Returns the bytes of `ranges`, which a reader loaded from the file of `file_ranges`, that are not counted yet.
    /// Clears `ranges`.
    size_t registerRanges(ReadRanges & file_ranges, CompressedBlockRanges & ranges);
    /// Adds to the statistics at once the bytes a reader counted from its files since the last `registerReadRanges`.
    void commit(size_t bytes);

    /// Past the limit: nothing is counted any more, and the readers stop collecting the ranges.
    [[nodiscard]] bool stopped() const;

private:
    /// A file takes about as much memory as 8 ranges: its `ReadRanges`, its slot in the map and the `unique_ptr`.
    /// Charged against the limit when a new file entry is created.
    static constexpr size_t units_per_file = 8;

    /// Reserves `units` of the limit; past it, stops the counting instead.
    bool reserve(size_t units);

    /// Frees the ranges of all the files, once the counting stopped. The files stay, as the readers refer to them.
    void releaseAll();

    const std::shared_ptr<RuntimeDataflowStatisticsBlock> block;
    const bool duplicated;

    std::mutex mutex;
    /// By the `sipHash128` of the path, which takes less than the path. The values do not move when the map grows.
    absl::flat_hash_map<
        UInt128,
        std::unique_ptr<ReadRanges>,
        UInt128TrivialHash,
        std::equal_to<>,
        AllocatorWithMemoryTracking<std::pair<const UInt128, std::unique_ptr<ReadRanges>>>>
        read_ranges_by_path TSA_GUARDED_BY(mutex);
    std::atomic_bool all_released{false};
    /// Given out for the files looked up once the counting stopped, which then keep nothing.
    static ReadRanges not_registered;
};

using ReadRangesRegistryPtr = std::shared_ptr<ReadRangesRegistry>;

/// Basic and the most low-level class
/// for reading single columns or indexes.
class MergeTreeReaderStream
{
public:
    MergeTreeReaderStream(
        DataPartStoragePtr data_part_storage_,
        const String & path_prefix_,
        const String & data_file_extension_,
        size_t marks_count_,
        const MarkRanges & all_mark_ranges_,
        const MergeTreeReaderSettings & settings_,
        UncompressedCache * uncompressed_cache_,
        size_t file_size_,
        MergeTreeMarksLoaderPtr marks_loader_,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
        clockid_t clock_type_);

    virtual ~MergeTreeReaderStream();

    /// Returns true if the mark file has at most `max_transitions` distinct
    /// consecutive (offset_in_compressed_file, offset_in_decompressed_block)
    /// positions. Loads marks from cache if available.
    /// Costs at most `max_transitions` binary searches over the marks.
    bool hasAtMostNDistinctMarks(size_t max_transitions) const;

    /// Seeks to start of @row_index mark. Column position is implementation defined.
    virtual void seekToMark(size_t row_index) = 0;

    /// Seeks to exact mark in file.
    void seekToMarkAndColumn(size_t row_index, size_t column_position);

    void seekToMark(const MarkInCompressedFile & mark);

    /// Seeks to the start of the file.
    void seekToStart();

    /**
     * Does buffer need to know something about mark ranges bounds it is going to read?
     * (In case of MergeTree* tables). Mostly needed for reading from remote fs.
     */
    void adjustRightMark(size_t right_mark);
    ReadBuffer * getDataBuffer();

    size_t getFileSize() const { return file_size; }

    /// See `CompressedReadBufferBase::setRangesReadSink`. A stream of uncompressed data records the ranges of the file it
    /// consumed, each closed by a seek or by `recordBytesConsumed`.
    void setRangesReadSink(CompressedBlockRanges * ranges_read_);
    /// Records the range of uncompressed data consumed since the previous seek or call.
    void recordBytesConsumed();

    /// Announces the new map if the buffer exists.
    void updateReadRequestMap(MarkRangesPtr request_map_);

private:
    /// Returns offset in file up to which it's needed to read file to read all rows up to @right_mark mark.
    virtual size_t getRightOffset(size_t right_mark) = 0;

    /// Offset of the first byte of @mark, or `nullopt` when marks do not delimit what the stream reads.
    virtual std::optional<size_t> getLeftOffset(size_t /* mark */) { return std::nullopt; }

    void announceReadRequestMap();

    /// Returns estimated max amount of bytes to read among mark ranges (which is used as size for read buffer)
    /// and total amount of bytes to read in all mark ranges.
    virtual std::pair<size_t, size_t> estimateMarkRangeBytes(const MarkRanges & mark_ranges) = 0;

    const ReadBufferFromFileBase::ProfileCallback profile_callback;
    const clockid_t clock_type;
    const MarkRanges all_mark_ranges;

    const DataPartStoragePtr data_part_storage;
    const std::string path_prefix;
    const std::string data_file_extension;

    UncompressedCache * const uncompressed_cache;
    ReadBuffer * data_buffer = nullptr;
    ReadBufferFromFileBase * plain_file_buffer = nullptr;
    CompressedReadBufferBase * compressed_data_buffer = nullptr;
    CompressedBlockRanges * ranges_read = nullptr;
    /// See `recordBytesConsumed`.
    size_t consumed_begin = 0;
    std::unique_ptr<ReadBuffer> read_buffer_holder;

    bool initialized = false;
    std::optional<size_t> last_right_offset;

protected:
    void init();
    void loadMarks();

    const MergeTreeReaderSettings settings;
    const size_t marks_count;
    const size_t file_size;

    const MergeTreeMarksLoaderPtr marks_loader;
    MergeTreeMarksGetterPtr marks_getter;

private:
    MarkRangesPtr request_map;
};

/// Class for reading a single column (or index) from file
/// that contains a single column (for wide parts).
class MergeTreeReaderStreamSingleColumn : public MergeTreeReaderStream
{
public:
    template <typename... Args>
    explicit MergeTreeReaderStreamSingleColumn(Args &&... args)
        : MergeTreeReaderStream{std::forward<Args>(args)...}
    {
    }

    size_t getRightOffset(size_t right_mark_non_included) override;
    std::optional<size_t> getLeftOffset(size_t mark) override;
    std::pair<size_t, size_t> estimateMarkRangeBytes(const MarkRanges & mark_ranges) override;
    void seekToMark(size_t row_index) override { seekToMarkAndColumn(row_index, 0); }
};

class MergeTreeReaderStreamSingleColumnWholePart : public MergeTreeReaderStream
{
public:
    template <typename... Args>
    explicit MergeTreeReaderStreamSingleColumnWholePart(Args &&... args)
        : MergeTreeReaderStream{std::forward<Args>(args)...}
    {
    }

    size_t getRightOffset(size_t right_mark_non_included) override;
    std::pair<size_t, size_t> estimateMarkRangeBytes(const MarkRanges & mark_ranges) override;
    void seekToMark(size_t row_index) override;
};

/// Base class for reading from file that contains multiple columns.
/// It is used to read from compact parts.
/// See more details about data layout in MergeTreeDataPartCompact.h.
class MergeTreeReaderStreamMultipleColumns : public MergeTreeReaderStream
{
public:
    template <typename... Args>
    explicit MergeTreeReaderStreamMultipleColumns(Args &&... args)
        : MergeTreeReaderStream{std::forward<Args>(args)...}
    {
    }

protected:
    size_t getRightOffsetOneColumn(size_t right_mark_non_included, size_t column_position);
    std::optional<size_t> getLeftOffsetOneColumn(size_t mark, size_t column_position);
    std::pair<size_t, size_t> estimateMarkRangeBytesOneColumn(const MarkRanges & mark_ranges, size_t column_position);
    MarkInCompressedFile getStartOfNextStripeMark(size_t row_index, size_t column_position);
};

/// Class for reading a single column from file that contains multiple columns
/// (for parallel reading from compact parts with large stripes).
class MergeTreeReaderStreamOneOfMultipleColumns : public MergeTreeReaderStreamMultipleColumns
{
public:
    template <typename... Args>
    explicit MergeTreeReaderStreamOneOfMultipleColumns(size_t column_position_, Args &&... args)
        : MergeTreeReaderStreamMultipleColumns{std::forward<Args>(args)...}
        , column_position(column_position_)
    {
    }

    size_t getRightOffset(size_t right_mark_non_included) override;
    std::optional<size_t> getLeftOffset(size_t mark) override { return getLeftOffsetOneColumn(mark, column_position); }
    std::pair<size_t, size_t> estimateMarkRangeBytes(const MarkRanges & mark_ranges) override;
    void seekToMark(size_t row_index) override { seekToMarkAndColumn(row_index, column_position); }

private:
    const size_t column_position;
};

/// Class for reading multiple columns from file that contains multiple columns
/// (for reading from compact parts with small stripes).
class MergeTreeReaderStreamAllOfMultipleColumns : public MergeTreeReaderStreamMultipleColumns
{
public:
    template <typename... Args>
    explicit MergeTreeReaderStreamAllOfMultipleColumns(Args &&... args)
        : MergeTreeReaderStreamMultipleColumns{std::forward<Args>(args)...}
    {
    }

    size_t getRightOffset(size_t right_mark_non_included) override;
    /// The first column comes first in a granule.
    std::optional<size_t> getLeftOffset(size_t mark) override { return getLeftOffsetOneColumn(mark, 0); }
    std::pair<size_t, size_t> estimateMarkRangeBytes(const MarkRanges & mark_ranges) override;
    void seekToMark(size_t row_index) override { seekToMarkAndColumn(row_index, 0); }
};

}
