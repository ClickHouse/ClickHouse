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
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>


namespace DB
{

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

    virtual ~MergeTreeReaderStream() = default;

    /// Seeks to start of @row_index mark. Column position is implementation defined.
    virtual void seekToMark(size_t row_index) = 0;

    /// Seeks to exact mark in file.
    void seekToMarkAndColumn(size_t row_index, size_t column_position);

    /// Seeks to the start of the file.
    void seekToStart();

    /**
     * Does buffer need to know something about mark ranges bounds it is going to read?
     * (In case of MergeTree* tables). Mostly needed for reading from remote fs.
     */
    void adjustRightMark(size_t right_mark);

    ReadBuffer * getDataBuffer();
    CompressedReadBufferBase * getCompressedDataBuffer();

private:
    /// Returns offset in file up to which it's needed to read file to read all rows up to @right_mark mark.
    virtual size_t getRightOffset(size_t right_mark) = 0;

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

    ReadBuffer * data_buffer;
    CompressedReadBufferBase * compressed_data_buffer;

    bool initialized = false;
    std::optional<size_t> last_right_offset;

    std::unique_ptr<CachedCompressedReadBuffer> cached_buffer;
    std::unique_ptr<CompressedReadBufferFromFile> non_cached_buffer;

protected:
    void init();
    void loadMarks();

    const MergeTreeReaderSettings settings;
    const size_t marks_count;
    const size_t file_size;

    const MergeTreeMarksLoaderPtr marks_loader;
    MergeTreeMarksGetterPtr marks_getter;
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
    std::pair<size_t, size_t> estimateMarkRangeBytes(const MarkRanges & mark_ranges) override;
    void seekToMark(size_t row_index) override { seekToMarkAndColumn(row_index, 0); }
};

}
