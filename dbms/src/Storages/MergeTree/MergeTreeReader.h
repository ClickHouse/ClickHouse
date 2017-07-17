#pragma once

#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Core/NamesAndTypes.h>


namespace DB
{

class IDataType;
class CachedCompressedReadBuffer;
class CompressedReadBufferFromFile;


/// Reads the data between pairs of marks in the same part. When reading consecutive ranges, avoids unnecessary seeks.
/// When ranges are almost consecutive, seeks are fast because they are performed inside the buffer.
/// Avoids loading the marks file if it is not needed (e.g. when reading the whole part).
class MergeTreeReader : private boost::noncopyable
{
public:
    using ValueSizeMap = std::map<std::string, double>;

    MergeTreeReader(const String & path, /// Path to the directory containing the part
        const MergeTreeData::DataPartPtr & data_part, const NamesAndTypesList & columns,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        bool save_marks_in_cache,
        MergeTreeData & storage, const MarkRanges & all_mark_ranges,
        size_t aio_threshold, size_t max_read_buffer_size,
        const ValueSizeMap & avg_value_size_hints = ValueSizeMap{},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback = ReadBufferFromFileBase::ProfileCallback{},
        clockid_t clock_type = CLOCK_MONOTONIC_COARSE);

    ~MergeTreeReader();

    const ValueSizeMap & getAvgValueSizeHints() const;

    /// Create MergeTreeRangeReader iterator, which allows reading arbitrary number of rows from range.
    MergeTreeRangeReader readRange(size_t from_mark, size_t to_mark);

    /// Add columns from ordered_names that are not present in the block.
    /// Missing columns are added in the order specified by ordered_names.
    /// If at least one column was added, reorders all columns in the block according to ordered_names.
    void fillMissingColumns(Block & res, const Names & ordered_names, const bool always_reorder = false);

    /// The same as fillMissingColumns(), but always reorders columns according to ordered_names
    /// (even if no columns were added).
    void fillMissingColumnsAndReorder(Block & res, const Names & ordered_names);

private:
    class Stream
    {
    public:
        Stream(
            const String & path_prefix_, const String & extension_, size_t marks_count_,
            const MarkRanges & all_mark_ranges,
            MarkCache * mark_cache, bool save_marks_in_cache,
            UncompressedCache * uncompressed_cache,
            size_t aio_threshold, size_t max_read_buffer_size,
            const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type);

        static std::unique_ptr<Stream> createEmptyPtr();

        void seekToMark(size_t index);

        bool isEmpty() const { return is_empty; }

        ReadBuffer * data_buffer;

    private:
        Stream() = default;

        /// NOTE: lazily loads marks from the marks cache.
        const MarkInCompressedFile & getMark(size_t index);

        void loadMarks();

        std::string path_prefix;
        std::string extension;

        size_t marks_count;

        MarkCache * mark_cache;
        bool save_marks_in_cache;
        MarkCache::MappedPtr marks;

        std::unique_ptr<CachedCompressedReadBuffer> cached_buffer;
        std::unique_ptr<CompressedReadBufferFromFile> non_cached_buffer;

        bool is_empty = false;
    };

    using FileStreams = std::map<std::string, std::unique_ptr<Stream>>;

    /// avg_value_size_hints are used to reduce the number of reallocations when creating columns of variable size.
    ValueSizeMap avg_value_size_hints;
    String path;
    MergeTreeData::DataPartPtr data_part;

    FileStreams streams;

    /// Columns that are read.
    NamesAndTypesList columns;

    UncompressedCache * uncompressed_cache;
    MarkCache * mark_cache;
    /// If save_marks_in_cache is false, then, if marks are not in cache, we will load them but won't save in the cache, to avoid evicting other data.
    bool save_marks_in_cache;

    MergeTreeData & storage;
    MarkRanges all_mark_ranges;
    size_t aio_threshold;
    size_t max_read_buffer_size;

    void addStream(const String & name, const IDataType & type, const MarkRanges & all_mark_ranges,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type,
        size_t level = 0);

    void readData(
        const String & name, const IDataType & type, IColumn & column,
        size_t from_mark, bool continue_reading, size_t max_rows_to_read,
        size_t level = 0, bool read_offsets = true);

    void fillMissingColumnsImpl(Block & res, const Names & ordered_names, bool always_reorder);

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Block & res);

    friend class MergeTreeRangeReader;
};

}
