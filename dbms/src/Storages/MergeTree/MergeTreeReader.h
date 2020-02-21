#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>


namespace DB
{

class IDataType;

/// Reads the data between pairs of marks in the same part. When reading consecutive ranges, avoids unnecessary seeks.
/// When ranges are almost consecutive, seeks are fast because they are performed inside the buffer.
/// Avoids loading the marks file if it is not needed (e.g. when reading the whole part).
class MergeTreeReader : private boost::noncopyable
{
public:
    using ValueSizeMap = std::map<std::string, double>;
    using DeserializeBinaryBulkStateMap = std::map<std::string, IDataType::DeserializeBinaryBulkStatePtr>;

    MergeTreeReader(String path_, /// Path to the directory containing the part
        MergeTreeData::DataPartPtr data_part_,
        NamesAndTypesList columns_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        bool save_marks_in_cache_,
        const MergeTreeData & storage_,
        MarkRanges all_mark_ranges_,
        size_t aio_threshold_,
        size_t mmap_threshold_,
        size_t max_read_buffer_size_,
        ValueSizeMap avg_value_size_hints_ = ValueSizeMap{},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_ = ReadBufferFromFileBase::ProfileCallback{},
        clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE);

    ~MergeTreeReader();

    const ValueSizeMap & getAvgValueSizeHints() const;

    /// Add columns from ordered_names that are not present in the block.
    /// Missing columns are added in the order specified by ordered_names.
    /// num_rows is needed in case if all res_columns are nullptr.
    void fillMissingColumns(Columns & res_columns, bool & should_evaluate_missing_defaults, size_t num_rows);
    /// Evaluate defaulted columns if necessary.
    void evaluateMissingDefaults(Block additional_columns, Columns & res_columns);

    const NamesAndTypesList & getColumns() const { return columns; }
    size_t numColumnsInResult() const { return columns.size(); }

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark.
    /// Fills res_columns in order specified in getColumns() list. If column was not read it will be nullptr.
    size_t readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns);

    MergeTreeData::DataPartPtr data_part;

    size_t getFirstMarkToRead() const
    {
        return all_mark_ranges.front().begin;
    }
private:
    using FileStreams = std::map<std::string, std::unique_ptr<MergeTreeReaderStream>>;

    /// avg_value_size_hints are used to reduce the number of reallocations when creating columns of variable size.
    ValueSizeMap avg_value_size_hints;
    /// Stores states for IDataType::deserializeBinaryBulk
    DeserializeBinaryBulkStateMap deserialize_binary_bulk_state_map;
    /// Path to the directory containing the part
    String path;

    FileStreams streams;

    /// Columns that are read.
    NamesAndTypesList columns;

    UncompressedCache * uncompressed_cache;
    MarkCache * mark_cache;
    /// If save_marks_in_cache is false, then, if marks are not in cache, we will load them but won't save in the cache, to avoid evicting other data.
    bool save_marks_in_cache;

    const MergeTreeData & storage;
    MarkRanges all_mark_ranges;
    size_t aio_threshold;
    size_t mmap_threshold;
    size_t max_read_buffer_size;

    void addStreams(const String & name, const IDataType & type,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type);

    void readData(
        const String & name, const IDataType & type, IColumn & column,
        size_t from_mark, bool continue_reading, size_t max_rows_to_read,
        bool read_offsets = true);


    friend class MergeTreeRangeReader::DelayedStream;
};

}
