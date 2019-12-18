#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <port/clock.h>


namespace DB
{

/// Reads the data between pairs of marks in the same part. When reading consecutive ranges, avoids unnecessary seeks.
/// When ranges are almost consecutive, seeks are fast because they are performed inside the buffer.
/// Avoids loading the marks file if it is not needed (e.g. when reading the whole part).
class MergeTreeReaderWide : public IMergeTreeReader
{
public:
    MergeTreeReaderWide(const MergeTreeData::DataPartPtr & data_part_,
        const NamesAndTypesList & columns_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        const MarkRanges & mark_ranges_,
        const MergeTreeReaderSettings & settings_,
        const ValueSizeMap & avg_value_size_hints_ = ValueSizeMap{},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_ = ReadBufferFromFileBase::ProfileCallback{},
        clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE);

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Block & res) override;

    bool canReadIncompleteGranules() const override { return true; }

private:
    using FileStreams = std::map<std::string, std::unique_ptr<MergeTreeReaderStream>>;

    FileStreams streams;

    /// Columns that are read.
    
    void addStreams(const String & name, const IDataType & type,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type);

    void readData(
        const String & name, const IDataType & type, IColumn & column,
        size_t from_mark, bool continue_reading, size_t max_rows_to_read,
        bool read_offsets = true);

    friend class MergeTreeRangeReader::DelayedStream;
};

}
