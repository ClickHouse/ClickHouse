#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <port/clock.h>


namespace DB
{

/// Reads the data between pairs of marks in the same part. When reading consecutive ranges, avoids unnecessary seeks.
/// When ranges are almost consecutive, seeks are fast because they are performed inside the buffer.
/// Avoids loading the marks file if it is not needed (e.g. when reading the whole part).ca
class MergeTreeReaderCompact : public IMergeTreeReader
{
public:
    MergeTreeReaderCompact(const MergeTreeData::DataPartPtr & data_part_,
        const NamesAndTypesList & columns_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        const MarkRanges & mark_ranges_,
        const MergeTreeReaderSettings & settings_,
        const ValueSizeMap & avg_value_size_hints_ = ValueSizeMap{});

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Block & res) override;

    bool canReadIncompleteGranules() const override { return false; }

private:
    ReadBuffer * data_buffer;
    std::unique_ptr<CachedCompressedReadBuffer> cached_buffer;
    std::unique_ptr<CompressedReadBufferFromFile> non_cached_buffer;

    MergeTreeMarksLoader marks_loader;

    std::vector<std::optional<size_t>> column_positions;

    size_t next_mark = 0;

    void initMarksLoader();
    void seekToStart();
    void seekToMark(size_t row, size_t col);
    const MarkInCompressedFile & getMark(size_t row, size_t col);

    void readData(const String & name, const IDataType & type, IColumn & column,
        size_t from_mark, size_t column_position, size_t rows_to_read);

    /// Columns that are read.

    friend class MergeTreeRangeReader::DelayedStream;
};

}
