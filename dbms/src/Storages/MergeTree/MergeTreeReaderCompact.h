#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <port/clock.h>


namespace DB
{

class MarksInCompressedFileCompact
{
public:
    using MarksPtr = MarkCache::MappedPtr;

    MarksInCompressedFileCompact() = default;

    MarksInCompressedFileCompact(const MarksPtr & data_, size_t columns_num_)
        : data(data_), columns_num(columns_num_) {}

    const MarkInCompressedFile & getMark(size_t index, size_t column) const
    {
        return (*data)[index * columns_num + column];
    }

    char * getRowAddress(size_t index) const
    {
        return reinterpret_cast<char *>(data->data() + index * columns_num);
    }

    size_t getRowSize() const
    {
        return sizeof(MarkInCompressedFile) * columns_num;
    }

private:
    MarksPtr data;
    size_t columns_num;
};

/// Reads the data between pairs of marks in the same part. When reading consecutive ranges, avoids unnecessary seeks.
/// When ranges are almost consecutive, seeks are fast because they are performed inside the buffer.
/// Avoids loading the marks file if it is not needed (e.g. when reading the whole part).
class MergeTreeReaderCompact : public IMergeTreeReader
{
public:
    MergeTreeReaderCompact(const MergeTreeData::DataPartPtr & data_part_,
        const NamesAndTypesList & columns_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        const MarkRanges & mark_ranges_,
        const ReaderSettings & settings_,
        const ValueSizeMap & avg_value_size_hints_ = ValueSizeMap{});

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Block & res) override;

private:
    ReadBuffer * data_buffer;

    MarksInCompressedFileCompact marks;

    void loadMarks();

    static auto constexpr NAME_OF_FILE_WITH_DATA = "data";

    /// Columns that are read.

    friend class MergeTreeRangeReader::DelayedStream;
};

}
