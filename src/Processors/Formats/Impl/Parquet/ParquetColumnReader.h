#pragma once

#include <Columns/IColumn.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Processors/Formats/Impl/Parquet/RowRanges.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace parquet
{

class PageReader;
class ColumnChunkMetaData;
class DataPageV1;
class DataPageV2;

}

namespace DB
{
using ParquetColumnReadSequence = std::vector<Int32>;
class ParquetColumnReadState
{
public:
    explicit ParquetColumnReadState(const ParquetColumnReadSequence & read_seq_)
        : read_seq(read_seq_)
    {
        LOG_ERROR(getLogger("ParquetColumnReadState"), "xxx read seq: {}", fmt::join(read_seq, ", "));
    }

    void skip(UInt32 rows)
    {
        advance(-static_cast<Int32>(rows));
    }

    void read(UInt32 rows)
    {
        advance(static_cast<Int32>(rows));
    }

    bool hasMoreToRead() const
    {
        return read_seq_idx < read_seq.size();
    }

    Int32 nextRowsToRead() const
    {
        return read_seq[read_seq_idx];
    }

private:
    ParquetColumnReadSequence read_seq;
    size_t read_seq_idx = 0;

    void advance(Int32 rows)
    {
        read_seq[read_seq_idx] -= rows;
        if (read_seq[read_seq_idx] == 0)
            ++read_seq_idx;
    }
};

class ParquetColumnReader
{
public:
    /// Read a batch of rows from the current column reader.
    virtual ColumnWithTypeAndName readBatch(UInt64 max_read_rows, const String & name) = 0;

    virtual ~ParquetColumnReader() = default;

    void setupReadState(const ParquetColumnReadSequence & read_seq_)
    {
        read_state = std::make_unique<ParquetColumnReadState>(read_seq_);
    }

    bool hasMoreToRead() const
    {
        return read_state->hasMoreToRead();
    }
protected:
    std::unique_ptr<ParquetColumnReadState> read_state;

    /// Skip records in the current column reader.
    virtual UInt64 skipRecords(UInt64 num_records) = 0;
};

using ParquetColReaderPtr = std::unique_ptr<ParquetColumnReader>;
using ParquetColReaders = std::vector<ParquetColReaderPtr>;

}
