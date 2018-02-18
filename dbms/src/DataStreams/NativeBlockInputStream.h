#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <Common/PODArray.h>

namespace DB
{

class CompressedReadBufferFromFile;


/** The Native format can contain a separately located index,
  *  which allows you to understand where what column is located,
  *  and skip unnecessary columns.
  */

/** The position of one piece of a single column. */
struct IndexOfOneColumnForNativeFormat
{
    String name;
    String type;
    MarkInCompressedFile location;
};

/** The index for the data block. */
struct IndexOfBlockForNativeFormat
{
    using Columns = std::vector<IndexOfOneColumnForNativeFormat>;

    size_t num_columns;
    size_t num_rows;
    Columns columns;
};

/** The whole index. */
struct IndexForNativeFormat
{
    using Blocks = std::vector<IndexOfBlockForNativeFormat>;
    Blocks blocks;

    IndexForNativeFormat() {}

    IndexForNativeFormat(ReadBuffer & istr, const NameSet & required_columns)
    {
        read(istr, required_columns);
    }

    /// Read the index, only for the required columns.
    void read(ReadBuffer & istr, const NameSet & required_columns);
};


/** Deserializes the stream of blocks from the native binary format (with names and column types).
  * Designed for communication between servers.
  *
  * Can also be used to store data on disk.
  * In this case, can use the index.
  */
class NativeBlockInputStream : public IProfilingBlockInputStream
{
public:
    /// If a non-zero server_revision is specified, additional block information may be expected and read.
    NativeBlockInputStream(ReadBuffer & istr_, UInt64 server_revision_);

    /// For cases when data structure (header) is known in advance.
    /// NOTE We may use header for data validation and/or type conversions. It is not implemented.
    NativeBlockInputStream(ReadBuffer & istr_, const Block & header_, UInt64 server_revision_);

    /// For cases when we have an index. It allows to skip columns. Only columns specified in the index will be read.
    NativeBlockInputStream(ReadBuffer & istr_, UInt64 server_revision_,
        IndexForNativeFormat::Blocks::const_iterator index_block_it_,
        IndexForNativeFormat::Blocks::const_iterator index_block_end_);

    String getName() const override { return "Native"; }

    static void readData(const IDataType & type, IColumn & column, ReadBuffer & istr, size_t rows, double avg_value_size_hint);

    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    ReadBuffer & istr;
    Block header;
    UInt64 server_revision;

    bool use_index = false;
    IndexForNativeFormat::Blocks::const_iterator index_block_it;
    IndexForNativeFormat::Blocks::const_iterator index_block_end;
    IndexOfBlockForNativeFormat::Columns::const_iterator index_column_it;

    /// If an index is specified, then `istr` must be CompressedReadBufferFromFile.
    CompressedReadBufferFromFile * istr_concrete;

    PODArray<double> avg_value_size_hints;

    void updateAvgValueSizeHints(const Block & block);
};

}
