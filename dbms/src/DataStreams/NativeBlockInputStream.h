#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/MarkInCompressedFile.h>


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
    /** If a non-zero server_revision is specified, additional block information may be expected and read,
      * depending on what is supported for the specified revision.
      *
      * `index` is not required parameter. If set, only parts of columns specified in the index will be read.
      */
    NativeBlockInputStream(
        ReadBuffer & istr_, UInt64 server_revision_ = 0,
        bool use_index_ = false,
        IndexForNativeFormat::Blocks::const_iterator index_block_it_ = IndexForNativeFormat::Blocks::const_iterator{},
        IndexForNativeFormat::Blocks::const_iterator index_block_end_ = IndexForNativeFormat::Blocks::const_iterator{});

    String getName() const override { return "Native"; }

    String getID() const override
    {
        std::stringstream res;
        res << this;
        return res.str();
    }

    static void readData(const IDataType & type, IColumn & column, ReadBuffer & istr, size_t rows);

protected:
    Block readImpl() override;

private:
    ReadBuffer & istr;
    UInt64 server_revision;

    bool use_index;
    IndexForNativeFormat::Blocks::const_iterator index_block_it;
    IndexForNativeFormat::Blocks::const_iterator index_block_end;
    IndexOfBlockForNativeFormat::Columns::const_iterator index_column_it;

    /// If an index is specified, then `istr` must be CompressedReadBufferFromFile.
    CompressedReadBufferFromFile * istr_concrete;
};

}
