#pragma once

#include <Formats/IndexForNativeFormat.h>
#include <Formats/MarkInCompressedFile.h>
#include <Common/PODArray.h>
#include <Core/Block.h>

namespace DB
{

class CompressedReadBufferFromFile;

/** Deserializes the stream of blocks from the native binary format (with names and column types).
  * Designed for communication between servers.
  *
  * Can also be used to store data on disk.
  * In this case, can use the index.
  */
class NativeReader
{
public:
    /// If a non-zero server_revision is specified, additional block information may be expected and read.
    NativeReader(ReadBuffer & istr_, UInt64 server_revision_);

    /// For cases when data structure (header) is known in advance.
    /// NOTE We may use header for data validation and/or type conversions. It is not implemented.
    NativeReader(ReadBuffer & istr_, const Block & header_, UInt64 server_revision_, bool skip_unknown_columns_ = false);

    /// For cases when we have an index. It allows to skip columns. Only columns specified in the index will be read.
    NativeReader(ReadBuffer & istr_, UInt64 server_revision_,
        IndexForNativeFormat::Blocks::const_iterator index_block_it_,
        IndexForNativeFormat::Blocks::const_iterator index_block_end_);

    static void readData(const ISerialization & serialization, ColumnPtr & column, ReadBuffer & istr, size_t rows, double avg_value_size_hint);

    Block getHeader() const;

    void resetParser();

    Block read();

private:
    ReadBuffer & istr;
    Block header;
    UInt64 server_revision;
    bool skip_unknown_columns;

    bool use_index = false;
    IndexForNativeFormat::Blocks::const_iterator index_block_it;
    IndexForNativeFormat::Blocks::const_iterator index_block_end;
    IndexOfBlockForNativeFormat::Columns::const_iterator index_column_it;

    /// If an index is specified, then `istr` must be CompressedReadBufferFromFile. Unused otherwise.
    CompressedReadBufferFromFile * istr_concrete = nullptr;

    PODArray<double> avg_value_size_hints;

    void updateAvgValueSizeHints(const Block & block);
};

}
