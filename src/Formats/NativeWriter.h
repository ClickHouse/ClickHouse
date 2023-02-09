#pragma once

#include <base/types.h>
#include <DataTypes/IDataType.h>
#include <Core/Block.h>

namespace DB
{

class WriteBuffer;
class CompressedWriteBuffer;
struct IndexForNativeFormat;

/** Serializes the stream of blocks in their native binary format (with names and column types).
  * Designed for communication between servers.
  *
  * A stream can be specified to write the index. The index contains offsets to each part of each column.
  * If an `append` is made to an existing file, and you need to write the index, then specify `initial_size_of_file`.
  */
class NativeWriter
{
public:
    /** If non-zero client_revision is specified, additional block information can be written.
      */
    NativeWriter(
        WriteBuffer & ostr_, UInt64 client_revision_, const Block & header_, bool remove_low_cardinality_ = false,
        IndexForNativeFormat * index_ = nullptr, size_t initial_size_of_file_ = 0);

    Block getHeader() const { return header; }
    void write(const Block & block);
    void flush();

    static String getContentType() { return "application/octet-stream"; }

private:
    WriteBuffer & ostr;
    UInt64 client_revision;
    Block header;
    IndexForNativeFormat * index = nullptr;
    size_t initial_size_of_file;    /// The initial size of the data file, if `append` done. Used for the index.
    /// If you need to write index, then `ostr` must be a CompressedWriteBuffer.
    CompressedWriteBuffer * ostr_concrete = nullptr;

    bool remove_low_cardinality;
};

}
