#pragma once

#include <tuple>

#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Common/PODArray.h>


namespace DB
{

/** Mark is the position in the compressed file. The compressed file consists of adjacent compressed blocks.
  * Mark is a tuple - the offset in the file to the start of the compressed block, the offset in the decompressed block to the start of the data.
  */
struct MarkInCompressedFile
{
    size_t offset_in_compressed_file;
    size_t offset_in_decompressed_block;

    bool operator==(const MarkInCompressedFile & rhs) const
    {
        return std::tie(offset_in_compressed_file, offset_in_decompressed_block)
            == std::tie(rhs.offset_in_compressed_file, rhs.offset_in_decompressed_block);
    }
    bool operator!=(const MarkInCompressedFile & rhs) const
    {
        return !(*this == rhs);
    }

    String toString() const
    {
        return "(" + DB::toString(offset_in_compressed_file) + "," + DB::toString(offset_in_decompressed_block) + ")";
    }

    String toStringWithRows(size_t rows_num)
    {
        return "(" + DB::toString(offset_in_compressed_file) + "," + DB::toString(offset_in_decompressed_block) + "," + DB::toString(rows_num) + ")";
    }

};

class MarksInCompressedFile : public PODArray<MarkInCompressedFile>
{
public:
    MarksInCompressedFile(size_t n) : PODArray(n) {}

    void read(ReadBuffer & buffer, size_t from, size_t count)
    {
        buffer.readStrict(reinterpret_cast<char *>(data() + from), count * sizeof(MarkInCompressedFile));
    }
};

}
