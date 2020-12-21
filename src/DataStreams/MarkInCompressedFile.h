#pragma once

#include <tuple>

#include <common/types.h>
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

class MarksInCompressedFile final: boost::noncopyable, Allocator<false>
{
public:
    MarksInCompressedFile()
        : marks_size(0)
        , owns_marks_data(false)
        , marks_data(nullptr)
    {}

    MarksInCompressedFile(size_t marks_size_)
        : marks_size(marks_size_)
        , owns_marks_data(true)
        , marks_data(Allocator::alloc(marks_size * sizeof(MarkInCompressedFile)))
    {}

    MarksInCompressedFile(size_t marks_size_, void * marks_data_)
        : marks_size(marks_size_)
        , owns_marks_data(false)
        , marks_data(marks_data_)
    {}

    MarksInCompressedFile(MarksInCompressedFile&& other)
        : marks_size(other.marks_size)
        , owns_marks_data(other.owns_marks_data)
        , marks_data(other.marks_data)
    {
        other.marks_size = 0;
        other.owns_marks_data = false;
        other.marks_data = nullptr;
    }

    MarksInCompressedFile& operator=(MarksInCompressedFile&& other)
    {
        marks_size = other.marks_size;
        owns_marks_data = other.owns_marks_data;
        marks_data = other.marks_data;

        other.marks_size = 0;
        other.owns_marks_data = false;
        other.marks_data = nullptr;

        return *this;
    }

    void read(ReadBuffer & buffer, size_t from, size_t count)
    {
        buffer.readStrict(reinterpret_cast<char *>(data() + from), count * sizeof(MarkInCompressedFile));
    }

    inline const MarkInCompressedFile * data() const { return static_cast<MarkInCompressedFile*>(marks_data); }

    inline MarkInCompressedFile * data() { return static_cast<MarkInCompressedFile*>(marks_data); }

    const MarkInCompressedFile & operator[](size_t i) const { return data()[i]; }

    MarkInCompressedFile & operator[](size_t i) { return data()[i]; }

    size_t size() const { return marks_size; }

    ~MarksInCompressedFile()
    {
        if (owns_marks_data && marks_data)
        {
            Allocator::free(marks_data, marks_size * sizeof(MarkInCompressedFile));
            marks_data = nullptr;    /// To avoid double free if next alloc will throw an exception
        }
    }
private:
    size_t marks_size;
    bool owns_marks_data;
    void * marks_data;
};

}
