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
    MarksInCompressedFile() = default;

    MarksInCompressedFile(size_t marks_size_)
        : marks_size(marks_size_)
        , marks_data(Allocator::alloc(marks_size * sizeof(MarkInCompressedFile)))
        , owns_marks_data(true)
    {}

    MarksInCompressedFile(size_t marks_size_, void * marks_data_)
        : marks_size(marks_size_)
        , marks_data(marks_data_)
        , owns_marks_data(false)
    {}

    MarksInCompressedFile(MarksInCompressedFile&& rhs)
    {
        *this = std::move(rhs);
    }

    MarksInCompressedFile& operator=(MarksInCompressedFile&& rhs)
    {
        std::swap(marks_size, rhs.marks_size);
        std::swap(owns_marks_data, rhs.owns_marks_data);
        std::swap(marks_data, rhs.marks_data);

        return *this;
    }

    inline void read(ReadBuffer & buffer, size_t from, size_t count)
    {
        buffer.readStrict(reinterpret_cast<char *>(data() + from), count * sizeof(MarkInCompressedFile));
    }

    inline const MarkInCompressedFile * data() const { return static_cast<MarkInCompressedFile*>(marks_data); }

    inline MarkInCompressedFile * data() { return static_cast<MarkInCompressedFile*>(marks_data); }

    inline const MarkInCompressedFile & operator[](size_t i) const { return data()[i]; }

    inline MarkInCompressedFile & operator[](size_t i) { return data()[i]; }

    inline size_t size() const { return marks_size; }

    ~MarksInCompressedFile()
    {
        dealloc();
    }
private:
    inline void dealloc()
    {
        if (owns_marks_data && marks_data)
        {
            Allocator::free(marks_data, marks_size * sizeof(MarkInCompressedFile));
            marks_data = nullptr;    /// To avoid double free if next alloc will throw an exception
        }
    }

    size_t marks_size = 0;
    void * marks_data = nullptr;
    bool owns_marks_data = false;
};

}
