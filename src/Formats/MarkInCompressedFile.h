#pragma once

#include <tuple>
#include <mutex>

#include <base/types.h>
#include <IO/WriteHelpers.h>
#include <Common/PODArray.h>
#include <common/logger_useful.h>
#include <zstd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_DECOMPRESS;
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_READ_COMPRESSED_CHUNK;
}

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

size_t const BLOCK_SIZE = 128;

struct MarksIndex
{
  std::string data;
  std::vector<size_t> index;
  size_t current_block;
  bool current_block_initialized;
  char compression_buf[2 * BLOCK_SIZE * sizeof(MarkInCompressedFile)];
  size_t uncompressed_size;
  std::mutex mutex;

  MarksIndex(size_t n)
  {
    data.reserve((n * sizeof(MarkInCompressedFile)) / 5);
    index.reserve(n / BLOCK_SIZE + 1);
    uncompressed_size = 0;
    current_block_initialized = false;
  }

  MarkInCompressedFile get(size_t i)
  {
    std::lock_guard lock(mutex);

    size_t const block = i / BLOCK_SIZE;
    assert(block < index.size());
    //LOG_TRACE(&Poco::Logger::get("MarksInCompressedFile"),"get {} block {}", i, block);
    if (!current_block_initialized || current_block != block)
    {
      size_t const block_start = index[block];
      size_t const block_end = block + 1 == index.size() ? data.size() : index[block+1];
      LOG_TRACE(&Poco::Logger::get("MarksInCompressedFile"),"get {} block {} block_start {} block_end {} index.size {} data.size {}", i, block, block_start, block_end, index.size(), data.size());
      assert(block_start < block_end);
      uncompressed_size = ZSTD_decompress(&compression_buf,sizeof(compression_buf),&data[block_start],block_end - block_start);
      if (ZSTD_isError(uncompressed_size)) {
        current_block_initialized = false;
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,"MarksIndex");
      }
      current_block_initialized = true;
      current_block = block;
    }
    if (((i % BLOCK_SIZE) + 1) * sizeof(MarkInCompressedFile) > uncompressed_size)
      throw Exception(ErrorCodes::CANNOT_READ_COMPRESSED_CHUNK,"MarksIndex");
    return *(reinterpret_cast<MarkInCompressedFile*>(compression_buf) + (i % BLOCK_SIZE));
  }

  size_t size_in_bytes() const { return data.capacity() + index.capacity() * sizeof(size_t) + sizeof(MarksIndex); }
};

class MarksInCompressedFile
{
private:
    std::unique_ptr<PODArray<MarkInCompressedFile>> marks_;
    std::unique_ptr<MarksIndex> indexed_;
public:
    MarksInCompressedFile(size_t n) { marks_.reset(new PODArray<MarkInCompressedFile>(n)); }

    void read(ReadBuffer & buffer, size_t from, size_t count)
    {
        buffer.readStrict(reinterpret_cast<char *>(marks_->data() + from), count * sizeof(MarkInCompressedFile));
    }

    MarkInCompressedFile get(size_t i)
    {
      if (indexed_)
        return indexed_->get(i);

      return (*marks_)[i];
    }

    MarkInCompressedFile* data() { return marks_->data(); }

    size_t size_in_bytes() const { return (marks_ ? marks_->size() * sizeof(MarkInCompressedFile) : indexed_->size_in_bytes()); }

    void finish()
    {
      auto const& marks = *marks_;

      if (marks.size() < BLOCK_SIZE * 5) return;

      LOG_TRACE(&Poco::Logger::get("MarksInCompressedFile"),"indexing {} marks", marks.size());

      indexed_.reset(new MarksIndex(marks.size()));
      size_t start = 0;
      while (start < marks.size())
      {
        size_t const next = (start + BLOCK_SIZE < marks.size() ? start + BLOCK_SIZE : marks.size());
        size_t const compressed_size = ZSTD_compress(&indexed_->compression_buf, sizeof(indexed_->compression_buf), &marks[start], (next - start) * sizeof(MarkInCompressedFile), 1);
        if (ZSTD_isError(compressed_size))
          throw Exception(ErrorCodes::CANNOT_COMPRESS,"MarksIndex");
        indexed_->index.push_back(indexed_->data.size());
        indexed_->data.append(indexed_->compression_buf, compressed_size);
        start = next;
      }

      LOG_TRACE(&Poco::Logger::get("MarksInCompressedFile"),"indexed {} bytes ({} marks) into {} bytes", marks.size() * sizeof(MarkInCompressedFile), marks.size(), indexed_->size_in_bytes());
      indexed_->data.shrink_to_fit();
      indexed_->index.shrink_to_fit();
      LOG_TRACE(&Poco::Logger::get("MarksInCompressedFile"),"indexed and shrinked {} bytes ({} marks) into {} bytes", marks.size() * sizeof(MarkInCompressedFile), marks.size(), indexed_->size_in_bytes());
      marks_.reset();
    }
};

}
