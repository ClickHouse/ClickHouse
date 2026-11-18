#pragma once

#include <memory>
#include <time.h>
#include <Compression/CompressedReadBufferBase.h>
#include <Compression/ZstdIncrementalDecompressor.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/LoggingHelpers.h>


namespace DB
{

class MMappedFileCache;


/// Unlike CompressedReadBuffer, it can do seek.
class CompressedReadBufferFromFile final : public CompressedReadBufferBase, public BufferWithOwnMemory<ReadBuffer>
{
private:
      /** At any time, one of three things is true:
      * a) size_compressed = 0 and working_buffer is empty
      * b)
      *  - `working_buffer` contains the entire block.
      *  - `file_in` points to the end of this block.
      *  - `size_compressed` contains the compressed size of this block.
      * c) the block is partially decompressed (`partial_decompressor.inProgress()`):
      *  - `working_buffer` contains the beginning of the block, up to the end of its decompressed part.
      *  - `file_in` points to the end of this block, and is not read or seeked until the reader leaves the block.
      *  - `size_compressed` contains the compressed size of this block.
      *  - `partial_decompressor` reads the rest of the compressed block from `own_compressed_buffer`, as `file_in`
      *    may discard its own buffer at any call.
      */
    std::unique_ptr<ReadBufferFromFileBase> p_file_in;
    ReadBufferFromFileBase & file_in;
    size_t size_compressed = 0;

    LogSeriesLimiter log;

    /// This field inherited from ReadBuffer. It's used to perform "lazy" seek, so in seek() call we:
    /// 1) actually seek only underlying compressed file_in to offset_in_compressed_file;
    /// 2) reset current working_buffer;
    /// 3) remember the position in decompressed block in nextimpl_working_buffer_offset.
    /// After following ReadBuffer::next() -> nextImpl call we will read new data into working_buffer and
    /// ReadBuffer::next() will move our position in the fresh working_buffer to nextimpl_working_buffer_offset and
    /// reset it to zero.
    ///
    /// NOTE: We have independent readBig implementation, so we have to take
    /// nextimpl_working_buffer_offset into account there as well.
    ///
    /* size_t nextimpl_working_buffer_offset; */

    /// A block compressed with ZSTD is decompressed block of the frame by block of the frame when a read needs only its
    /// beginning (for example, a PREWHERE step that reads the rows up to the last one that passed the previous steps).
    /// The rest is decompressed if the following reads reach it, and is never decompressed if the buffer seeks to
    /// another block. While a block is partially decompressed, `working_buffer` starts at the beginning of `memory`
    /// and ends at the end of the decompressed part (state c above).
    ZstdIncrementalDecompressor partial_decompressor;

    bool startPartialDecompression(size_t size_decompressed, size_t size_compressed_without_checksum, size_t min_bytes);
    void continuePartialDecompression(size_t min_bytes);

    bool nextImpl() override;

    void prefetch(Priority priority) override;

public:
    explicit CompressedReadBufferFromFile(std::unique_ptr<ReadBufferFromFileBase> buf, bool allow_different_codecs_ = false);

    /// Seek is lazy in some sense. We move position in compressed file_in to offset_in_compressed_file, but don't
    /// read data into working_buffer and don't shift our position to offset_in_decompressed_block. Instead
    /// we store this offset inside nextimpl_working_buffer_offset.
    void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block) override;

    [[nodiscard]] off_t getPosition() const override;

    [[nodiscard]] size_t readBig(char * to, size_t n) override;

    void setProfileCallback(const ReadBufferFromFileBase::ProfileCallback & profile_callback_, clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE)
    {
        file_in.setProfileCallback(profile_callback_, clock_type_);
    }

    void setReadUntilPosition(size_t position) override { file_in.setReadUntilPosition(position); }

    void setReadUntilEnd() override { file_in.setReadUntilEnd(); }

    void setRequestMap(ByteRangeSet ranges) override { file_in.setRequestMap(std::move(ranges)); }
};

}
