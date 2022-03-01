#pragma once

#include <Compression/CompressedReadBufferBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <time.h>
#include <memory>


namespace DB
{

class MMappedFileCache;


/// Unlike CompressedReadBuffer, it can do seek.
class CompressedReadBufferFromFile : public CompressedReadBufferBase, public BufferWithOwnMemory<ReadBuffer>
{
private:
      /** At any time, one of two things is true:
      * a) size_compressed = 0
      * b)
      *  - `working_buffer` contains the entire block.
      *  - `file_in` points to the end of this block.
      *  - `size_compressed` contains the compressed size of this block.
      */
    std::unique_ptr<ReadBufferFromFileBase> p_file_in;
    ReadBufferFromFileBase & file_in;
    size_t size_compressed = 0;

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

    bool nextImpl() override;

    void prefetch() override;

public:
    explicit CompressedReadBufferFromFile(std::unique_ptr<ReadBufferFromFileBase> buf, bool allow_different_codecs_ = false);

    /// Seek is lazy in some sense. We move position in compressed file_in to offset_in_compressed_file, but don't
    /// read data into working_buffer and don't shift our position to offset_in_decompressed_block. Instead
    /// we store this offset inside nextimpl_working_buffer_offset.
    void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block) override;

    size_t readBig(char * to, size_t n) override;

    void setProfileCallback(const ReadBufferFromFileBase::ProfileCallback & profile_callback_, clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE)
    {
        file_in.setProfileCallback(profile_callback_, clock_type_);
    }

    void setReadUntilPosition(size_t position) override { file_in.setReadUntilPosition(position); }

    void setReadUntilEnd() override { file_in.setReadUntilEnd(); }
};

}
