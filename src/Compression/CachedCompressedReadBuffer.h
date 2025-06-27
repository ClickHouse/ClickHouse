#pragma once

#include <memory>
#include <time.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Compression/CompressedReadBufferBase.h>
#include <IO/UncompressedCache.h>


namespace DB
{


/** A buffer for reading from a compressed file using the cache of decompressed blocks.
  * The external cache is passed as an argument to the constructor.
  * Allows you to increase performance in cases where the same blocks are often read.
  * Disadvantages:
  * - in case you need to read a lot of data in a row, but some of them only a part is cached, you have to do seek-and.
  */
class CachedCompressedReadBuffer final : public CompressedReadBufferBase, public ReadBuffer
{
private:
    std::function<std::unique_ptr<ReadBufferFromFileBase>()> file_in_creator;
    UncompressedCache * cache;
    std::unique_ptr<ReadBufferFromFileBase> file_in;

    const std::string path;

    /// Current position in file_in
    size_t file_pos;

    /// A piece of data from the cache, or a piece of read data that we put into the cache.
    UncompressedCache::MappedPtr owned_cell;

    void initInput();

    bool nextImpl() override;

    void prefetch(Priority priority) override;

    /// Passed into file_in.
    ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type {};


    /// Check comment in CompressedReadBuffer
    /* size_t nextimpl_working_buffer_offset; */

public:
    CachedCompressedReadBuffer(const std::string & path, std::function<std::unique_ptr<ReadBufferFromFileBase>()> file_in_creator, UncompressedCache * cache_, bool allow_different_codecs_ = false);

    /// Seek is lazy. It doesn't move the position anywhere, just remember them and perform actual
    /// seek inside nextImpl.
    void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block) override;

    void setProfileCallback(const ReadBufferFromFileBase::ProfileCallback & profile_callback_, clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE)
    {
        profile_callback = profile_callback_;
        clock_type = clock_type_;
    }

    void setReadUntilPosition(size_t position) override
    {
        initInput();
        file_in->setReadUntilPosition(position);
    }

    void setReadUntilEnd() override
    {
        initInput();
        file_in->setReadUntilEnd();
    }
};

}
