#pragma once

#include <memory>
#include <time.h>

#include <IO/createReadBufferFromFileBase.h>
#include <IO/CompressedReadBufferBase.h>
#include <IO/UncompressedCache.h>


namespace DB
{


/** A buffer for reading from a compressed file using the cache of decompressed blocks.
  * The external cache is passed as an argument to the constructor.
  * Allows you to increase performance in cases where the same blocks are often read.
  * Disadvantages:
  * - in case you need to read a lot of data in a row, but of them only a part is cached, you have to do seek-and.
  */
class CachedCompressedReadBuffer : public CompressedReadBufferBase, public ReadBuffer
{
private:
    const std::string path;
    UncompressedCache * cache;
    size_t buf_size;
    size_t estimated_size;
    size_t aio_threshold;

    std::unique_ptr<ReadBufferFromFileBase> file_in;
    size_t file_pos;

    /// A piece of data from the cache, or a piece of read data that we put into the cache.
    UncompressedCache::MappedPtr owned_cell;

    void initInput();
    bool nextImpl() override;

    /// Passed into file_in.
    ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type;

public:
    CachedCompressedReadBuffer(
        const std::string & path_, UncompressedCache * cache_, size_t estimated_size_, size_t aio_threshold_,
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE);


    void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block);

    void setProfileCallback(const ReadBufferFromFileBase::ProfileCallback & profile_callback_, clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE)
    {
        profile_callback = profile_callback_;
        clock_type = clock_type_;
    }
};

}
