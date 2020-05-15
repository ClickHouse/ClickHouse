#pragma once

#include <memory>
#include <string_view>
#include <time.h>
#include <IO/ReadBufferFromFileBase.h>
#include "CompressedReadBufferBase.h"
#include <IO/UncompressedCache.h>

namespace DB
{
/**
 * A buffer for reading from a compressed file using the cache of decompressed blocks.
 * The external cache is passed as an argument to the constructor.
 * Allows you to increase performance in cases where the same blocks are often read.
 * Disadvantages:
 * - in case you need to read a lot of data in a row, but of them only a part is cached, you have to do seek-and.
 */
class CachedCompressedReadBuffer : public CompressedReadBufferBase, public ReadBuffer
{
public:
    using CreatorCallback = std::function<std::unique_ptr<ReadBufferFromFileBase>()>;

    CachedCompressedReadBuffer(
            std::string_view path_,
            CreatorCallback file_in_creator_,
            UncompressedCache * cache_):
        ReadBuffer(nullptr, 0),
        file_in_creator(std::move(file_in_creator_)),
        cache(cache_),
        path(path_),
        file_pos(0) { }

    void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block);

    void setProfileCallback(
            const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
            clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE)
    {
        profile_callback = profile_callback_;
        clock_type = clock_type_;
    }

private:
    CreatorCallback file_in_creator;
    UncompressedCache * cache;

    std::unique_ptr<ReadBufferFromFileBase> file_in;

    const std::string path;
    size_t file_pos;

    /// A piece of data from the cache, or a piece of read data that we put into the cache.
    UncompressedCache::ValuePtr owned_cell;

    /// Passed into file_in.
    ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type {};

    void initInput() noexcept
    {
        if (file_in) return;

        file_in = file_in_creator();
        compressed_in = file_in.get();

        if (profile_callback)
            file_in->setProfileCallback(profile_callback, clock_type);
    }

    bool nextImpl() override;
};
}

