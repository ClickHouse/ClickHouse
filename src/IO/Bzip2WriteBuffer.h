#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBufferDecorator.h>

#include "config.h"

#if USE_BZIP2
#    include <bzlib.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BZIP2_STREAM_ENCODER_FAILED;
}

class Bzip2WriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    template<typename WriteBufferT>
    Bzip2WriteBuffer(
        WriteBufferT && out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr, /// NOLINT(readability-non-const-parameter)
        size_t alignment = 0,
        bool compress_empty_ = true)
    : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment) /// NOLINT(bugprone-move-forwarding-reference)
    , compress_empty(compress_empty_)
    {
        memset(&stream, 0, sizeof(stream));

        int ret = BZ2_bzCompressInit(&stream, compression_level, 0, 0);

        if (ret != BZ_OK)
            throw Exception(
                ErrorCodes::BZIP2_STREAM_ENCODER_FAILED,
                "bzip2 stream encoder init failed: error code: {}",
                ret);
    }

    ~Bzip2WriteBuffer() override;

private:
    void nextImpl() override;

    void finalizeBefore() override;

    bz_stream stream;
    bool compress_empty = true;
    UInt64 total_in = 0;
};

}

#endif
