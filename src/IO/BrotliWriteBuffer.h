#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBufferDecorator.h>

#include "config.h"

#if USE_BROTLI
#    include <brotli/encode.h>

namespace DB
{


class BrotliWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    template<typename WriteBufferT>
    BrotliWriteBuffer(
        WriteBufferT && out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr, /// NOLINT(readability-non-const-parameter)
        size_t alignment = 0,
        bool compress_empty_ = true)
    : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment) /// NOLINT(bugprone-move-forwarding-reference)
    , brotli(std::make_unique<BrotliStateWrapper>())
    , in_available(0)
    , in_data(nullptr)
    , out_capacity(0)
    , out_data(nullptr)
    , compress_empty(compress_empty_)
    {
        BrotliEncoderSetParameter(brotli->state, BROTLI_PARAM_QUALITY, static_cast<uint32_t>(compression_level));
        // Set LZ77 window size. According to brotli sources default value is 24 (c/tools/brotli.c:81)
        BrotliEncoderSetParameter(brotli->state, BROTLI_PARAM_LGWIN, 24);
    }

    ~BrotliWriteBuffer() override;

private:
    void nextImpl() override;

    void finalizeBefore() override;

    class BrotliStateWrapper
    {
    public:
        BrotliStateWrapper();
        ~BrotliStateWrapper();

        BrotliEncoderState * state;
    };

    std::unique_ptr<BrotliStateWrapper> brotli;


    size_t in_available;
    const uint8_t * in_data;

    size_t out_capacity;
    uint8_t * out_data;

protected:
    UInt64 total_in = 0;
    bool compress_empty = true;
};

}

#endif
