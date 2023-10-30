#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBufferDecorator.h>

#include "config.h"

#if USE_BZIP2
#    include <bzlib.h>

namespace DB
{

class Bzip2WriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    template<typename WriteBufferT>
    Bzip2WriteBuffer(
        WriteBufferT && out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        bool compress_empty_ = true)
    : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment), bz(std::make_unique<Bzip2StateWrapper>(compression_level))
    , compress_empty(compress_empty_)
    {
    }

    ~Bzip2WriteBuffer() override;

private:
    void nextImpl() override;

    void finalizeBefore() override;

    class Bzip2StateWrapper
    {
    public:
        explicit Bzip2StateWrapper(int compression_level);
        ~Bzip2StateWrapper();

        bz_stream stream;
    };

    std::unique_ptr<Bzip2StateWrapper> bz;
    bool compress_empty = true;
    UInt64 total_in = 0;
};

}

#endif
