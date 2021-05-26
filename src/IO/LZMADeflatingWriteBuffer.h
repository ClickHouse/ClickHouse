#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>

#if !defined(ARCADIA_BUILD)
    #include <lzma.h> // Y_IGNORE
#endif


namespace DB
{

#if !defined(ARCADIA_BUILD)

/// Performs compression using lzma library and writes compressed data to out_ WriteBuffer.
class LZMADeflatingWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    LZMADeflatingWriteBuffer(
        std::unique_ptr<WriteBuffer> out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    void finalize() override { finish(); }

    ~LZMADeflatingWriteBuffer() override;

private:
    void nextImpl() override;

    void finish();
    void finishImpl();

    std::unique_ptr<WriteBuffer> out;
    lzma_stream lstr;
    bool finished = false;
};

#else

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class LZMADeflatingWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    LZMADeflatingWriteBuffer(
        std::unique_ptr<WriteBuffer> out_ [[maybe_unused]],
        int compression_level [[maybe_unused]],
        size_t buf_size [[maybe_unused]] = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory [[maybe_unused]] = nullptr,
        size_t alignment [[maybe_unused]] = 0)
    {
        throw Exception("LZMADeflatingWriteBuffer is not implemented for arcadia build", ErrorCodes::NOT_IMPLEMENTED);
    }
};

#endif
}
