#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>

#if !defined(ARCADIA_BUILD)
    #include <lzma.h> // Y_IGNORE
#endif

namespace DB
{

#if !defined(ARCADIA_BUILD)
class LZMAInflatingReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    LZMAInflatingReadBuffer(
        std::unique_ptr<ReadBuffer> in_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~LZMAInflatingReadBuffer() override;

private:
    bool nextImpl() override;

    std::unique_ptr<ReadBuffer> in;
    lzma_stream lstr;

    bool eof;
};

#else

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class LZMAInflatingReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    LZMAInflatingReadBuffer(
            std::unique_ptr<ReadBuffer> in_ [[maybe_unused]],
            size_t buf_size [[maybe_unused]] = DBMS_DEFAULT_BUFFER_SIZE,
            char * existing_memory [[maybe_unused]] = nullptr,
            size_t alignment [[maybe_unused]] = 0)
    {
        throw Exception("LZMADeflatingWriteBuffer is not implemented for arcadia build", ErrorCodes::NOT_IMPLEMENTED);
    }
};

#endif
}
