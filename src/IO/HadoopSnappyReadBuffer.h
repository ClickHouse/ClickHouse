#pragma once

#include <Common/config.h>

#if USE_SNAPPY

#include <memory>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{
class HadoopSnappyDecoder
{
public:
    enum class Status : int
    {
        OK = 0,
        INVALID_INPUT = 1,
        BUFFER_TOO_SMALL = 2,
        NEEDS_MORE_INPUT = 3,
    };

    HadoopSnappyDecoder() = default;
    ~HadoopSnappyDecoder() = default;

    Status readBlock(size_t * avail_in, const char ** next_in, size_t * avail_out, char ** next_out);

    inline void reset()
    {
        buffer_length = 0;
        block_length = -1;
        compressed_length = -1;
        total_uncompressed_length = 0;
    }

    Status result;

private:
    inline bool checkBufferLength(int max) const;
    inline static bool checkAvailIn(size_t avail_in, int min);

    inline void copyToBuffer(size_t * avail_in, const char ** next_in);

    inline static uint32_t readLength(const char * in);
    inline Status readLength(size_t * avail_in, const char ** next_in, int * length);
    inline Status readBlockLength(size_t * avail_in, const char ** next_in);
    inline Status readCompressedLength(size_t * avail_in, const char ** next_in);
    inline Status readCompressedData(size_t * avail_in, const char ** next_in, size_t * avail_out, char ** next_out);

    char buffer[DBMS_DEFAULT_BUFFER_SIZE] = {0};
    int buffer_length = 0;

    int block_length = -1;
    int compressed_length = -1;
    int total_uncompressed_length = 0;
};


class HadoopSnappyReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    using Status = HadoopSnappyDecoder::Status;

    inline static String statusToString(Status status)
    {
        switch (status)
        {
            case Status::OK:
                return "OK";
            case Status::INVALID_INPUT:
                return "INVALID_INPUT";
            case Status::BUFFER_TOO_SMALL:
                return "BUFFER_TOO_SMALL";
            case Status::NEEDS_MORE_INPUT:
                return "NEEDS_MORE_INPUT";
        }
        __builtin_unreachable();
    }

    explicit HadoopSnappyReadBuffer(
        std::unique_ptr<ReadBuffer> in_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~HadoopSnappyReadBuffer() override;

private:
    bool nextImpl() override;

    std::unique_ptr<ReadBuffer> in;
    std::unique_ptr<HadoopSnappyDecoder> decoder;

    size_t in_available;
    const char * in_data;

    size_t out_capacity;
    char * out_data;

    bool eof;
};

}
#endif
