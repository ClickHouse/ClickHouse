#include <Common/config.h>

#if USE_SNAPPY
#include <fcntl.h>
#include <sys/types.h>
#include <memory>
#include <string>
#include <cstring>

#include <snappy-c.h>

#include "HadoopSnappyReadBuffer.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int SNAPPY_UNCOMPRESS_FAILED;
}


inline bool HadoopSnappyDecoder::checkBufferLength(int max) const
{
    return buffer_length >= 0 && buffer_length < max;
}

inline bool HadoopSnappyDecoder::checkAvailIn(size_t avail_in, int min)
{
    return avail_in >= static_cast<size_t>(min);
}

inline void HadoopSnappyDecoder::copyToBuffer(size_t * avail_in, const char ** next_in)
{
    assert(*avail_in + buffer_length <= sizeof(buffer));

    memcpy(buffer + buffer_length, *next_in, *avail_in);

    buffer_length += *avail_in;
    *next_in += *avail_in;
    *avail_in = 0;
}


inline uint32_t HadoopSnappyDecoder::readLength(const char * in)
{
    uint32_t b1 = *(reinterpret_cast<const uint8_t *>(in));
    uint32_t b2 = *(reinterpret_cast<const uint8_t *>(in + 1));
    uint32_t b3 = *(reinterpret_cast<const uint8_t *>(in + 2));
    uint32_t b4 = *(reinterpret_cast<const uint8_t *>(in + 3));
    uint32_t res = ((b1 << 24) + (b2 << 16) + (b3 << 8) + b4);
    return res;
}


inline HadoopSnappyDecoder::Status HadoopSnappyDecoder::readLength(size_t * avail_in, const char ** next_in, int * length)
{
    char tmp[4] = {0};

    if (!checkBufferLength(4))
        return Status::INVALID_INPUT;
    memcpy(tmp, buffer, buffer_length);

    if (!checkAvailIn(*avail_in, 4 - buffer_length))
    {
        copyToBuffer(avail_in, next_in);
        return Status::NEEDS_MORE_INPUT;
    }
    memcpy(tmp + buffer_length, *next_in, 4 - buffer_length);

    *avail_in -= 4 - buffer_length;
    *next_in += 4 - buffer_length;
    buffer_length = 0;
    *length = readLength(tmp);
    return Status::OK;
}

inline HadoopSnappyDecoder::Status HadoopSnappyDecoder::readBlockLength(size_t * avail_in, const char ** next_in)
{
    if (block_length < 0)
    {
        return readLength(avail_in, next_in, &block_length);
    }
    return Status::OK;
}

inline HadoopSnappyDecoder::Status HadoopSnappyDecoder::readCompressedLength(size_t * avail_in, const char ** next_in)
{
    if (compressed_length < 0)
    {
        auto status = readLength(avail_in, next_in, &compressed_length);
        if (unlikely(compressed_length > 0 && static_cast<size_t>(compressed_length) > sizeof(buffer)))
            throw Exception(ErrorCodes::SNAPPY_UNCOMPRESS_FAILED, "Too large snappy compressed block. buffer size: {}, compressed block size: {}", sizeof(buffer), compressed_length);
        return status;
    }
    return Status::OK;
}

inline HadoopSnappyDecoder::Status
HadoopSnappyDecoder::readCompressedData(size_t * avail_in, const char ** next_in, size_t * avail_out, char ** next_out)
{
    if (!checkBufferLength(compressed_length))
        return Status::INVALID_INPUT;

    if (!checkAvailIn(*avail_in, compressed_length - buffer_length))
    {
        copyToBuffer(avail_in, next_in);
        return Status::NEEDS_MORE_INPUT;
    }

    const char * compressed = nullptr;
    if (buffer_length > 0)
    {
        compressed = buffer;
        memcpy(buffer + buffer_length, *next_in, compressed_length - buffer_length);
    }
    else
    {
        compressed = const_cast<char *>(*next_in);
    }
    size_t uncompressed_length = *avail_out;
    auto status = snappy_uncompress(compressed, compressed_length, *next_out, &uncompressed_length);
    if (status != SNAPPY_OK)
    {
        return Status(status);
    }

    *avail_in -= compressed_length - buffer_length;
    *next_in += compressed_length - buffer_length;
    *avail_out -= uncompressed_length;
    *next_out += uncompressed_length;

    total_uncompressed_length += uncompressed_length;
    compressed_length = -1;
    buffer_length = 0;
    return Status::OK;
}

HadoopSnappyDecoder::Status HadoopSnappyDecoder::readBlock(size_t * avail_in, const char ** next_in, size_t * avail_out, char ** next_out)
{
    if (*avail_in == 0)
    {
        if (buffer_length == 0 && block_length < 0 && compressed_length < 0)
            return Status::OK;
        return Status::NEEDS_MORE_INPUT;
    }

    HadoopSnappyDecoder::Status status = readBlockLength(avail_in, next_in);
    if (status != Status::OK)
        return status;

    while (total_uncompressed_length < block_length)
    {
        status = readCompressedLength(avail_in, next_in);
        if (status != Status::OK)
            return status;

        status = readCompressedData(avail_in, next_in, avail_out, next_out);
        if (status != Status::OK)
            return status;
    }
    if (total_uncompressed_length != block_length)
    {
        return Status::INVALID_INPUT;
    }
    return Status::OK;
}

HadoopSnappyReadBuffer::HadoopSnappyReadBuffer(std::unique_ptr<ReadBuffer> in_, size_t buf_size, char * existing_memory, size_t alignment)
    : CompressedReadBufferWrapper(std::move(in_), buf_size, existing_memory, alignment)
    , decoder(std::make_unique<HadoopSnappyDecoder>())
    , in_available(0)
    , in_data(nullptr)
    , out_capacity(0)
    , out_data(nullptr)
    , eof(false)
{
}

HadoopSnappyReadBuffer::~HadoopSnappyReadBuffer() = default;

bool HadoopSnappyReadBuffer::nextImpl()
{
    if (eof)
        return false;

    if (!in_available)
    {
        in->nextIfAtEnd();
        in_available = in->buffer().end() - in->position();
        in_data = in->position();
    }

    if (decoder->result == Status::NEEDS_MORE_INPUT && (!in_available || in->eof()))
    {
        throw Exception(String("hadoop snappy decode error:") + statusToString(decoder->result), ErrorCodes::SNAPPY_UNCOMPRESS_FAILED);
    }

    out_capacity = internal_buffer.size();
    out_data = internal_buffer.begin();
    decoder->result = decoder->readBlock(&in_available, &in_data, &out_capacity, &out_data);

    in->position() = in->buffer().end() - in_available;
    working_buffer.resize(internal_buffer.size() - out_capacity);

    if (decoder->result == Status::OK)
    {
        decoder->reset();
        if (in->eof())
        {
            eof = true;
            return !working_buffer.empty();
        }
        return true;
    }
    else if (decoder->result == Status::INVALID_INPUT || decoder->result == Status::BUFFER_TOO_SMALL)
    {
        throw Exception(String("hadoop snappy decode error:") + statusToString(decoder->result), ErrorCodes::SNAPPY_UNCOMPRESS_FAILED);
    }
    return true;
}

}

#endif
