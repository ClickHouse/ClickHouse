#pragma once

#include <list>
#include <memory>

#include <Common/PODArray.h>

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionFactory.h>
#include <Common/ThreadPool.h>


namespace DB
{

/** Uses multi-buffering for parallel compression.
  * When the buffer is filled, it will be compressed in the background,
  * and a new buffer is created for the next input data.
  */
class ParallelCompressedWriteBuffer final : public WriteBuffer
{
public:
    explicit ParallelCompressedWriteBuffer(
        WriteBuffer & out_,
        CompressionCodecPtr codec_,
        size_t buf_size_,
        size_t num_threads_,
        ThreadPool & pool_);

    ~ParallelCompressedWriteBuffer() override;

    /// The amount of compressed data
    size_t getCompressedBytes()
    {
        nextIfAtEnd();
        return out.count();
    }

    /// How many uncompressed bytes were written to the buffer
    size_t getUncompressedBytes()
    {
        return count();
    }

    /// How many bytes are in the buffer (not yet compressed)
    size_t getRemainingBytes()
    {
        nextIfAtEnd();
        return offset();
    }

private:
    void nextImpl() override;
    void finalizeImpl() override;

    WriteBuffer & out;
    CompressionCodecPtr codec;
    size_t buf_size;
    size_t num_threads;
    ThreadPool & pool;

    struct BufferPair
    {
        BufferPair(size_t input_size)
            : uncompressed(input_size)
        {
        }

        Memory<> uncompressed;
        size_t uncompressed_size = 0;
        PODArray<char> compressed;
        const BufferPair * previous = nullptr;
        size_t sequence_num = 0;
        bool busy = false;
    };

    std::mutex mutex;
    std::condition_variable cond;
    std::list<BufferPair> buffers;

    using Iterator = std::list<BufferPair>::iterator;
    Iterator current_buffer;
    size_t current_sequence_num = 0;

    void compress(Iterator buffer);
};

}
