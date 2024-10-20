#include <city.h>
#include <iostream>

#include <base/types.h>
#include <base/defines.h>
#include <base/getThreadId.h>

#include <IO/WriteHelpers.h>
#include <Common/setThreadName.h>
#include <Common/scope_guard_safe.h>
#include <Common/CurrentThread.h>
#include <Common/CurrentMetrics.h>

#include <Compression/ParallelCompressedWriteBuffer.h>


namespace CurrentMetrics
{
    extern const Metric ParallelCompressedWriteBufferThreads;
    extern const Metric ParallelCompressedWriteBufferWait;
}

namespace DB
{

ParallelCompressedWriteBuffer::ParallelCompressedWriteBuffer(
    WriteBuffer & out_,
    CompressionCodecPtr codec_,
    size_t buf_size_,
    size_t num_threads_,
    ThreadPool & pool_)
    : WriteBuffer(nullptr, 0), out(out_), codec(codec_), buf_size(buf_size_), num_threads(num_threads_), pool(pool_)
{
    std::cerr << getThreadId() << " Create a new buffer 1\n";
    buffers.emplace_back(buf_size);
    current_buffer = buffers.begin();
    BufferBase::set(current_buffer->uncompressed.data(), buf_size, 0);
}

void ParallelCompressedWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    std::unique_lock lock(mutex);

    /// The buffer will be compressed and processed in the thread.
    current_buffer->busy = true;
    current_buffer->sequence_num = current_sequence_num;
    ++current_sequence_num;
    current_buffer->uncompressed_size = offset();
    pool.trySchedule([this, my_current_buffer = current_buffer, thread_group = CurrentThread::getGroup()]
    {
        SCOPE_EXIT_SAFE(
            if (thread_group)
                CurrentThread::detachFromGroupIfNotDetached();
        );

        if (thread_group)
            CurrentThread::attachToGroupIfDetached(thread_group);
        setThreadName("ParallelCompres");

        compress(my_current_buffer);
    });

    const BufferPair * previous_buffer = &*current_buffer;
    ++current_buffer;
    if (current_buffer == buffers.end())
    {
        if (buffers.size() < num_threads)
        {
            /// If we didn't use all num_threads buffers yet, create a new one.
            std::cerr << getThreadId() << " Create a new buffer " << (buffers.size() + 1) << "\n";
            current_buffer = buffers.emplace(current_buffer, buf_size);
        }
        else
        {
            /// Otherwise, wrap around to the first buffer in the list.
            std::cerr << getThreadId() << " Wrap around\n";
            current_buffer = buffers.begin();
        }
    }

    if (current_buffer->busy)
        std::cerr << getThreadId() << " Wait while the buffer " << current_buffer->sequence_num << " becomes not busy\n";
    /// Wait while the buffer becomes not busy
    {
        CurrentMetrics::Increment metric_increment(CurrentMetrics::ParallelCompressedWriteBufferWait);
        cond.wait(lock, [&]{ return !current_buffer->busy; });
    }

    /// Now this buffer can be used.
    current_buffer->previous = previous_buffer;
    BufferBase::set(current_buffer->uncompressed.data(), buf_size, 0);
}

void ParallelCompressedWriteBuffer::finalizeImpl()
{
    next();
    pool.wait();
}

void ParallelCompressedWriteBuffer::compress(Iterator buffer)
{
    std::cerr << getThreadId() << " Compressing " << buffer->sequence_num << "...\n";
    CurrentMetrics::Increment metric_increment(CurrentMetrics::ParallelCompressedWriteBufferThreads);

    chassert(buffer->uncompressed_size <= INT_MAX);
    UInt32 uncompressed_size = static_cast<UInt32>(buffer->uncompressed_size);
    UInt32 compressed_reserve_size = codec->getCompressedReserveSize(uncompressed_size);

    buffer->compressed.resize(compressed_reserve_size);
    UInt32 compressed_size = codec->compress(buffer->uncompressed.data(), uncompressed_size, buffer->compressed.data());

    CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(buffer->compressed.data(), compressed_size);

    if (buffer->previous && buffer->previous->busy)
        std::cerr << getThreadId() << " Compressed " << buffer->sequence_num << ", waiting for prev buffer to be written...\n";
    /// Wait while all previous buffers have been written.
    {
        CurrentMetrics::Increment metric_wait_increment(CurrentMetrics::ParallelCompressedWriteBufferWait);
        std::unique_lock lock(mutex);
        cond.wait(lock, [&]{ return !buffer->previous || !buffer->previous->busy || buffer->previous->sequence_num > buffer->sequence_num; });
    }

    std::cerr << getThreadId() << " Writing " << buffer->sequence_num << "...\n";

    writeBinaryLittleEndian(checksum.low64, out);
    writeBinaryLittleEndian(checksum.high64, out);

    out.write(buffer->compressed.data(), compressed_size);

    std::unique_lock lock(mutex);
    buffer->busy = false;
    cond.notify_all();
}

ParallelCompressedWriteBuffer::~ParallelCompressedWriteBuffer()
{
    if (!canceled)
        finalize();
}

}
