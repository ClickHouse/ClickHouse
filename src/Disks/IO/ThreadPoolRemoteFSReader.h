#pragma once

#include <IO/AsynchronousReader.h>
#include <IO/SeekableReadBuffer.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/threadPoolCallbackRunner.h>

namespace DB
{

struct AsyncReadCounters;

class ThreadPoolRemoteFSReader : public IAsynchronousReader
{
public:
    ThreadPoolRemoteFSReader(size_t pool_size, size_t queue_size_);

    std::future<IAsynchronousReader::Result> submit(Request request) override;
    IAsynchronousReader::Result execute(Request request) override;

    void wait() override;

private:
    IAsynchronousReader::Result execute(Request request, bool seek_performed);

    std::unique_ptr<ThreadPool> pool;
};

class RemoteFSFileDescriptor : public IAsynchronousReader::IFileDescriptor
{
public:
    /// `reader_` implementation must ensure that next() places data at the start of internal_buffer,
    /// even if there was previously a seek. I.e. seek() shouldn't leave pending data (no short seek
    /// optimization), and nextImpl() shouldn't assign nextimpl_working_buffer_offset.
    explicit RemoteFSFileDescriptor(
        SeekableReadBuffer & reader_,
        std::shared_ptr<AsyncReadCounters> async_read_counters_)
        : reader(reader_)
        , async_read_counters(async_read_counters_) {}

    SeekableReadBuffer & getReader() { return reader; }

    std::shared_ptr<AsyncReadCounters> getReadCounters() const { return async_read_counters; }

private:
    /// Reader is used for reading only by RemoteFSFileDescriptor.
    SeekableReadBuffer & reader;
    std::shared_ptr<AsyncReadCounters> async_read_counters;
};

}
