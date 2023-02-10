#pragma once

#include <IO/AsynchronousReader.h>
#include <IO/ReadBuffer.h>
#include <Common/ThreadPool.h>
#include <Interpreters/threadPoolCallbackRunner.h>

namespace DB
{

class ThreadPoolRemoteFSReader : public IAsynchronousReader
{
public:
    ThreadPoolRemoteFSReader(size_t pool_size, size_t queue_size_);

    std::future<IAsynchronousReader::Result> submit(Request request) override;

    void wait() override { pool.wait(); }

private:
    ThreadPool pool;
};

class RemoteFSFileDescriptor : public IAsynchronousReader::IFileDescriptor
{
public:
    explicit RemoteFSFileDescriptor(ReadBuffer & reader_) : reader(reader_) { }

    IAsynchronousReader::Result readInto(char * data, size_t size, size_t offset, size_t ignore = 0);

private:
    ReadBuffer & reader;
};

}
