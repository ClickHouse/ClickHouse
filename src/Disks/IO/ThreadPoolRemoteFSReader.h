#pragma once

#include <IO/AsynchronousReader.h>
#include <IO/ReadBuffer.h>
#include <Common/ThreadPool.h>

namespace DB
{

class ThreadPoolRemoteFSReader : public IAsynchronousReader
{
public:
    ThreadPoolRemoteFSReader(size_t pool_size, size_t queue_size_);

    std::future<IAsynchronousReader::Result> submit(Request request) override;

private:
    ThreadPool pool;
};

class RemoteFSFileDescriptor : public IAsynchronousReader::IFileDescriptor
{
public:
    explicit RemoteFSFileDescriptor(ReadBufferPtr reader_) : reader(std::move(reader_)) { }

    IAsynchronousReader::Result readInto(char * data, size_t size, size_t offset, size_t ignore = 0);

private:
    ReadBufferPtr reader;
};

}
