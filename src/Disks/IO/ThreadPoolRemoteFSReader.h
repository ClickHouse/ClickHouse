#pragma once

#include <IO/AsynchronousReader.h>
#include <Common/ThreadPool.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>

namespace DB
{

template <class Reader>
class ThreadPoolRemoteFSReader : public IAsynchronousReader
{
public:
    using ReadResult = IAsynchronousReader::Result;

    ThreadPoolRemoteFSReader(size_t pool_size, size_t queue_size_);

    std::future<Result> submit(Request request) override;

private:
    ThreadPool pool;
};


template <class Reader>
class RemoteFSFileDescriptor : public IAsynchronousReader::IFileDescriptor
{
public:
    using ReadResult = IAsynchronousReader::Result;

    explicit RemoteFSFileDescriptor(std::shared_ptr<Reader> reader_) : reader(std::move(reader_)) { }

    ReadResult readInto(char * data, size_t size, size_t offset, size_t ignore = 0);

private:
    std::shared_ptr<Reader> reader;
};

}
