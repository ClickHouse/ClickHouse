#pragma once

#include <IO/AsynchronousReader.h>
#include <IO/SeekableReadBuffer.h>
#include <Common/ThreadPool.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IDiskRemote.h>


namespace DB
{

class ThreadPoolRemoteFSReader : public IAsynchronousReader
{

private:
    ThreadPool pool;

public:
    ThreadPoolRemoteFSReader(size_t pool_size, size_t queue_size_);

    std::future<Result> submit(Request request) override;

    struct RemoteFSFileDescriptor;
};


struct ThreadPoolRemoteFSReader::RemoteFSFileDescriptor : public IFileDescriptor
{
public:
    explicit RemoteFSFileDescriptor(std::shared_ptr<ReadBufferFromRemoteFSGather> reader_) : reader(reader_) {}

    ReadBufferFromRemoteFSGather::ReadResult readInto(char * data, size_t size, size_t offset, size_t ignore = 0);

private:
    std::shared_ptr<ReadBufferFromRemoteFSGather> reader;
};

}
