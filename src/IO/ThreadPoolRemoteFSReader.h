#pragma once

#include <IO/AsynchronousReader.h>
#include <IO/SeekableReadBuffer.h>
#include <Common/ThreadPool.h>
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

    struct RemoteFSFileDescriptor : IFileDescriptor
    {
        ReadBufferPtr impl;
    };

};

}
