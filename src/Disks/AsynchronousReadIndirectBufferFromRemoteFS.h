#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <IO/ReadBufferFromFile.h>
#include <IO/AsynchronousReader.h>
#include <IO/ReadBufferFromRemoteFS.h>
#include <Disks/IDiskRemote.h>
#include <utility>


namespace DB
{

/// Reads data from S3/HDFS/Web using stored paths in metadata.
class AsynchronousReadIndirectBufferFromRemoteFS : public ReadBufferFromFileBase
{
public:
    explicit AsynchronousReadIndirectBufferFromRemoteFS(
        AsynchronousReaderPtr reader_, Int32 priority_, ReadBufferFromRemoteFSImpl impl_);

    ~AsynchronousReadIndirectBufferFromRemoteFS() override;

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override { return impl->absolute_position - available(); }

    String getFileName() const override { return impl->getFileName(); }

    void prefetch() override;

private:
    bool nextImpl() override;

    void finalize();

    std::future<IAsynchronousReader::Result> readNext();

    AsynchronousReaderPtr reader;
    Int32 priority;
    ReadBufferFromRemoteFSImpl impl;
    std::future<IAsynchronousReader::Result> prefetch_future;
};

}
