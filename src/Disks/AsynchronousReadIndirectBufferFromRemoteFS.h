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

/// Reads data from S3/HDFS using stored paths in metadata.
class AsynchronousReadIndirectBufferFromRemoteFS : public ReadBufferFromFileBase
{
public:
    explicit AsynchronousReadIndirectBufferFromRemoteFS(
        AsynchronousReaderPtr reader_, Int32 priority_, ReadBufferFromRemoteFSImpl impl_);

    ~AsynchronousReadIndirectBufferFromRemoteFS() override;

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override { return absolute_position - available(); }

    String getFileName() const override { return metadata_file_path; }

    void prefetch() override;

private:
    bool nextImpl() override;

    void finalize();

    std::future<IAsynchronousReader::Result> read();

    String metadata_file_path;
    size_t absolute_position = 0;

    AsynchronousReaderPtr reader;
    Int32 priority;
    ReadBufferFromRemoteFSImpl impl;

    std::future<IAsynchronousReader::Result> prefetch_future;
};

}
