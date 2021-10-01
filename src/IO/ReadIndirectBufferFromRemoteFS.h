#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <IO/ReadBufferFromFile.h>
#include <Disks/IDiskRemote.h>
#include <utility>


namespace DB
{

class ReadBufferFromRemoteFSGather;

/// Reads data from S3/HDFS/Web using stored paths in metadata.
class ReadIndirectBufferFromRemoteFS : public ReadBufferFromFileBase
{

public:
    explicit ReadIndirectBufferFromRemoteFS(std::shared_ptr<ReadBufferFromRemoteFSGather> impl_);

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override;

    String getFileName() const override;

private:
    bool nextImpl() override;

    std::shared_ptr<ReadBufferFromRemoteFSGather> impl;
};

}
