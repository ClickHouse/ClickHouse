#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromRemoteFS.h>
#include <Disks/IDiskRemote.h>
#include <utility>


namespace DB
{

/// Reads data from S3/HDFS/Web using stored paths in metadata.
class ReadIndirectBufferFromRemoteFS : public ReadBufferFromFileBase
{
public:
    explicit ReadIndirectBufferFromRemoteFS(ReadBufferFromRemoteFSImpl impl_);

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override { return absolute_position - available(); }

    String getFileName() const override { return impl->getFileName(); }

private:
    bool nextImpl() override;

    ReadBufferFromRemoteFSImpl impl;

    size_t absolute_position = 0;
};

}
