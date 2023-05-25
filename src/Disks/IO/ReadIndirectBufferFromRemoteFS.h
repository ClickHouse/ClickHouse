#pragma once

#include "config.h"
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <utility>


namespace DB
{

class ReadBufferFromRemoteFSGather;

/**
* Reads data from S3/HDFS/Web using stored paths in metadata.
* There is asynchronous version of this class -- AsynchronousReadIndirectBufferFromRemoteFS.
*/
class ReadIndirectBufferFromRemoteFS : public ReadBufferFromFileBase
{

public:
    explicit ReadIndirectBufferFromRemoteFS(std::shared_ptr<ReadBufferFromRemoteFSGather> impl_, const ReadSettings & settings);

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override;

    String getFileName() const override;

    void setReadUntilPosition(size_t position) override;

    void setReadUntilEnd() override;

    size_t getFileSize() override;

private:
    bool nextImpl() override;

    std::shared_ptr<ReadBufferFromRemoteFSGather> impl;

    ReadSettings read_settings;

    size_t file_offset_of_buffer_end = 0;
};

}
