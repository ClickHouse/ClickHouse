#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <IO/ReadBufferFromFile.h>
#include <Disks/IDiskRemote.h>
#include <utility>


namespace DB
{

/// Reads data from S3/HDFS using stored paths in metadata.
template <typename T>
class ReadIndirectBufferFromRemoteFS : public ReadBufferFromFileBase
{
public:
    explicit ReadIndirectBufferFromRemoteFS(RemoteMetadata metadata_);

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override { return absolute_position - available(); }

    String getFileName() const override { return metadata.metadata_file_path; }

    virtual std::unique_ptr<T> createReadBuffer(const String & path) = 0;

protected:
    RemoteMetadata metadata;

private:
    std::unique_ptr<T> initialize();

    bool nextAndShiftPosition();

    bool nextImpl() override;

    size_t absolute_position = 0;

    size_t current_buf_idx = 0;

    std::unique_ptr<T> current_buf;
};

}
