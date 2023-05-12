#pragma once

#include <Common/config.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDecorator.h>

namespace DB
{

using CreateMetadataCallback = std::function<void(size_t bytes_count)>;

/// Stores data in S3/HDFS and adds the object path and object size to metadata file on local FS.
class WriteIndirectBufferFromRemoteFS final : public WriteBufferFromFileDecorator
{
public:
    WriteIndirectBufferFromRemoteFS(
        std::unique_ptr<WriteBuffer> impl_,
        CreateMetadataCallback && create_callback_,
        const String & remote_path_);

    ~WriteIndirectBufferFromRemoteFS() override;

    String getFileName() const override { return remote_path; }

private:
    void finalizeImpl() override;

    CreateMetadataCallback create_metadata_callback;
    String remote_path;
};

}
