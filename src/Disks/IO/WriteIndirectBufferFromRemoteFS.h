#pragma once

#include <Common/config.h>

#include <Disks/IDiskRemote.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDecorator.h>

namespace DB
{

/// Stores data in S3/HDFS and adds the object path and object size to metadata file on local FS.
template <typename T>
class WriteIndirectBufferFromRemoteFS final : public WriteBufferFromFileDecorator
{
public:
    WriteIndirectBufferFromRemoteFS(
        std::unique_ptr<T> impl_,
        IDiskRemote::Metadata metadata_,
        const String & remote_fs_path_);

    virtual ~WriteIndirectBufferFromRemoteFS() override;

    void finalize() override;

    void sync() override;

    String getFileName() const override { return metadata.metadata_file_path; }

private:
    IDiskRemote::Metadata metadata;

    String remote_fs_path;
};

}
