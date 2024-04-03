#pragma once

#include "config.h"

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDecorator.h>

namespace DB
{

using FinalizeCallback = std::function<void(size_t bytes_count)>;

/// Stores data in S3/HDFS and adds the object path and object size to metadata file on local FS.
class WriteBufferWithFinalizeCallback final : public WriteBufferFromFileDecorator
{
public:
    WriteBufferWithFinalizeCallback(
        std::unique_ptr<WriteBuffer> impl_,
        FinalizeCallback && create_callback_,
        const String & remote_path_);

    String getFileName() const override { return remote_path; }

private:
    void finalizeImpl() override;

    FinalizeCallback create_metadata_callback;
    String remote_path;
};

}
