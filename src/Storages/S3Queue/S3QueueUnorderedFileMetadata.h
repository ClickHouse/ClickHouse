#pragma once
#include "S3QueueIFileMetadata.h"
#include <filesystem>
#include <Common/logger_useful.h>

namespace DB
{

class UnorderedFileMetadata : public IFileMetadata
{
public:
    using Bucket = size_t;

    explicit UnorderedFileMetadata(
        const std::filesystem::path & zk_path,
        const std::string & path_,
        FileStatusPtr file_status_,
        size_t max_loading_retries_,
        LoggerPtr log_);

private:
    std::pair<bool, FileStatus::State> setProcessingImpl() override;
    void setProcessedImpl() override;
};

}
