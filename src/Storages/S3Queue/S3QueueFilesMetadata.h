#pragma once
#include "config.h"

#if USE_AWS_S3
#include <filesystem>
#include <Core/Types.h>
#include <Core/SettingsEnums.h>

namespace fs = std::filesystem;
namespace Poco { class Logger; }

namespace DB
{
struct S3QueueSettings;
class StorageS3Queue;

class S3QueueFilesMetadata
{
public:
    S3QueueFilesMetadata(const StorageS3Queue * storage_, const S3QueueSettings & settings_);

    bool trySetFileAsProcessing(const std::string & path);

    void setFileProcessed(const std::string & path);

    void setFileFailed(const std::string & path, const std::string & exception_message);

private:
    const StorageS3Queue * storage;
    const S3QueueMode mode;
    const UInt64 max_set_size;
    const UInt64 max_set_age_sec;
    const UInt64 max_loading_retries;

    const fs::path zookeeper_processing_path;
    const fs::path zookeeper_processed_path;
    const fs::path zookeeper_failed_path;

    mutable std::mutex mutex;
    Poco::Logger * log;

    bool trySetFileAsProcessingForOrderedMode(const std::string & path);
    bool trySetFileAsProcessingForUnorderedMode(const std::string & path);

    void setFileProcessedForOrderedMode(const std::string & path);
    void setFileProcessedForUnorderedMode(const std::string & path);

    std::string getNodeName(const std::string & path);

    struct NodeMetadata
    {
        std::string file_path;
        UInt64 last_processed_timestamp = 0;
        std::string last_exception;
        UInt64 retries = 0;

        std::string toString() const;
        static NodeMetadata fromString(const std::string & metadata_str);
    };

    NodeMetadata createNodeMetadata(const std::string & path, const std::string & exception = "", size_t retries = 0);
};

}

#endif
