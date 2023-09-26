#pragma once
#include <boost/noncopyable.hpp>
#include <Storages/S3Queue/S3QueueSettings.h>
#include <Storages/S3Queue/S3QueueFilesMetadata.h>

namespace DB
{

class S3QueueMetadataFactory final : private boost::noncopyable
{
public:
    using MetadataPtr = std::shared_ptr<S3QueueFilesMetadata>;
    using MetadataByPath = std::unordered_map<std::string, MetadataPtr>;

    static S3QueueMetadataFactory & instance();

    MetadataPtr getOrCreate(const std::string & zookeeper_path, const S3QueueSettings & settings);

    MetadataByPath getAll() { return metadata_by_path; }

private:
    MetadataByPath metadata_by_path;
    std::mutex mutex;
};

}
