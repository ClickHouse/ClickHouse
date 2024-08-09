#pragma once
#include <boost/noncopyable.hpp>
#include <Storages/S3Queue/S3QueueSettings.h>
#include <Storages/S3Queue/S3QueueFilesMetadata.h>

namespace DB
{

class S3QueueMetadataFactory final : private boost::noncopyable
{
public:
    using FilesMetadataPtr = std::shared_ptr<S3QueueFilesMetadata>;

    static S3QueueMetadataFactory & instance();

    FilesMetadataPtr getOrCreate(const std::string & zookeeper_path, const S3QueueSettings & settings);

    void remove(const std::string & zookeeper_path);

    std::unordered_map<std::string, FilesMetadataPtr> getAll();

private:
    struct Metadata
    {
        explicit Metadata(std::shared_ptr<S3QueueFilesMetadata> metadata_) : metadata(metadata_), ref_count(1) {}

        std::shared_ptr<S3QueueFilesMetadata> metadata;
        /// TODO: the ref count should be kept in keeper, because of the case with distributed processing.
        size_t ref_count = 0;
    };
    using MetadataByPath = std::unordered_map<std::string, Metadata>;

    MetadataByPath metadata_by_path;
    std::mutex mutex;
};

}
