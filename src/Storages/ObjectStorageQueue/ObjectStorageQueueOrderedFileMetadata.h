#pragma once
#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <filesystem>

namespace DB
{

class ObjectStorageQueueOrderedFileMetadata : public ObjectStorageQueueIFileMetadata
{
public:
    explicit ObjectStorageQueueOrderedFileMetadata(
        const std::filesystem::path & zk_path_,
        const std::string & path_,
        FileStatusPtr file_status_,
        BucketInfoPtr bucket_info_,
        size_t buckets_num_,
        size_t max_loading_retries_,
        LoggerPtr log_);

    static std::vector<std::string> getMetadataPaths(size_t buckets_num_);

    static void migrateToBuckets(const std::string & zk_path_, size_t value);

    void prepareProcessedAtStartRequests(
        Coordination::Requests & requests,
        const zkutil::ZooKeeperPtr & zk_client) override;

private:
    std::pair<bool, FileStatus::State> setProcessingImpl() override;

    void prepareProcessedRequestsImpl(Coordination::Requests & requests) override;

    bool getMaxProcessedFile(
        NodeMetadata & result,
        Coordination::Stat * stat,
        const zkutil::ZooKeeperPtr & zk_client);

    static bool getMaxProcessedFile(
        NodeMetadata & result,
        Coordination::Stat * stat,
        const std::string & processed_node_path_,
        const zkutil::ZooKeeperPtr & zk_client);

    void prepareProcessedRequests(
        Coordination::Requests & requests,
        const zkutil::ZooKeeperPtr & zk_client,
        const std::string & processed_node_path_,
        bool ignore_if_exists);
};

}
