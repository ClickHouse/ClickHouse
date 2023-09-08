#include "config.h"

#if USE_AWS_S3
#include <base/sleep.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Storages/S3Queue/S3QueueFilesMetadata.h>
#include <Storages/S3Queue/StorageS3Queue.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/StorageSnapshot.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    UInt64 getCurrentTime()
    {
        return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    }
}

S3QueueFilesMetadata::S3QueueFilesMetadata(
    const StorageS3Queue * storage_,
    const S3QueueSettings & settings_)
    : storage(storage_)
    , mode(settings_.mode)
    , max_set_size(settings_.s3queue_tracked_files_limit.value)
    , max_set_age_sec(settings_.s3queue_tracked_file_ttl_sec.value)
    , max_loading_retries(settings_.s3queue_loading_retries.value)
    , zookeeper_processing_path(storage->getZooKeeperPath() / "processing")
    , zookeeper_processed_path(storage->getZooKeeperPath() / "processed")
    , zookeeper_failed_path(storage->getZooKeeperPath() / "failed")
    , log(&Poco::Logger::get("S3QueueFilesMetadata"))
{
}

std::string S3QueueFilesMetadata::NodeMetadata::toString() const
{
    Poco::JSON::Object json;
    json.set("file_path", file_path);
    json.set("last_processed_timestamp", getCurrentTime());
    json.set("last_exception", last_exception);
    json.set("retries", retries);

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
}

S3QueueFilesMetadata::NodeMetadata S3QueueFilesMetadata::NodeMetadata::fromString(const std::string & metadata_str)
{
    Poco::JSON::Parser parser;
    auto json = parser.parse(metadata_str).extract<Poco::JSON::Object::Ptr>();

    NodeMetadata metadata;
    metadata.file_path = json->getValue<String>("file_path");
    metadata.last_processed_timestamp = json->getValue<UInt64>("last_processed_timestamp");
    metadata.last_exception = json->getValue<String>("last_exception");
    metadata.retries = json->getValue<UInt64>("retries");
    return metadata;
}

std::string S3QueueFilesMetadata::getNodeName(const std::string & path)
{
    SipHash path_hash;
    path_hash.update(path);
    return toString(path_hash.get64());
}

S3QueueFilesMetadata::NodeMetadata S3QueueFilesMetadata::createNodeMetadata(
    const std::string & path,
    const std::string & exception,
    size_t retries)
{
    NodeMetadata metadata;
    metadata.file_path = path;
    metadata.last_processed_timestamp = getCurrentTime();
    metadata.last_exception = exception;
    metadata.retries = retries;
    return metadata;
}

bool S3QueueFilesMetadata::trySetFileAsProcessing(const std::string & path)
{
    switch (mode)
    {
        case S3QueueMode::ORDERED:
        {
            return trySetFileAsProcessingForOrderedMode(path);
        }
        case S3QueueMode::UNORDERED:
        {
            return trySetFileAsProcessingForUnorderedMode(path);
        }
    }
}

bool S3QueueFilesMetadata::trySetFileAsProcessingForUnorderedMode(const std::string & path)
{
    const auto node_name = getNodeName(path);
    const auto node_metadata = createNodeMetadata(path).toString();
    const auto zk_client = storage->getZooKeeper();

    Coordination::Requests requests;
    zkutil::addCheckNotExistsRequest(requests, *zk_client, zookeeper_processed_path / node_name);
    zkutil::addCheckNotExistsRequest(requests, *zk_client, zookeeper_failed_path / node_name);
    requests.push_back(zkutil::makeCreateRequest(zookeeper_processing_path / node_name, node_metadata, zkutil::CreateMode::Ephemeral));

    Coordination::Responses responses;
    auto code = zk_client->tryMulti(requests, responses);
    return code == Coordination::Error::ZOK;
}

bool S3QueueFilesMetadata::trySetFileAsProcessingForOrderedMode(const std::string & path)
{
    const auto node_name = getNodeName(path);
    const auto node_metadata = createNodeMetadata(path).toString();
    const auto zk_client = storage->getZooKeeper();

    while (true)
    {
        Coordination::Requests requests;
        zkutil::addCheckNotExistsRequest(requests, *zk_client, zookeeper_failed_path / node_name);
        zkutil::addCheckNotExistsRequest(requests, *zk_client, zookeeper_processing_path / node_name);

        Coordination::Responses responses;
        auto code = zk_client->tryMulti(requests, responses);
        if (code != Coordination::Error::ZOK)
        {
            LOG_TEST(log, "Skipping file `{}`: {}",
                    path, responses[0]->error != Coordination::Error::ZOK ? "failed" : "processing");
            return false;
        }

        Coordination::Stat processed_node_stat;
        auto data = zk_client->get(zookeeper_processed_path, &processed_node_stat);
        NodeMetadata processed_node_metadata;
        if (!data.empty())
            processed_node_metadata = NodeMetadata::fromString(data);

        auto max_processed_file_path = processed_node_metadata.file_path;
        if (!max_processed_file_path.empty() && path <= max_processed_file_path)
            return false;

        requests.clear();
        responses.clear();
        zkutil::addCheckNotExistsRequest(requests, *zk_client, zookeeper_failed_path / node_name);
        requests.push_back(zkutil::makeCreateRequest(zookeeper_processing_path / node_name, node_metadata, zkutil::CreateMode::Ephemeral));
        requests.push_back(zkutil::makeCheckRequest(zookeeper_processed_path, processed_node_stat.version));

        code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
            return true;

        if (responses[0]->error != Coordination::Error::ZOK
            || responses[1]->error != Coordination::Error::ZOK)
        {
            LOG_TEST(log, "Skipping file `{}`: {}",
                    path, responses[0]->error != Coordination::Error::ZOK ? "failed" : "processing");
            return false;
        }
        else
        {
            LOG_TEST(log, "Version of max processed file changed. Retring the check for file `{}`", path);
        }
    }
}

void S3QueueFilesMetadata::setFileProcessed(const String & path)
{
    switch (mode)
    {
        case S3QueueMode::ORDERED:
        {
            return setFileProcessedForOrderedMode(path);
        }
        case S3QueueMode::UNORDERED:
        {
            return setFileProcessedForUnorderedMode(path);
        }
    }
}

void S3QueueFilesMetadata::setFileProcessedForUnorderedMode(const String & path)
{
    /// List results in s3 are always returned in UTF-8 binary order.
    /// (https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html)

    const auto node_name = getNodeName(path);
    const auto node_metadata = createNodeMetadata(path).toString();
    const auto zk_client = storage->getZooKeeper();

    Coordination::Requests requests;
    requests.push_back(zkutil::makeRemoveRequest(zookeeper_processing_path / node_name, -1));
    requests.push_back(zkutil::makeCreateRequest(zookeeper_processed_path / node_name, node_metadata, zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    auto code = zk_client->tryMulti(requests, responses);
    if (code == Coordination::Error::ZOK)
        return;

    /// TODO this could be because of the expired session.
    if (responses[0]->error != Coordination::Error::ZOK)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to set file as processed but it is not processing");
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to set file as processed but it is already processed");
}

void S3QueueFilesMetadata::setFileProcessedForOrderedMode(const String & path)
{
    const auto node_name = getNodeName(path);
    const auto node_metadata = createNodeMetadata(path).toString();
    const auto zk_client = storage->getZooKeeper();

    while (true)
    {
        std::string res;
        Coordination::Stat stat;
        bool exists = zk_client->tryGet(zookeeper_processed_path, res, &stat);
        Coordination::Requests requests;
        if (exists)
        {
            if (!res.empty())
            {
                auto metadata = NodeMetadata::fromString(res);
                if (metadata.file_path >= path)
                    return;
            }
            requests.push_back(zkutil::makeSetRequest(zookeeper_processed_path, node_metadata, stat.version));
        }
        else
        {
            requests.push_back(zkutil::makeCreateRequest(zookeeper_processed_path, node_metadata, zkutil::CreateMode::Persistent));
        }

        Coordination::Responses responses;
        auto code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
            return;
    }
}

void S3QueueFilesMetadata::setFileFailed(const String & path, const String & exception_message)
{
    const auto node_name = getNodeName(path);
    auto node_metadata = createNodeMetadata(path, exception_message);
    const auto zk_client = storage->getZooKeeper();

    if (max_loading_retries == 0)
    {
        Coordination::Requests requests;
        requests.push_back(zkutil::makeRemoveRequest(zookeeper_processing_path / node_name, -1));
        requests.push_back(zkutil::makeCreateRequest(zookeeper_failed_path / node_name,
                                                     node_metadata.toString(),
                                                     zkutil::CreateMode::Persistent));

        Coordination::Responses responses;
        auto code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
        {
            LOG_TEST(log, "File `{}` failed to process and will not be retried. "
                     "Error: {}", path, exception_message);
            return;
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set file as failed");
    }

    const auto node_name_with_retriable_suffix = node_name + ".retriable";

    Coordination::Stat stat;
    std::string res;
    if (zk_client->tryGet(zookeeper_failed_path / node_name_with_retriable_suffix, res, &stat))
    {
        auto failed_node_metadata = NodeMetadata::fromString(res);
        node_metadata.retries = failed_node_metadata.retries + 1;
    }

    LOG_TEST(log, "File `{}` failed to process, try {}/{} (Error: {})",
             path, node_metadata.retries, max_loading_retries, exception_message);

    if (node_metadata.retries >= max_loading_retries)
    {
        /// File is no longer retriable.
        /// Make a failed/node_name node and remove failed/node_name.retriable node.
        /// TODO always add version for processing node.

        Coordination::Requests requests;
        requests.push_back(zkutil::makeRemoveRequest(zookeeper_processing_path / node_name, -1));
        requests.push_back(zkutil::makeRemoveRequest(zookeeper_failed_path / node_name_with_retriable_suffix,
                                                     stat.version));
        requests.push_back(zkutil::makeCreateRequest(zookeeper_failed_path / node_name,
                                                     node_metadata.toString(),
                                                     zkutil::CreateMode::Persistent));

        Coordination::Responses responses;
        auto code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
            return;

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set file as failed");
    }
    else
    {
        Coordination::Requests requests;
        requests.push_back(zkutil::makeRemoveRequest(zookeeper_processing_path / node_name, -1));
        if (node_metadata.retries == 0)
        {
            requests.push_back(zkutil::makeCreateRequest(zookeeper_failed_path / node_name_with_retriable_suffix,
                                                         node_metadata.toString(),
                                                         zkutil::CreateMode::Persistent));
        }
        else
        {
            requests.push_back(zkutil::makeSetRequest(zookeeper_failed_path / node_name_with_retriable_suffix,
                                                      node_metadata.toString(),
                                                      stat.version));
        }
        Coordination::Responses responses;
        auto code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
            return;

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set file as failed");
    }
}

}

#endif
