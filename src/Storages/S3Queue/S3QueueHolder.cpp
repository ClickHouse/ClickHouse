#include "config.h"

#if USE_AWS_S3
#include <algorithm>
#include <Storages/S3Queue/S3QueueHolder.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/StorageSnapshot.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NO_ZOOKEEPER;
}

S3QueueHolder::S3QueueHolder(const String & zookeeper_path_, const S3QueueMode & mode_, const UUID & table_uuid_, ContextPtr context_)
    : WithContext(context_)
    , zookeeper_path(zookeeper_path_)
    , zookeeper_failed_path(zookeeper_path_ + "/failed")
    , zookeeper_processing_path(zookeeper_path_ + "/processing")
    , zookeeper_processed_path(zookeeper_path_ + "/processed")
    , mode(mode_)
    , table_uuid(table_uuid_)
    , log(&Poco::Logger::get("S3QueueHolder"))
{
    current_zookeeper = getContext()->getZooKeeper();
}


zkutil::ZooKeeperPtr S3QueueHolder::tryGetZooKeeper() const
{
    std::lock_guard lock(current_zookeeper_mutex);
    return current_zookeeper;
}

zkutil::ZooKeeperPtr S3QueueHolder::getZooKeeper() const
{
    auto res = tryGetZooKeeper();
    if (!res)
        throw Exception(ErrorCodes::NO_ZOOKEEPER, "Cannot get ZooKeeper");
    return res;
}


void S3QueueHolder::setFileProcessed(const String & file_path)
{
    auto zookeeper = getZooKeeper();
    std::lock_guard lock(mutex);

    switch (mode)
    {
        case S3QueueMode::UNORDERED: {
            String processed_files = zookeeper->get(zookeeper_processed_path);
            S3FilesCollection processed = parseCollection(processed_files);
            processed.insert(file_path);
            Strings set_processed;
            set_processed.insert(set_processed.end(), processed.begin(), processed.end());
            zookeeper->set(zookeeper_processed_path, toString(set_processed));
            break;
        }
        case S3QueueMode::ORDERED: {
            zookeeper->set(zookeeper_processed_path, file_path);
        }
    }
}


void S3QueueHolder::setFileFailed(const String & file_path)
{
    auto zookeeper = getZooKeeper();

    std::lock_guard lock(mutex);
    String failed_files = zookeeper->get(zookeeper_failed_path);
    S3FilesCollection failed = parseCollection(failed_files);

    failed.insert(file_path);
    Strings set_failed;
    set_failed.insert(set_failed.end(), failed.begin(), failed.end());

    zookeeper->set(zookeeper_failed_path, toString(set_failed));
}

S3QueueHolder::S3FilesCollection S3QueueHolder::parseCollection(String & files)
{
    ReadBuffer rb(const_cast<char *>(reinterpret_cast<const char *>(files.data())), files.length(), 0);
    Strings deserialized;
    try
    {
        readQuoted(deserialized, rb);
    }
    catch (...)
    {
        deserialized = {};
    }

    std::unordered_set<String> processed(deserialized.begin(), deserialized.end());

    return processed;
}

S3QueueHolder::S3FilesCollection S3QueueHolder::getExcludedFiles()
{
    std::unordered_set<String> exclude_files;

    std::unordered_set<String> failed_files = getFailedFiles();
    LOG_DEBUG(log, "failed_files {}", failed_files.size());
    exclude_files.merge(failed_files);

    if (mode != S3QueueMode::ORDERED)
    {
        std::unordered_set<String> processed_files = getProcessedFiles();
        LOG_DEBUG(log, "processed_files {}", processed_files.size());
        exclude_files.merge(processed_files);
    }

    std::unordered_set<String> processing_files = getProcessingFiles();
    LOG_DEBUG(log, "processing {}", processing_files.size());
    exclude_files.merge(processing_files);

    return exclude_files;
}

String S3QueueHolder::getMaxProcessedFile()
{
    if (mode != S3QueueMode::ORDERED)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getMaxProcessedFile not implemented for unordered mode");

    auto zookeeper = getZooKeeper();
    std::lock_guard lock(mutex);
    String processed = zookeeper->get(zookeeper_path + "/processed");
    return processed;
}

void S3QueueHolder::setFilesProcessing(Strings & file_paths)
{
    auto zookeeper = getZooKeeper();

    std::lock_guard lock(mutex);
    String node_data;
    if (zookeeper->tryGet(fs::path(zookeeper_processing_path), node_data))
    {
        S3FilesCollection processing_files = parseCollection(node_data);
        for (auto x : processing_files)
        {
            if (!std::count(file_paths.begin(), file_paths.end(), x))
            {
                file_paths.push_back(x);
            }
        }
    }
    zookeeper->set(fs::path(zookeeper_processing_path), toString(file_paths));
}

S3QueueHolder::S3FilesCollection S3QueueHolder::getFailedFiles()
{
    auto zookeeper = getZooKeeper();
    std::lock_guard lock(mutex);

    String failed = zookeeper->get(zookeeper_failed_path);
    return parseCollection(failed);
}

S3QueueHolder::S3FilesCollection S3QueueHolder::getProcessedFiles()
{
    auto zookeeper = getZooKeeper();

    std::lock_guard lock(mutex);
    String processed = zookeeper->get(zookeeper_processed_path);
    return parseCollection(processed);
}

S3QueueHolder::S3FilesCollection S3QueueHolder::getProcessingFiles()
{
    auto zookeeper = getZooKeeper();

    std::lock_guard lock(mutex);
    String processing = zookeeper->get(fs::path(zookeeper_processing_path));
    return parseCollection(processing);
}

}

#endif
