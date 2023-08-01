#include "IO/VarInt.h"
#include "config.h"

#if USE_AWS_S3
#    include <algorithm>
#    include <IO/Operators.h>
#    include <IO/ReadBufferFromString.h>
#    include <IO/ReadHelpers.h>
#    include <Storages/S3Queue/S3QueueHolder.h>
#    include <Storages/StorageS3Settings.h>
#    include <Storages/StorageSnapshot.h>
#    include <base/sleep.h>
#    include <Common/ZooKeeper/ZooKeeper.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}

/// TODO: update zk session if expired

void S3QueueHolder::S3QueueCollection::read(ReadBuffer & in)
{
    files = {};

    if (in.eof())
        return;

    size_t files_num;
    in >> files_num >> "\n";
    while (files_num--)
    {
        TrackedCollectionItem item;
        in >> item.file_path >> "\n";
        in >> item.timestamp >> "\n";
        in >> item.retries_count >> "\n";
        in >> item.last_exception >> "\n";
        files.push_back(item);
    }
}

void S3QueueHolder::S3QueueCollection::write(WriteBuffer & out) const
{
    out << files.size() << "\n";
    for (const auto & processed_file : files)
    {
        out << processed_file.file_path << "\n";
        out << processed_file.timestamp << "\n";
        out << processed_file.retries_count << "\n";
        out << processed_file.last_exception << "\n";
    }
}

String S3QueueHolder::S3QueueCollection::toString() const
{
    WriteBufferFromOwnString out;
    write(out);
    return out.str();
}

S3QueueHolder::S3FilesCollection S3QueueHolder::S3QueueCollection::getFileNames()
{
    S3FilesCollection keys = {};
    for (const auto & pair : files)
    {
        keys.insert(pair.file_path);
    }
    return keys;
}


S3QueueHolder::S3QueueProcessedCollection::S3QueueProcessedCollection(const UInt64 & max_size_, const UInt64 & max_age_)
    : max_size(max_size_), max_age(max_age_)
{
}

void S3QueueHolder::S3QueueProcessedCollection::parse(const String & collection_str)
{
    ReadBufferFromString buf(collection_str);
    read(buf);
    if (max_age > 0) // Remove old items
    {
        UInt64 timestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        UInt64 max_seconds_diff = max_age;
        std::erase_if(
            files,
            [&timestamp, &max_seconds_diff](const TrackedCollectionItem & processed_file)
            { return (timestamp - processed_file.timestamp) > max_seconds_diff; });
    }
}


void S3QueueHolder::S3QueueProcessedCollection::add(const String & file_name)
{
    UInt64 timestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    TrackedCollectionItem processed_file = {.file_path=file_name, .timestamp=timestamp};
    files.push_back(processed_file);

    while (files.size() > max_size)
    {
        files.pop_front();
    }
}


S3QueueHolder::S3QueueFailedCollection::S3QueueFailedCollection(const UInt64 & max_retries_count_) : max_retries_count(max_retries_count_)
{
}

void S3QueueHolder::S3QueueFailedCollection::parse(const String & collection_str)
{
    ReadBufferFromString buf(collection_str);
    read(buf);
}


bool S3QueueHolder::S3QueueFailedCollection::add(const String & file_name, const String & exception_message)
{
    auto failed_it
        = std::find_if(files.begin(), files.end(), [&file_name](const TrackedCollectionItem & s) { return s.file_path == file_name; });
    if (failed_it != files.end())
    {
        if (failed_it->retries_count == 0 || --failed_it->retries_count == 0)
        {
            return false;
        }
    }
    else
    {
        TrackedCollectionItem failed_file = { .file_path=file_name, .retries_count=max_retries_count, .last_exception = exception_message };
        files.push_back(failed_file);
    }
    return true;
}

S3QueueHolder::S3FilesCollection S3QueueHolder::S3QueueFailedCollection::getFileNames()
{
    S3FilesCollection failed_keys;
    for (const auto & pair : files)
    {
        if (pair.retries_count <= 0)
        {
            failed_keys.insert(pair.file_path);
        }
    }
    return failed_keys;
}

S3QueueHolder::S3QueueHolder(
    const String & zookeeper_path_,
    const S3QueueMode & mode_,
    ContextPtr context_,
    UInt64 & max_set_size_,
    UInt64 & max_set_age_sec_,
    UInt64 & max_loading_retries_)
    : WithContext(context_)
    , max_set_size(max_set_size_)
    , max_set_age_sec(max_set_age_sec_)
    , max_loading_retries(max_loading_retries_)
    , zk_client(getContext()->getZooKeeper())
    , zookeeper_path(zookeeper_path_)
    , zookeeper_failed_path(fs::path(zookeeper_path_) / "failed")
    , zookeeper_processing_path(fs::path(zookeeper_path_) / "processing")
    , zookeeper_processed_path(fs::path(zookeeper_path_) / "processed")
    , zookeeper_lock_path(fs::path(zookeeper_path_) / "lock")
    , mode(mode_)
    , log(&Poco::Logger::get("S3QueueHolder"))
{
}


void S3QueueHolder::setFileProcessed(const String & file_path)
{
    auto lock = acquireLock();

    if (mode == S3QueueMode::UNORDERED)
    {
        String processed_files = zk_client->get(zookeeper_processed_path);
        auto processed = S3QueueProcessedCollection(max_set_size, max_set_age_sec);
        processed.parse(processed_files);
        processed.add(file_path);
        zk_client->set(zookeeper_processed_path, processed.toString());
    }
    else if (mode == S3QueueMode::ORDERED)
    {
        String max_file = getMaxProcessedFile();
        // Check that we set in ZooKeeper node only maximum processed file path.
        // This check can be useful, when multiple table engines consume in ordered mode.
        if (max_file.compare(file_path) <= 0)
        {
            zk_client->set(zookeeper_processed_path, file_path);
        }
    }
    removeProcessingFile(file_path);
}


bool S3QueueHolder::setFileFailed(const String & file_path, const String & exception_message)
{
    auto lock = acquireLock();

    auto failed_collection = S3QueueFailedCollection(max_loading_retries);
    failed_collection.parse(zk_client->get(zookeeper_failed_path));
    bool retry_later = failed_collection.add(file_path, exception_message);

    zk_client->set(zookeeper_failed_path, failed_collection.toString());
    removeProcessingFile(file_path);

    return retry_later;
}

S3QueueHolder::S3FilesCollection S3QueueHolder::getFailedFiles()
{
    String failed_files = zk_client->get(zookeeper_failed_path);

    auto failed_collection = S3QueueFailedCollection(max_loading_retries);
    failed_collection.parse(failed_files);

    return failed_collection.getFileNames();
}

String S3QueueHolder::getMaxProcessedFile()
{
    String processed = zk_client->get(zookeeper_processed_path);
    return processed;
}

S3QueueHolder::S3FilesCollection S3QueueHolder::getProcessingFiles()
{
    String processing_files;
    if (!zk_client->tryGet(zookeeper_processing_path, processing_files))
        return {};
    return parseCollection(processing_files);
}

void S3QueueHolder::setFilesProcessing(Strings & file_paths)
{
    std::unordered_set<String> processing_files(file_paths.begin(), file_paths.end());
    processing_files.merge(getProcessingFiles());
    String processing_files_str = toString(Strings(processing_files.begin(), processing_files.end()));

    if (zk_client->exists(zookeeper_processing_path))
        zk_client->set(fs::path(zookeeper_processing_path), processing_files_str);
    else
        zk_client->create(fs::path(zookeeper_processing_path), processing_files_str, zkutil::CreateMode::Ephemeral);
}

S3QueueHolder::S3FilesCollection S3QueueHolder::getUnorderedProcessedFiles()
{
    String processed = zk_client->get(zookeeper_processed_path);
    auto collection = S3QueueProcessedCollection(max_set_size, max_set_age_sec);
    collection.parse(processed);
    return collection.getFileNames();
}

S3QueueHolder::S3FilesCollection S3QueueHolder::getProcessedAndFailedFiles()
{
    S3FilesCollection processed_and_failed_files = getFailedFiles();

    if (mode == S3QueueMode::UNORDERED)
    {
        processed_and_failed_files.merge(getUnorderedProcessedFiles());
    }
    else
    {
        String processed = getMaxProcessedFile();
        processed_and_failed_files.insert(processed);
    }

    S3FilesCollection processing_files = getProcessingFiles();
    processed_and_failed_files.merge(processing_files);

    return processed_and_failed_files;
}

void S3QueueHolder::removeProcessingFile(const String & file_path)
{
    String node_data;
    String processing = zk_client->get(zookeeper_processing_path);
    S3FilesCollection processing_files = parseCollection(processing);

    processing_files.erase(file_path);

    Strings file_paths(processing_files.begin(), processing_files.end());
    zk_client->set(fs::path(zookeeper_processing_path), toString(file_paths));
}

std::shared_ptr<zkutil::EphemeralNodeHolder> S3QueueHolder::acquireLock()
{
    UInt32 retry_count = 200;
    UInt32 sleep_ms = 100;

    UInt32 retries = 0;
    while (true)
    {
        Coordination::Error code = zk_client->tryCreate(zookeeper_lock_path, "", zkutil::CreateMode::Ephemeral);
        if (code == Coordination::Error::ZNONODE || code == Coordination::Error::ZNODEEXISTS)
        {
            retries++;
            if (retries > retry_count)
            {
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Can't acquire zookeeper lock");
            }
            sleepForMilliseconds(sleep_ms);
        }
        else if (code != Coordination::Error::ZOK)
        {
            throw Coordination::Exception(code, zookeeper_lock_path);
        }
        else
        {
            return zkutil::EphemeralNodeHolder::existing(zookeeper_lock_path, *zk_client);
        }
    }
}

S3QueueHolder::S3FilesCollection S3QueueHolder::parseCollection(const String & collection_str)
{
    ReadBufferFromString rb(collection_str);
    Strings deserialized;
    try
    {
        readQuoted(deserialized, rb);
    }
    catch (const Exception & e)
    {
        LOG_WARNING(log, "Can't parse collection from ZooKeeper node: {}", e.displayText());
        deserialized = {};
    }

    return std::unordered_set<String>(deserialized.begin(), deserialized.end());
}

}

#endif
