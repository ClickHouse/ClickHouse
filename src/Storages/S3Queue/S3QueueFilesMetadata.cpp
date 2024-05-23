#include "IO/VarInt.h"
#include "config.h"

#if USE_AWS_S3
#    include <algorithm>
#    include <IO/Operators.h>
#    include <IO/ReadBufferFromString.h>
#    include <IO/ReadHelpers.h>
#    include <Storages/S3Queue/S3QueueFilesMetadata.h>
#    include <Storages/S3Queue/StorageS3Queue.h>
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

namespace
{
    UInt64 getCurrentTime()
    {
        return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    }
}

void S3QueueFilesMetadata::S3QueueCollection::read(ReadBuffer & in)
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

void S3QueueFilesMetadata::S3QueueCollection::write(WriteBuffer & out) const
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

String S3QueueFilesMetadata::S3QueueCollection::toString() const
{
    WriteBufferFromOwnString out;
    write(out);
    return out.str();
}

S3QueueFilesMetadata::S3FilesCollection S3QueueFilesMetadata::S3QueueCollection::getFileNames()
{
    S3FilesCollection keys = {};
    for (const auto & pair : files)
        keys.insert(pair.file_path);
    return keys;
}


S3QueueFilesMetadata::S3QueueProcessedCollection::S3QueueProcessedCollection(const UInt64 & max_size_, const UInt64 & max_age_)
    : max_size(max_size_), max_age(max_age_)
{
}

void S3QueueFilesMetadata::S3QueueProcessedCollection::parse(const String & collection_str)
{
    ReadBufferFromString buf(collection_str);
    read(buf);
    if (max_age > 0) // Remove old items
    {
        std::erase_if(
            files,
            [timestamp = getCurrentTime(), this](const TrackedCollectionItem & processed_file)
            { return (timestamp - processed_file.timestamp) > max_age; });
    }
}


void S3QueueFilesMetadata::S3QueueProcessedCollection::add(const String & file_name)
{
    TrackedCollectionItem processed_file;
    processed_file.file_path = file_name;
    processed_file.timestamp = getCurrentTime();
    files.push_back(processed_file);

    /// TODO: it is strange that in parse() we take into account only max_age, but here only max_size.
    while (files.size() > max_size)
    {
        files.pop_front();
    }
}


S3QueueFilesMetadata::S3QueueFailedCollection::S3QueueFailedCollection(const UInt64 & max_retries_count_)
    : max_retries_count(max_retries_count_)
{
}

void S3QueueFilesMetadata::S3QueueFailedCollection::parse(const String & collection_str)
{
    ReadBufferFromString buf(collection_str);
    read(buf);
}


bool S3QueueFilesMetadata::S3QueueFailedCollection::add(const String & file_name, const String & exception_message)
{
    auto failed_it = std::find_if(
        files.begin(), files.end(),
        [&file_name](const TrackedCollectionItem & s) { return s.file_path == file_name; });

    if (failed_it == files.end())
    {
        files.emplace_back(file_name, 0, max_retries_count, exception_message);
    }
    else if (failed_it->retries_count == 0 || --failed_it->retries_count == 0)
    {
        return false;
    }
    return true;
}

S3QueueFilesMetadata::S3FilesCollection S3QueueFilesMetadata::S3QueueFailedCollection::getFileNames()
{
    S3FilesCollection failed_keys;
    for (const auto & pair : files)
    {
        if (pair.retries_count == 0)
            failed_keys.insert(pair.file_path);
    }
    return failed_keys;
}

void S3QueueFilesMetadata::S3QueueProcessingCollection::parse(const String & collection_str)
{
    ReadBufferFromString rb(collection_str);
    Strings result;
    readQuoted(result, rb);
    files = S3FilesCollection(result.begin(), result.end());
}

void S3QueueFilesMetadata::S3QueueProcessingCollection::add(const Strings & file_names)
{
    files.insert(file_names.begin(), file_names.end());
}

void S3QueueFilesMetadata::S3QueueProcessingCollection::remove(const String & file_name)
{
    files.erase(file_name);
}

String S3QueueFilesMetadata::S3QueueProcessingCollection::toString() const
{
    return DB::toString(Strings(files.begin(), files.end()));
}


S3QueueFilesMetadata::S3QueueFilesMetadata(
    const StorageS3Queue * storage_,
    const S3QueueSettings & settings_)
    : storage(storage_)
    , mode(settings_.mode)
    , max_set_size(settings_.s3queue_tracked_files_limit.value)
    , max_set_age_sec(settings_.s3queue_tracked_file_ttl_sec.value)
    , max_loading_retries(settings_.s3queue_loading_retries.value)
    , zookeeper_processing_path(fs::path(storage->getZooKeeperPath()) / "processing")
    , zookeeper_processed_path(fs::path(storage->getZooKeeperPath()) / "processed")
    , zookeeper_failed_path(fs::path(storage->getZooKeeperPath()) / "failed")
    , zookeeper_lock_path(fs::path(storage->getZooKeeperPath()) / "lock")
    , log(&Poco::Logger::get("S3QueueFilesMetadata"))
{
}

void S3QueueFilesMetadata::setFileProcessed(const String & file_path)
{
    auto zookeeper = storage->getZooKeeper();
    auto lock = acquireLock(zookeeper);

    switch (mode)
    {
        case S3QueueMode::UNORDERED:
        {
            S3QueueProcessedCollection processed_files(max_set_size, max_set_age_sec);
            processed_files.parse(zookeeper->get(zookeeper_processed_path));
            processed_files.add(file_path);
            zookeeper->set(zookeeper_processed_path, processed_files.toString());
            break;
        }
        case S3QueueMode::ORDERED:
        {
            // Check that we set in ZooKeeper node only maximum processed file path.
            // This check can be useful, when multiple table engines consume in ordered mode.
            String max_file = getMaxProcessedFile();
            if (max_file.compare(file_path) <= 0)
                zookeeper->set(zookeeper_processed_path, file_path);
            break;
        }
    }
    removeProcessingFile(file_path);
}


bool S3QueueFilesMetadata::setFileFailed(const String & file_path, const String & exception_message)
{
    auto zookeeper = storage->getZooKeeper();
    auto lock = acquireLock(zookeeper);

    S3QueueFailedCollection failed_collection(max_loading_retries);
    failed_collection.parse(zookeeper->get(zookeeper_failed_path));
    const bool can_be_retried = failed_collection.add(file_path, exception_message);
    zookeeper->set(zookeeper_failed_path, failed_collection.toString());
    removeProcessingFile(file_path);
    return can_be_retried;
}

S3QueueFilesMetadata::S3FilesCollection S3QueueFilesMetadata::getFailedFiles()
{
    auto zookeeper = storage->getZooKeeper();
    String failed_files = zookeeper->get(zookeeper_failed_path);

    S3QueueFailedCollection failed_collection(max_loading_retries);
    failed_collection.parse(failed_files);
    return failed_collection.getFileNames();
}

String S3QueueFilesMetadata::getMaxProcessedFile()
{
    auto zookeeper = storage->getZooKeeper();
    return zookeeper->get(zookeeper_processed_path);
}

S3QueueFilesMetadata::S3FilesCollection S3QueueFilesMetadata::getProcessingFiles()
{
    auto zookeeper = storage->getZooKeeper();
    String processing_files;
    if (!zookeeper->tryGet(zookeeper_processing_path, processing_files))
        return {};

    S3QueueProcessingCollection processing_collection;
    if (!processing_files.empty())
        processing_collection.parse(processing_files);
    return processing_collection.getFileNames();
}

void S3QueueFilesMetadata::setFilesProcessing(const Strings & file_paths)
{
    auto zookeeper = storage->getZooKeeper();
    String processing_files;
    zookeeper->tryGet(zookeeper_processing_path, processing_files);

    S3QueueProcessingCollection processing_collection;
    if (!processing_files.empty())
        processing_collection.parse(processing_files);
    processing_collection.add(file_paths);

    if (zookeeper->exists(zookeeper_processing_path))
        zookeeper->set(zookeeper_processing_path, processing_collection.toString());
    else
        zookeeper->create(zookeeper_processing_path, processing_collection.toString(), zkutil::CreateMode::Ephemeral);
}

void S3QueueFilesMetadata::removeProcessingFile(const String & file_path)
{
    auto zookeeper = storage->getZooKeeper();
    String processing_files;
    zookeeper->tryGet(zookeeper_processing_path, processing_files);

    S3QueueProcessingCollection processing_collection;
    processing_collection.parse(processing_files);
    processing_collection.remove(file_path);
    zookeeper->set(zookeeper_processing_path, processing_collection.toString());
}

S3QueueFilesMetadata::S3FilesCollection S3QueueFilesMetadata::getUnorderedProcessedFiles()
{
    auto zookeeper = storage->getZooKeeper();
    S3QueueProcessedCollection processed_collection(max_set_size, max_set_age_sec);
    processed_collection.parse(zookeeper->get(zookeeper_processed_path));
    return processed_collection.getFileNames();
}

S3QueueFilesMetadata::S3FilesCollection S3QueueFilesMetadata::getProcessedFailedAndProcessingFiles()
{
    S3FilesCollection processed_and_failed_files = getFailedFiles();
    switch (mode)
    {
        case S3QueueMode::UNORDERED:
        {
            processed_and_failed_files.merge(getUnorderedProcessedFiles());
            break;
        }
        case S3QueueMode::ORDERED:
        {
            processed_and_failed_files.insert(getMaxProcessedFile());
            break;
        }
    }
    processed_and_failed_files.merge(getProcessingFiles());
    return processed_and_failed_files;
}

std::shared_ptr<zkutil::EphemeralNodeHolder> S3QueueFilesMetadata::acquireLock(zkutil::ZooKeeperPtr zookeeper)
{
    UInt32 retry_count = 200;
    UInt32 sleep_ms = 100;
    UInt32 retries = 0;

    while (true)
    {
        Coordination::Error code = zookeeper->tryCreate(zookeeper_lock_path, "", zkutil::CreateMode::Ephemeral);
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
            throw Coordination::Exception::fromPath(code, zookeeper_lock_path);
        }
        else
        {
            return zkutil::EphemeralNodeHolder::existing(zookeeper_lock_path, *zookeeper);
        }
    }
}

}

#endif
