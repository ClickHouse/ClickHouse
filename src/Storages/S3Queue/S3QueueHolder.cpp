#include "config.h"

#if USE_AWS_S3
#    include <algorithm>
#    include <IO/Operators.h>
#    include <IO/ReadBufferFromString.h>
#    include <Storages/S3Queue/S3QueueHolder.h>
#    include <Storages/StorageS3Settings.h>
#    include <Storages/StorageSnapshot.h>
#    include <base/sleep.h>
#    include <Common/ZooKeeper/ZooKeeper.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
    extern const int TIMEOUT_EXCEEDED;
}

void S3QueueHolder::S3QueueCollection::read(ReadBuffer & in)
{
    files = {};
    in >> "collection:\n";
    while (!in.eof())
    {
        String file_name;
        Int64 timestamp;
        in >> file_name >> "\n";
        in >> timestamp >> "\n";
        auto pair = std::make_pair(file_name, timestamp);
        files.push_back(pair);
    }
}

void S3QueueHolder::S3QueueCollection::write(WriteBuffer & out) const
{
    out << "collection:\n";
    for (const auto & processed_file : files)
    {
        out << processed_file.first << "\n";
        out << processed_file.second << "\n";
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
        keys.insert(pair.first);
    }
    return keys;
}


S3QueueHolder::S3QueueProcessedCollection::S3QueueProcessedCollection(const UInt64 & max_size_, const UInt64 & max_age_)
    : max_size(max_size_), max_age(max_age_)
{
}

void S3QueueHolder::S3QueueProcessedCollection::parse(const String & s)
{
    ReadBufferFromString buf(s);
    read(buf);
    // Remove old items
    if (max_age > 0)
    {
        Int64 timestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        Int64 max_seconds_diff = max_age;
        auto new_end = std::remove_if(
            files.begin(),
            files.end(),
            [&timestamp, &max_seconds_diff](std::pair<String, Int64> processed_file)
            { return (timestamp - processed_file.second) > max_seconds_diff; });
        files.erase(new_end, files.end());
    }
}


void S3QueueHolder::S3QueueProcessedCollection::add(const String & file_name)
{
    Int64 timestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto pair = std::make_pair(file_name, timestamp);
    files.push_back(pair);

    // Check set size
    if (files.size() > max_size)
    {
        files.erase(files.begin(), files.begin() + (files.size() - max_size));
    }
}


S3QueueHolder::S3QueueFailedCollection::S3QueueFailedCollection(const UInt64 & max_retries_count_) : max_retries_count(max_retries_count_)
{
}

void S3QueueHolder::S3QueueFailedCollection::parse(const String & s)
{
    ReadBufferFromString buf(s);
    read(buf);
}


bool S3QueueHolder::S3QueueFailedCollection::add(const String & file_name)
{
    auto failed_it
        = std::find_if(files.begin(), files.end(), [&file_name](const std::pair<String, Int64> & s) { return s.first == file_name; });
    if (failed_it != files.end())
    {
        failed_it->second--;
        if (failed_it->second == 0)
        {
            return false;
        }
    }
    else
    {
        auto pair = std::make_pair(file_name, max_retries_count);
        files.push_back(pair);
    }
    return true;
}

S3QueueHolder::S3FilesCollection S3QueueHolder::S3QueueFailedCollection::getFilesWithoutRetries()
{
    S3FilesCollection failed_keys;
    for (const auto & pair : files)
    {
        if (pair.second <= 0)
        {
            failed_keys.insert(pair.first);
        }
    }
    return failed_keys;
}

S3QueueHolder::S3QueueHolder(
    const String & zookeeper_path_,
    const S3QueueMode & mode_,
    ContextPtr context_,
    UInt64 & max_set_size_,
    UInt64 & max_set_age_s_,
    UInt64 & max_loading_retries_)
    : WithContext(context_)
    , max_set_size(max_set_size_)
    , max_set_age_s(max_set_age_s_)
    , max_loading_retries(max_loading_retries_)
    , zookeeper_path(zookeeper_path_)
    , zookeeper_failed_path(zookeeper_path_ + "/failed")
    , zookeeper_processing_path(zookeeper_path_ + "/processing")
    , zookeeper_processed_path(zookeeper_path_ + "/processed")
    , zookeeper_lock_path(zookeeper_path_ + "/lock")
    , mode(mode_)
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
    auto lock = AcquireLock();

    if (mode == S3QueueMode::UNORDERED)
    {
        String processed_files = zookeeper->get(zookeeper_processed_path);
        auto processed = S3QueueProcessedCollection(max_set_size, max_set_age_s);
        processed.parse(processed_files);
        processed.add(file_path);
        zookeeper->set(zookeeper_processed_path, processed.toString());
    }
    else
    {
        String max_file = getMaxProcessedFile();
        if (max_file.compare(file_path) <= 0)
        {
            zookeeper->set(zookeeper_processed_path, file_path);
        }
    }
    removeProcessingFile(file_path);
}


bool S3QueueHolder::markFailedAndCheckRetry(const String & file_path)
{
    auto zookeeper = getZooKeeper();
    auto lock = AcquireLock();

    String failed_files = zookeeper->get(zookeeper_failed_path);
    auto failed_collection = S3QueueFailedCollection(max_loading_retries);
    failed_collection.parse(failed_files);
    bool retry_later = failed_collection.add(file_path);

    zookeeper->set(zookeeper_failed_path, failed_collection.toString());
    removeProcessingFile(file_path);

    return retry_later;
}

S3QueueHolder::S3FilesCollection S3QueueHolder::getFailedFiles()
{
    auto zookeeper = getZooKeeper();
    String failed_files = zookeeper->get(zookeeper_failed_path);

    auto failed_collection = S3QueueFailedCollection(max_loading_retries);
    failed_collection.parse(failed_files);

    return failed_collection.getFilesWithoutRetries();
}

String S3QueueHolder::getMaxProcessedFile()
{
    auto zookeeper = getZooKeeper();
    String processed = zookeeper->get(zookeeper_processed_path);
    return processed;
}

S3QueueHolder::S3FilesCollection S3QueueHolder::getProcessingFiles()
{
    auto zookeeper = getZooKeeper();
    String processing = zookeeper->get(fs::path(zookeeper_processing_path));
    return parseCollection(processing);
}

void S3QueueHolder::setFilesProcessing(Strings & file_paths)
{
    auto zookeeper = getZooKeeper();

    for (const auto & x : getProcessingFiles())
    {
        if (!std::count(file_paths.begin(), file_paths.end(), x))
        {
            file_paths.push_back(x);
        }
    }
    zookeeper->set(fs::path(zookeeper_processing_path), toString(file_paths));
}

S3QueueHolder::S3FilesCollection S3QueueHolder::getUnorderedProcessedFiles()
{
    auto zookeeper = getZooKeeper();

    String processed = zookeeper->get(zookeeper_processed_path);
    auto collection = S3QueueProcessedCollection(max_set_size, max_set_age_s);
    collection.parse(processed);

    return collection.getFileNames();
}

S3QueueHolder::S3FilesCollection S3QueueHolder::getExcludedFiles()
{
    auto zookeeper = getZooKeeper();

    S3FilesCollection exclude_files = getFailedFiles();

    if (mode == S3QueueMode::UNORDERED)
    {
        S3FilesCollection processed_files = getUnorderedProcessedFiles();
        exclude_files.merge(processed_files);
    }
    else
    {
        String processed = getMaxProcessedFile();
        exclude_files.insert(processed);
    }

    S3FilesCollection processing_files = getProcessingFiles();
    exclude_files.merge(processing_files);

    return exclude_files;
}

void S3QueueHolder::removeProcessingFile(const String & file_path)
{
    auto zookeeper = getZooKeeper();
    String node_data;
    Strings file_paths;
    String processing = zookeeper->get(zookeeper_processing_path);
    S3FilesCollection processing_files = parseCollection(processing);
    file_paths.insert(file_paths.end(), processing_files.begin(), processing_files.end());

    file_paths.erase(std::remove(file_paths.begin(), file_paths.end(), file_path), file_paths.end());
    zookeeper->set(fs::path(zookeeper_processing_path), toString(file_paths));
}

std::shared_ptr<zkutil::EphemeralNodeHolder> S3QueueHolder::AcquireLock()
{
    auto zookeeper = getZooKeeper();
    UInt32 retry_count = 200;
    UInt32 sleep_ms = 100;

    UInt32 retries = 0;
    while (true)
    {
        Coordination::Error code = zookeeper->tryCreate(zookeeper_lock_path, "", zkutil::CreateMode::Ephemeral);
        if (code == Coordination::Error::ZNONODE || code == Coordination::Error::ZNODEEXISTS)
        {
            retries++;
            if (retries >= retry_count)
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
            return zkutil::EphemeralNodeHolder::existing(zookeeper_lock_path, *zookeeper);
        }
    }
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

}

#endif
