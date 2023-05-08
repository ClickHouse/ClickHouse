#pragma once

#if USE_AWS_S3

#    include <Core/UUID.h>
#    include <Interpreters/Context.h>
#    include <Storages/StorageS3Settings.h>
#    include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{
class S3QueueHolder : public WithContext
{
public:
    using S3FilesCollection = std::unordered_set<String>;
    using ProcessedFiles = std::vector<std::pair<String, Int64>>;

    S3QueueHolder(
        const String & zookeeper_path_, const S3QueueMode & mode_, ContextPtr context_, UInt64 & max_set_size_, UInt64 & max_set_age_s_);

    void setFileProcessed(const String & file_path);
    void setFileFailed(const String & file_path);
    void setFilesProcessing(Strings & file_paths);
    static S3FilesCollection parseCollection(String & files);

    S3FilesCollection getExcludedFiles();
    String getMaxProcessedFile();
    S3FilesCollection getFailedFiles();
    S3FilesCollection getProcessedFiles();
    S3FilesCollection getProcessingFiles();
    std::shared_ptr<zkutil::EphemeralNodeHolder> AcquireLock();

    struct ProcessedCollection
    {
        ProcessedCollection(const UInt64 & max_size_, const UInt64 & max_age_);

        void parse(const String & s);

        String toString() const;

        void add(const String & file_name);
        S3FilesCollection getFileNames();
        const UInt64 max_size;
        const UInt64 max_age;

        void read(ReadBuffer & in);
        void write(WriteBuffer & out) const;
        ProcessedFiles files;
    };

    const UInt64 max_set_size;
    const UInt64 max_set_age_s;

private:
    zkutil::ZooKeeperPtr current_zookeeper;
    mutable std::mutex current_zookeeper_mutex;
    mutable std::mutex mutex;
    const String zookeeper_path;
    const String zookeeper_failed_path;
    const String zookeeper_processing_path;
    const String zookeeper_processed_path;
    const String zookeeper_lock_path;
    const S3QueueMode mode;
    const UUID table_uuid;
    Poco::Logger * log;

    zkutil::ZooKeeperPtr tryGetZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeper() const;
};


}


#endif
