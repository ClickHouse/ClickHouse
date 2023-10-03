#pragma once

#if USE_AWS_S3

#    include <Core/UUID.h>
#    include <Interpreters/Context.h>
#    include <Storages/StorageS3Settings.h>
#    include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{
class StorageS3Queue;
struct S3QueueSettings;

class S3QueueFilesMetadata
{
public:
    struct TrackedCollectionItem
    {
        TrackedCollectionItem() = default;
        TrackedCollectionItem(const String & file_path_, UInt64 timestamp_, UInt64 retries_count_, const String & last_exception_)
            : file_path(file_path_), timestamp(timestamp_), retries_count(retries_count_), last_exception(last_exception_) {}
        String file_path;
        UInt64 timestamp = 0;
        UInt64 retries_count = 0;
        String last_exception;
    };

    using S3FilesCollection = std::unordered_set<String>;
    using TrackedFiles = std::deque<TrackedCollectionItem>;

    S3QueueFilesMetadata(const StorageS3Queue * storage_, const S3QueueSettings & settings_);

    void setFilesProcessing(const Strings & file_paths);
    void setFileProcessed(const String & file_path);
    bool setFileFailed(const String & file_path, const String & exception_message);

    S3FilesCollection getProcessedFailedAndProcessingFiles();
    String getMaxProcessedFile();
    std::shared_ptr<zkutil::EphemeralNodeHolder> acquireLock(zkutil::ZooKeeperPtr zookeeper);

    struct S3QueueCollection
    {
    public:
        virtual ~S3QueueCollection() = default;
        virtual String toString() const;
        S3FilesCollection getFileNames();

        virtual void parse(const String & collection_str) = 0;

    protected:
        TrackedFiles files;

        void read(ReadBuffer & in);
        void write(WriteBuffer & out) const;
    };

    struct S3QueueProcessedCollection : public S3QueueCollection
    {
    public:
        S3QueueProcessedCollection(const UInt64 & max_size_, const UInt64 & max_age_);

        void parse(const String & collection_str) override;
        void add(const String & file_name);

    private:
        const UInt64 max_size;
        const UInt64 max_age;
    };

    struct S3QueueFailedCollection : S3QueueCollection
    {
    public:
        S3QueueFailedCollection(const UInt64 & max_retries_count_);

        void parse(const String & collection_str) override;
        bool add(const String & file_name, const String & exception_message);

        S3FilesCollection getFileNames();

    private:
        UInt64 max_retries_count;
    };

    struct S3QueueProcessingCollection
    {
    public:
        S3QueueProcessingCollection() = default;

        void parse(const String & collection_str);
        void add(const Strings & file_names);
        void remove(const String & file_name);

        String toString() const;
        const S3FilesCollection & getFileNames() const { return files; }

    private:
        S3FilesCollection files;
    };

private:
    const StorageS3Queue * storage;
    const S3QueueMode mode;
    const UInt64 max_set_size;
    const UInt64 max_set_age_sec;
    const UInt64 max_loading_retries;

    const String zookeeper_processing_path;
    const String zookeeper_processed_path;
    const String zookeeper_failed_path;
    const String zookeeper_lock_path;

    mutable std::mutex mutex;
    Poco::Logger * log;

    S3FilesCollection getFailedFiles();
    S3FilesCollection getProcessingFiles();
    S3FilesCollection getUnorderedProcessedFiles();

    void removeProcessingFile(const String & file_path);
};


}


#endif
