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
    struct TrackedCollectionItem
    {
        String file_path;
        UInt64 timestamp = 0;
        UInt64 retries_count = 0;
        String last_exception;
    };

    using S3FilesCollection = std::unordered_set<String>;
    using TrackedFiles = std::deque<TrackedCollectionItem>;

    S3QueueHolder(
        const String & zookeeper_path_,
        const S3QueueMode & mode_,
        ContextPtr context_,
        UInt64 & max_set_size_,
        UInt64 & max_set_age_sec_,
        UInt64 & max_loading_retries_);

    void setFileProcessed(const String & file_path);
    bool setFileFailed(const String & file_path, const String & exception_message);
    void setFilesProcessing(Strings & file_paths);
    S3FilesCollection getProcessedAndFailedFiles();
    String getMaxProcessedFile();

    std::shared_ptr<zkutil::EphemeralNodeHolder> acquireLock();

    struct S3QueueCollection
    {
    public:
        virtual ~S3QueueCollection() = default;
        String toString() const;
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


private:
    const UInt64 max_set_size;
    const UInt64 max_set_age_sec;
    const UInt64 max_loading_retries;

    zkutil::ZooKeeperPtr zk_client;
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

    S3FilesCollection getFailedFiles();
    S3FilesCollection getProcessingFiles();
    S3FilesCollection getUnorderedProcessedFiles();
    void removeProcessingFile(const String & file_path);

    S3FilesCollection parseCollection(const String & collection_str);
};


}


#endif
