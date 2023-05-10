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
    using FailedFiles = std::vector<std::pair<String, Int64>>;

    S3QueueHolder(
        const String & zookeeper_path_,
        const S3QueueMode & mode_,
        ContextPtr context_,
        UInt64 & max_set_size_,
        UInt64 & max_set_age_s_,
        UInt64 & max_loading_retries_);

    void setFileProcessed(const String & file_path);
    bool markFailedAndCheckRetry(const String & file_path);
    void setFilesProcessing(Strings & file_paths);
    S3FilesCollection getExcludedFiles();
    String getMaxProcessedFile();

    std::shared_ptr<zkutil::EphemeralNodeHolder> AcquireLock();

    struct S3QueueCollection
    {
    public:
        virtual ~S3QueueCollection() = default;
        String toString() const;
        S3FilesCollection getFileNames();

        virtual void parse(const String & s) = 0;

    protected:
        ProcessedFiles files;

        void read(ReadBuffer & in);
        void write(WriteBuffer & out) const;
    };

    struct S3QueueProcessedCollection : public S3QueueCollection
    {
    public:
        S3QueueProcessedCollection(const UInt64 & max_size_, const UInt64 & max_age_);

        void parse(const String & s) override;
        void add(const String & file_name);

    private:
        const UInt64 max_size;
        const UInt64 max_age;
    };

    struct S3QueueFailedCollection : S3QueueCollection
    {
    public:
        S3QueueFailedCollection(const UInt64 & max_retries_count_);

        void parse(const String & s) override;
        bool add(const String & file_name);

        S3FilesCollection getFilesWithoutRetries();

    private:
        const UInt64 max_retries_count;
    };


private:
    const UInt64 max_set_size;
    const UInt64 max_set_age_s;
    const UInt64 max_loading_retries;

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

    S3FilesCollection getFailedFiles();
    S3FilesCollection getProcessingFiles();
    S3FilesCollection getUnorderedProcessedFiles();
    void removeProcessingFile(const String & file_path);

    static S3FilesCollection parseCollection(String & files);
};


}


#endif
