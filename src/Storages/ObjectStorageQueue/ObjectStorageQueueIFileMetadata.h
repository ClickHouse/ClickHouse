#pragma once
#include <Core/Types.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

class ObjectStorageQueueIFileMetadata
{
public:
    struct FileStatus
    {
        enum class State : uint8_t
        {
            Processing,
            Processed,
            Failed,
            None
        };

        void setProcessingEndTime();
        void onProcessing();
        void onProcessed();
        void reset();
        void onFailed(const std::string & exception);
        void updateState(State state_);

        std::string getException() const;

        std::mutex processing_lock;

        std::atomic<State> state = State::None;
        std::atomic<size_t> processed_rows = 0;
        std::atomic<time_t> processing_start_time = 0;
        std::atomic<time_t> processing_end_time = 0;
        std::atomic<size_t> retries = 0;

    private:
        mutable std::mutex last_exception_mutex;
        std::string last_exception;
    };
    using FileStatusPtr = std::shared_ptr<FileStatus>;

    explicit ObjectStorageQueueIFileMetadata(
        const std::string & path_,
        const std::string & processing_node_path_,
        const std::string & processed_node_path_,
        const std::string & failed_node_path_,
        FileStatusPtr file_status_,
        size_t max_loading_retries_,
        LoggerPtr log_);

    virtual ~ObjectStorageQueueIFileMetadata();

    bool setProcessing();
    void setProcessed();
    void resetProcessing();
    void setFailed(const std::string & exception_message, bool reduce_retry_count, bool overwrite_status);

    virtual void setProcessedAtStartRequests(
        Coordination::Requests & requests,
        const zkutil::ZooKeeperPtr & zk_client) = 0;

    FileStatusPtr getFileStatus() { return file_status; }
    const std::string & getPath() const { return path; }
    size_t getMaxTries() const { return max_loading_retries; }

    struct NodeMetadata
    {
        std::string file_path; UInt64 last_processed_timestamp = 0;
        std::string last_exception;
        UInt64 retries = 0;
        std::string processing_id; /// For ephemeral processing node.

        std::string toString() const;
        static NodeMetadata fromString(const std::string & metadata_str);
    };

protected:
    virtual std::pair<bool, FileStatus::State> setProcessingImpl() = 0;
    virtual void setProcessedImpl() = 0;
    virtual void resetProcessingImpl();
    void setFailedNonRetriable();
    void setFailedRetriable();

    const std::string path;
    const std::string node_name;
    const FileStatusPtr file_status;
    const size_t max_loading_retries;

    const std::string processing_node_path;
    const std::string processed_node_path;
    const std::string failed_node_path;

    NodeMetadata node_metadata;
    LoggerPtr log;

    /// processing node is ephemeral, so we cannot verify with it if
    /// this node was created by a certain processor on a previous processing stage,
    /// because we could get a session expired in between the stages
    /// and someone else could just create this processing node.
    /// Therefore we also create a persistent processing node
    /// which is updated on each creation of ephemeral processing node.
    /// We use the version of this node to verify the version of the processing ephemeral node.
    const std::string processing_node_id_path;
    /// Id of the processor.
    std::optional<std::string> processing_id;
    /// Version of the processing id persistent node.
    std::optional<int> processing_id_version;

    static std::string getNodeName(const std::string & path);

    static NodeMetadata createNodeMetadata(const std::string & path, const std::string & exception = {}, size_t retries = 0);

    static std::string getProcessorInfo(const std::string & processor_id);
};

}
