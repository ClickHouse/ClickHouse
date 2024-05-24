#pragma once
#include <Core/Types.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

class IFileMetadata
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

        void onProcessing();
        void onProcessed();
        void onFailed(const std::string & exception);
        void updateState(State state_) { state = state_; }

        std::string getException() const;

        std::mutex processing_lock;

        std::atomic<State> state = State::None;
        std::atomic<size_t> processed_rows = 0;
        std::atomic<time_t> processing_start_time = 0;
        std::atomic<time_t> processing_end_time = 0;
        std::atomic<size_t> retries = 0;
        ProfileEvents::Counters profile_counters;

    private:
        mutable std::mutex last_exception_mutex;
        std::string last_exception;
    };
    using FileStatusPtr = std::shared_ptr<FileStatus>;

    explicit IFileMetadata(
        const std::string & path_,
        const std::string & processing_node_path_,
        const std::string & processed_node_path_,
        const std::string & failed_node_path_,
        FileStatusPtr file_status_,
        size_t max_loading_retries_,
        LoggerPtr log_);

    virtual ~IFileMetadata();

    bool setProcessing();
    void setProcessed();
    void setFailed(const std::string & exception);

    FileStatusPtr getFileStatus() { return file_status; }

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
    std::optional<std::string> processing_id;

    static std::string getNodeName(const std::string & path);

    static NodeMetadata createNodeMetadata(const std::string & path, const std::string & exception = {}, size_t retries = 0);
};

}
