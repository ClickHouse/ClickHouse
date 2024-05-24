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
        State state = State::None;
        std::atomic<size_t> processed_rows = 0;
        time_t processing_start_time = 0;
        time_t processing_end_time = 0;
        size_t retries = 0;
        std::string last_exception;
        ProfileEvents::Counters profile_counters;

        std::mutex processing_lock;
        std::mutex metadata_lock;

        void updateState(const FileStatus::State & state_, time_t processing_start_time_ = 0)
        {
            std::lock_guard lock(metadata_lock);
            state = state_;
            processing_start_time = processing_start_time_;
        }
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

    /// TODO: remove processing node in desctructor

    virtual ~IFileMetadata() = default;

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
