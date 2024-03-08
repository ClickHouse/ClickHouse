#pragma once

#include <memory>
#include <Client/ConnectionPool.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Common/ThreadPool.h>

namespace DB
{

struct Progress;
using ReadProgressCallbackPtr = std::unique_ptr<ReadProgressCallback>;

struct ProfileInfo;
using ProfileInfoCallback = std::function<void(const ProfileInfo & info)>;

using setExceptionCallback = std::function<void(std::exception_ptr exception_)>;

class RemotePipelinesManager
{
public:
    RemotePipelinesManager(const StorageLimitsList & storage_limits_) : log(&Poco::Logger::get("RemotePipelinesManager"))
    {
        /// Remove leaf limits for remote pipelines manager.
        for (const auto & value : storage_limits_)
            storage_limits.emplace_back(StorageLimits{value.local_limits, {}});
    }

    ~RemotePipelinesManager();

    void setManagedNode(const std::unordered_map<String, IConnectionPool::Entry> & host_connection)
    {
        for (auto & [host, connection] : host_connection)
            managed_nodes.emplace_back(ManagedNode{.host_port = host, .connection = connection});
    }

    void asyncReceiveReporter();

    void cancel();

    void setExceptionCallback(setExceptionCallback exception_callback_) { exception_callback = exception_callback_; }

    /// Set callback for progress. It will be called on Progress packet.
    void setProgressCallback(ProgressCallback callback, QueryStatusPtr process_list_element)
    {
        read_progress_callback = std::make_unique<ReadProgressCallback>();
        read_progress_callback->setProgressCallback(callback);
        read_progress_callback->setProcessListElement(process_list_element);
    }

    /// Set callback for profile info. It will be called on ProfileInfo packet.
    void setProfileInfoCallback(ProfileInfoCallback callback) { profile_info_callback = std::move(callback); }

    void waitFinish();

    bool allFinished();

private:
    void receiveReporter(ThreadGroupPtr thread_group);

    struct ManagedNode
    {
        bool is_finished = false;
        String host_port;
        IConnectionPool::Entry connection;
    };

    void processPacket(Packet & packet, ManagedNode & node);

    Poco::Logger * log;

    StorageLimitsList storage_limits;

    ReadProgressCallbackPtr read_progress_callback;

    ProfileInfoCallback profile_info_callback;

    std::vector<ManagedNode> managed_nodes;

    ThreadFromGlobalPool receive_reporter_thread;

    DB::setExceptionCallback exception_callback;

    std::atomic_bool cancelled = false;
    std::atomic_bool cancelled_reading = false;

    Poco::Event finish_event{false};
    std::mutex finish_mutex;
};

}
