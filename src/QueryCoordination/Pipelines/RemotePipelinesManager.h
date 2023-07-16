#pragma once

#include <Common/ThreadPool.h>
#include <Client/ConnectionPool.h>
#include <memory>

namespace DB
{

struct Progress;
using ProgressCallback = std::function<void(const Progress & progress)>;

struct ProfileInfo;
using ProfileInfoCallback = std::function<void(const ProfileInfo & info)>;

using setExceptionCallback = std::function<void(std::exception_ptr exception_)>;

class RemotePipelinesManager
{
public:
    RemotePipelinesManager() : log(&Poco::Logger::get("RemotePipelinesManager")) {}

    ~RemotePipelinesManager();

    void setManagedNode(const std::unordered_map<String, IConnectionPool::Entry> & host_connection, const String & local_host)
    {
        for (auto & [host, connection] : host_connection)
        {
            managed_nodes.emplace_back(ManagedNode{.host_port = host, .connection = connection});
        }
    }

    void asyncReceiveReporter();

    void cancel();

    void setExceptionCallback(setExceptionCallback exception_callback_)
    {
        exception_callback = exception_callback_;
    }

    /// Set callback for progress. It will be called on Progress packet.
    void setProgressCallback(ProgressCallback callback) { progress_callback = std::move(callback); }

    /// Set callback for profile info. It will be called on ProfileInfo packet.
    void setProfileInfoCallback(ProfileInfoCallback callback) { profile_info_callback = std::move(callback); }


private:
    void receiveReporter(ThreadGroupPtr thread_group);

    struct ManagedNode
    {
        bool is_finished = false;
        String host_port;
        IConnectionPool::Entry connection;
    };

    Poco::Logger * log;

    ProgressCallback progress_callback;

    ProfileInfoCallback profile_info_callback;

    std::vector<ManagedNode> managed_nodes;

    ThreadFromGlobalPool receive_reporter_thread;

    DB::setExceptionCallback exception_callback;

    std::atomic_bool cancelled = false;
    std::atomic_bool cancelled_reading = false;
};

}
