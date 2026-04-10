#include <Coordination/KeeperSessionRegistry.h>
#include <Coordination/KeeperRequestsQueue.h>
#include <Coordination/SessionRequest.h>
#include <Coordination/CoordinationSettings.h>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>


namespace CurrentMetrics
{
    extern const Metric KeeperAliveConnections;
}

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsBool quorum_reads;
    extern const CoordinationSettingsUInt64 max_session_active_requests;
}

KeeperSessionRegistry::KeeperSessionRegistry(
    KeeperContextPtr keeper_context,
    KeeperRequestsQueue & requests_queue_,
    KeeperSession::LocalReadCallback local_read_dispatch_)
    : local_read_dispatch(std::move(local_read_dispatch_))
    , requests_queue(&requests_queue_)
{
    const auto & settings = keeper_context->getCoordinationSettings();
    quorum_reads = settings[CoordinationSetting::quorum_reads];
    max_session_active_requests = settings[CoordinationSetting::max_session_active_requests];
}

KeeperSessionPtr KeeperSessionRegistry::registerSession(int64_t session_id, KeeperSession::ResponseCallback callback)
{
    std::unique_lock lock(mutex);

    if (session_index.contains(session_id))
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Session with id {} already registered in session registry", session_id);

    chassert(requests_queue != nullptr);
    auto handle = requests_queue->getSubqueue(session_id);

    auto session = std::make_shared<KeeperSession>(session_id, std::move(callback), *this, handle, local_read_dispatch);
    sessions.push_back(session);
    session_index[session_id] = sessions.size() - 1;

    CurrentMetrics::add(CurrentMetrics::KeeperAliveConnections);
    return session;
}

KeeperSessionPtr KeeperSessionRegistry::findSession(int64_t session_id) const
{
    std::shared_lock lock(mutex);

    auto it = session_index.find(session_id);
    if (it == session_index.end())
        return {};

    return sessions[it->second];
}

KeeperSessionPtr KeeperSessionRegistry::detachSession(int64_t session_id)
{
    std::lock_guard lock(mutex);

    auto it = session_index.find(session_id);
    if (it == session_index.end())
        return {};

    size_t idx = it->second;
    auto session = std::move(sessions[idx]);

    /// Swap-and-pop: move the last element into the vacated slot.
    size_t last = sessions.size() - 1;
    if (idx != last)
    {
        sessions[idx] = std::move(sessions[last]);
        session_index[sessions[idx]->getSessionID()] = idx;
    }
    sessions.pop_back();
    session_index.erase(it);

    CurrentMetrics::sub(CurrentMetrics::KeeperAliveConnections);
    return session;
}

void KeeperSessionRegistry::registerNewSessionCallback(int64_t internal_id, ZooKeeperResponseCallback callback)
{
    std::lock_guard lock(mutex);
    new_session_callbacks[internal_id] = std::move(callback);
}

ZooKeeperResponseCallback KeeperSessionRegistry::extractNewSessionCallback(int64_t internal_id, int32_t server_id, int32_t our_server_id)
{
    std::lock_guard lock(mutex);

    if (server_id != our_server_id)
        return {};

    auto it = new_session_callbacks.find(internal_id);
    if (it == new_session_callbacks.end())
        return {};

    auto callback = std::move(it->second);
    new_session_callbacks.erase(it);
    return callback;
}

int64_t KeeperSessionRegistry::nextInternalSessionId()
{
    return internal_session_id_counter.fetch_add(1);
}

std::vector<KeeperSessionPtr> KeeperSessionRegistry::shutdown()
{

    /// Extract callbacks under lock, invoke outside to avoid deadlock
    /// if a callback re-enters the registry.
    decltype(new_session_callbacks) callbacks_to_fail;
    std::vector<KeeperSessionPtr> detached_sessions;

    {
        std::lock_guard lock(mutex);

        detached_sessions = std::move(sessions);
        session_index.clear();

        callbacks_to_fail = std::move(new_session_callbacks);
    }

    /// Fail pending SessionID waiters so they don't hang until timeout.
    /// The callbacks hold promises that `getSessionID` is waiting on.
    for (auto & [internal_id, callback] : callbacks_to_fail)
    {
        try
        {
            auto error_response = std::make_shared<Coordination::ZooKeeperSessionIDResponse>();
            error_response->error = Coordination::Error::ZSESSIONEXPIRED;
            error_response->internal_id = internal_id;
            error_response->server_id = 0;
            callback(error_response, nullptr);
        }
        catch (...)
        {
            tryLogCurrentException("KeeperSessionRegistry", "Failed to fail SessionID callback during shutdown");
        }
    }

    return detached_sessions;
}

}
