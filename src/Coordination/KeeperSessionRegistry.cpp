#include <Coordination/KeeperSessionRegistry.h>
#include <Coordination/KeeperSession.h>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>

namespace CurrentMetrics
{
    extern const Metric KeeperAliveConnections;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

KeeperSessionPtr KeeperSessionRegistry::registerSession(
    int64_t session_id,
    ZooKeeperResponseCallback callback,
    bool quorum_reads,
    size_t max_session_active_requests,
    KeeperSession::LocalReadCallback local_read_callback)
{
    bool inserted = false;
    {
        std::lock_guard lock(live_sessions_mutex);
        inserted = live_sessions.insert(session_id).second;
    }

    auto session = std::make_shared<KeeperSession>(session_id, callback, quorum_reads, max_session_active_requests, std::move(local_read_callback));

    try
    {
        std::lock_guard lock(session_to_response_callback_mutex);
        if (!session_to_response_callback.try_emplace(session_id, callback).second)
            throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Session with id {} already registered in dispatcher", session_id);
        sessions_[session_id] = session;
        CurrentMetrics::add(CurrentMetrics::KeeperAliveConnections);
    }
    catch (...)
    {
        if (inserted)
        {
            std::lock_guard lock(live_sessions_mutex);
            live_sessions.erase(session_id);
        }
        throw;
    }

    return session;
}

ZooKeeperResponseCallback KeeperSessionRegistry::unregisterSession(int64_t session_id)
{
    ZooKeeperResponseCallback callback;
    KeeperSessionPtr session;
    {
        std::lock_guard lock(session_to_response_callback_mutex);
        auto it = session_to_response_callback.find(session_id);
        if (it != session_to_response_callback.end())
        {
            callback = std::move(it->second);
            session_to_response_callback.erase(it);
            CurrentMetrics::sub(CurrentMetrics::KeeperAliveConnections);
        }
        auto sit = sessions_.find(session_id);
        if (sit != sessions_.end())
        {
            session = std::move(sit->second);
            sessions_.erase(sit);
        }
    }
    /// Close the session object outside the lock.
    if (session)
        session->close();
    return callback;
}

KeeperSessionPtr KeeperSessionRegistry::findSession(int64_t session_id) const
{
    std::lock_guard lock(session_to_response_callback_mutex);
    auto it = sessions_.find(session_id);
    if (it != sessions_.end())
        return it->second;
    return nullptr;
}

bool KeeperSessionRegistry::isSessionAlive(int64_t session_id) const
{
    std::lock_guard lock(live_sessions_mutex);
    return live_sessions.contains(session_id);
}

void KeeperSessionRegistry::addLiveSession(int64_t session_id)
{
    std::lock_guard lock(live_sessions_mutex);
    live_sessions.insert(session_id);
}

void KeeperSessionRegistry::removeLiveSession(int64_t session_id)
{
    std::lock_guard lock(live_sessions_mutex);
    live_sessions.erase(session_id);
}

ZooKeeperResponseCallback KeeperSessionRegistry::getSessionCallback(int64_t session_id) const
{
    std::lock_guard lock(session_to_response_callback_mutex);
    auto it = session_to_response_callback.find(session_id);
    if (it != session_to_response_callback.end())
        return it->second;
    return {};
}

bool KeeperSessionRegistry::hasSessionCallback(int64_t session_id) const
{
    std::lock_guard lock(session_to_response_callback_mutex);
    return session_to_response_callback.contains(session_id);
}

ZooKeeperResponseCallback KeeperSessionRegistry::extractAndEraseSessionCallback(int64_t session_id)
{
    ZooKeeperResponseCallback callback;
    std::lock_guard lock(session_to_response_callback_mutex);
    auto it = session_to_response_callback.find(session_id);
    if (it != session_to_response_callback.end())
    {
        callback = std::move(it->second);
        session_to_response_callback.erase(it);
        CurrentMetrics::sub(CurrentMetrics::KeeperAliveConnections);
    }
    return callback;
}

void KeeperSessionRegistry::registerNewSessionCallback(int64_t internal_id, ZooKeeperResponseCallback callback)
{
    std::lock_guard lock(session_to_response_callback_mutex);
    new_session_id_response_callback[internal_id] = std::move(callback);
}

ZooKeeperResponseCallback KeeperSessionRegistry::extractNewSessionCallback(int64_t internal_id)
{
    ZooKeeperResponseCallback callback;
    std::lock_guard lock(session_to_response_callback_mutex);
    auto it = new_session_id_response_callback.find(internal_id);
    if (it != new_session_id_response_callback.end())
    {
        callback = std::move(it->second);
        new_session_id_response_callback.erase(it);
    }
    return callback;
}

bool KeeperSessionRegistry::hasNewSessionCallback(int64_t internal_id) const
{
    std::lock_guard lock(session_to_response_callback_mutex);
    return new_session_id_response_callback.contains(internal_id);
}

int64_t KeeperSessionRegistry::nextInternalSessionId()
{
    return internal_session_id_counter.fetch_add(1);
}

KeeperSessionRegistry::ShutdownResult KeeperSessionRegistry::clearAllSessions()
{
    ShutdownResult result;
    std::vector<KeeperSessionPtr> sessions_to_close;
    {
        std::lock_guard lock(session_to_response_callback_mutex);
        result.sessions.reserve(session_to_response_callback.size());
        for (auto & [session_id, callback] : session_to_response_callback)
            result.sessions.emplace_back(session_id, std::move(callback));
        session_to_response_callback.clear();

        sessions_to_close.reserve(sessions_.size());
        for (auto & [_, session] : sessions_)
            sessions_to_close.push_back(std::move(session));
        sessions_.clear();
    }
    /// Close session objects outside the lock.
    for (auto & session : sessions_to_close)
        session->close();
    return result;
}

}
