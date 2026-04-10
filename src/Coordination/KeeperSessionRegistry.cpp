#include <Coordination/KeeperSessionRegistry.h>

#include <Common/ProfiledLocks.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>

namespace CurrentMetrics
{
    extern const Metric KeeperAliveConnections;
}

namespace ProfileEvents
{
    extern const Event KeeperLiveSessionsLockWaitMicroseconds;
    extern const Event KeeperLiveSessionsLockHoldMicroseconds;
    extern const Event KeeperSessionCallbackLockWaitMicroseconds;
    extern const Event KeeperSessionCallbackLockHoldMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void KeeperSessionRegistry::registerSession(int64_t session_id, ZooKeeperResponseCallback callback)
{
    bool inserted = false;
    {
        ProfiledMutexLock lock(live_sessions_mutex, ProfileEvents::KeeperLiveSessionsLockWaitMicroseconds, ProfileEvents::KeeperLiveSessionsLockHoldMicroseconds);
        inserted = live_sessions.insert(session_id).second;
    }

    try
    {
        ProfiledMutexLock lock(session_to_response_callback_mutex, ProfileEvents::KeeperSessionCallbackLockWaitMicroseconds, ProfileEvents::KeeperSessionCallbackLockHoldMicroseconds);
        if (!session_to_response_callback.try_emplace(session_id, callback).second)
            throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Session with id {} already registered in dispatcher", session_id);
        CurrentMetrics::add(CurrentMetrics::KeeperAliveConnections);
    }
    catch (...)
    {
        if (inserted)
        {
            ProfiledMutexLock lock(live_sessions_mutex, ProfileEvents::KeeperLiveSessionsLockWaitMicroseconds, ProfileEvents::KeeperLiveSessionsLockHoldMicroseconds);
            live_sessions.erase(session_id);
        }
        throw;
    }
}

ZooKeeperResponseCallback KeeperSessionRegistry::unregisterSession(int64_t session_id)
{
    ZooKeeperResponseCallback callback;
    {
        ProfiledMutexLock lock(session_to_response_callback_mutex, ProfileEvents::KeeperSessionCallbackLockWaitMicroseconds, ProfileEvents::KeeperSessionCallbackLockHoldMicroseconds);
        auto it = session_to_response_callback.find(session_id);
        if (it != session_to_response_callback.end())
        {
            callback = std::move(it->second);
            session_to_response_callback.erase(it);
            CurrentMetrics::sub(CurrentMetrics::KeeperAliveConnections);
        }
    }
    return callback;
}

bool KeeperSessionRegistry::isSessionAlive(int64_t session_id) const
{
    ProfiledMutexLock lock(live_sessions_mutex, ProfileEvents::KeeperLiveSessionsLockWaitMicroseconds, ProfileEvents::KeeperLiveSessionsLockHoldMicroseconds);
    return live_sessions.contains(session_id);
}

void KeeperSessionRegistry::addLiveSession(int64_t session_id)
{
    ProfiledMutexLock lock(live_sessions_mutex, ProfileEvents::KeeperLiveSessionsLockWaitMicroseconds, ProfileEvents::KeeperLiveSessionsLockHoldMicroseconds);
    live_sessions.insert(session_id);
}

void KeeperSessionRegistry::removeLiveSession(int64_t session_id)
{
    ProfiledMutexLock lock(live_sessions_mutex, ProfileEvents::KeeperLiveSessionsLockWaitMicroseconds, ProfileEvents::KeeperLiveSessionsLockHoldMicroseconds);
    live_sessions.erase(session_id);
}

ZooKeeperResponseCallback KeeperSessionRegistry::getSessionCallback(int64_t session_id) const
{
    ProfiledMutexLock lock(session_to_response_callback_mutex, ProfileEvents::KeeperSessionCallbackLockWaitMicroseconds, ProfileEvents::KeeperSessionCallbackLockHoldMicroseconds);
    auto it = session_to_response_callback.find(session_id);
    if (it != session_to_response_callback.end())
        return it->second;
    return {};
}

bool KeeperSessionRegistry::hasSessionCallback(int64_t session_id) const
{
    ProfiledMutexLock lock(session_to_response_callback_mutex, ProfileEvents::KeeperSessionCallbackLockWaitMicroseconds, ProfileEvents::KeeperSessionCallbackLockHoldMicroseconds);
    return session_to_response_callback.contains(session_id);
}

ZooKeeperResponseCallback KeeperSessionRegistry::extractAndEraseSessionCallback(int64_t session_id)
{
    ZooKeeperResponseCallback callback;
    ProfiledMutexLock lock(session_to_response_callback_mutex, ProfileEvents::KeeperSessionCallbackLockWaitMicroseconds, ProfileEvents::KeeperSessionCallbackLockHoldMicroseconds);
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
    ProfiledMutexLock lock(session_to_response_callback_mutex, ProfileEvents::KeeperSessionCallbackLockWaitMicroseconds, ProfileEvents::KeeperSessionCallbackLockHoldMicroseconds);
    new_session_id_response_callback[internal_id] = std::move(callback);
}

ZooKeeperResponseCallback KeeperSessionRegistry::extractNewSessionCallback(int64_t internal_id)
{
    ZooKeeperResponseCallback callback;
    ProfiledMutexLock lock(session_to_response_callback_mutex, ProfileEvents::KeeperSessionCallbackLockWaitMicroseconds, ProfileEvents::KeeperSessionCallbackLockHoldMicroseconds);
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
    ProfiledMutexLock lock(session_to_response_callback_mutex, ProfileEvents::KeeperSessionCallbackLockWaitMicroseconds, ProfileEvents::KeeperSessionCallbackLockHoldMicroseconds);
    return new_session_id_response_callback.contains(internal_id);
}

int64_t KeeperSessionRegistry::nextInternalSessionId()
{
    return internal_session_id_counter.fetch_add(1);
}

KeeperSessionRegistry::ShutdownResult KeeperSessionRegistry::clearAllSessions()
{
    ShutdownResult result;
    ProfiledMutexLock lock(session_to_response_callback_mutex, ProfileEvents::KeeperSessionCallbackLockWaitMicroseconds, ProfileEvents::KeeperSessionCallbackLockHoldMicroseconds);
    result.sessions.reserve(session_to_response_callback.size());
    for (auto & [session_id, callback] : session_to_response_callback)
        result.sessions.emplace_back(session_id, std::move(callback));
    session_to_response_callback.clear();
    return result;
}

}
