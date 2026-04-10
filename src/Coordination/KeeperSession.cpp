#include <Coordination/KeeperSession.h>

#include <Common/logger_useful.h>


namespace DB
{

KeeperSession::KeeperSession(int64_t session_id_, ResponseCallback callback_)
    : session_id(session_id_)
    , callback(std::move(callback_))
{
}

bool KeeperSession::canAcceptRequests() const
{
    std::lock_guard lock(mutex);
    return state == SessionState::Active;
}

bool KeeperSession::deliverResponse(const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr request)
{
    std::lock_guard lock(mutex);

    if (state == SessionState::Closed)
        return false;

    if (!callback)
        return false;

    try
    {
        callback(response, std::move(request));
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return true;
}

void KeeperSession::close()
{
    std::lock_guard lock(mutex);
    state = SessionState::Closed;
    callback = {};
}

KeeperSession::SessionState KeeperSession::getState() const
{
    std::lock_guard lock(mutex);
    return state;
}

}
