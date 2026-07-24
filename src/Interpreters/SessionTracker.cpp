#include <Interpreters/SessionTracker.h>

#include <Core/Field.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int USER_SESSION_LIMIT_EXCEEDED;
}

SessionTracker::Session::Session(SessionTracker & tracker_,
                                 const UUID& user_id_,
                                 SessionInfos::const_iterator session_info_iter_) noexcept
    : tracker(tracker_), user_id(user_id_), session_info_iter(session_info_iter_)
{
}

SessionTracker::Session::~Session()
{
    tracker.stopTracking(user_id, session_info_iter);
}

SessionTracker::SessionTrackerHandle
SessionTracker::trackSession(const UUID & user_id,
                             const SessionInfo & session_info,
                             size_t max_sessions_for_user)
{
    std::lock_guard lock(mutex);

    auto sessions_for_user_iter = sessions_for_user.find(user_id);
    if (sessions_for_user_iter == sessions_for_user.end())
        sessions_for_user_iter = sessions_for_user.emplace(user_id, SessionInfos()).first;

    SessionInfos & session_infos = sessions_for_user_iter->second;
    if (max_sessions_for_user && session_infos.size() >= max_sessions_for_user)
    {
        throw Exception(ErrorCodes::USER_SESSION_LIMIT_EXCEEDED,
                        "User {} has overflown session count {}",
                        toString(user_id),
                        max_sessions_for_user);
    }

    session_infos.emplace_front(session_info);

    return std::make_unique<SessionTracker::Session>(*this, user_id, session_infos.begin());
}

void SessionTracker::stopTracking(const UUID& user_id, SessionInfos::const_iterator session_info_iter)
{
    std::lock_guard lock(mutex);

    auto sessions_for_user_iter = sessions_for_user.find(user_id);
    chassert(sessions_for_user_iter != sessions_for_user.end());

    sessions_for_user_iter->second.erase(session_info_iter);
    if (sessions_for_user_iter->second.empty())
        sessions_for_user.erase(sessions_for_user_iter);
}

}
