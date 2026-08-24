#pragma once

#include <Core/Types_fwd.h>
#include <base/types.h>

#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include <boost/noncopyable.hpp>

namespace DB
{

struct SessionInfo
{
    const String session_id;
};

using SessionInfos = std::list<SessionInfo>;

using SessionsForUser = std::unordered_map<UUID, SessionInfos>;

class SessionTracker;

class SessionTracker
{
public:
    class Session : boost::noncopyable
    {
    public:
        explicit Session(SessionTracker & tracker_,
                         const UUID & user_id_,
                         SessionInfos::const_iterator session_info_iter_) noexcept;

        ~Session();

    private:
        friend class SessionTracker;

        SessionTracker & tracker;
        const UUID user_id;
        const SessionInfos::const_iterator session_info_iter;
    };

    using SessionTrackerHandle = std::unique_ptr<SessionTracker::Session>;

    SessionTrackerHandle trackSession(const UUID & user_id,
                                      const SessionInfo & session_info,
                                      size_t max_sessions_for_user);

private:
    /// disallow manual messing with session tracking
    friend class Session;

    std::mutex mutex;
    SessionsForUser sessions_for_user TSA_GUARDED_BY(mutex);

    void stopTracking(const UUID& user_id, SessionInfos::const_iterator session_info_iter);
};

}
