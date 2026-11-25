#pragma once

/// While an object of this class lives, `DateLUT::instance` does not use the `session_timezone` of the query,
/// so it returns the time zone of background merges: the `session_timezone` of the background profile, else the server one.
struct SessionTimezoneBlockerInThread
{
    SessionTimezoneBlockerInThread();
    ~SessionTimezoneBlockerInThread();
    SessionTimezoneBlockerInThread(const SessionTimezoneBlockerInThread &) = delete;
    SessionTimezoneBlockerInThread & operator=(const SessionTimezoneBlockerInThread &) = delete;

    static bool isBlocked();
};
