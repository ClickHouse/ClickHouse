#pragma once

/// While an object of this class lives, `DateLUT::instance` does not use the `session_timezone` of the query,
/// so it returns the time zone of background merges, which run without a query.
struct SessionTimezoneBlockerInThread
{
    SessionTimezoneBlockerInThread();
    ~SessionTimezoneBlockerInThread();
    SessionTimezoneBlockerInThread(const SessionTimezoneBlockerInThread &) = delete;
    SessionTimezoneBlockerInThread & operator=(const SessionTimezoneBlockerInThread &) = delete;

    static bool isBlocked();
};
