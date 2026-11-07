#include <Common/SessionTimezoneBlockerInThread.h>
#include <Common/FiberLocal.h>
#include <cstdint>

static constinit FiberLocal<uint64_t, FiberLocalSlot::SESSION_TIMEZONE_BLOCKER_COUNTER> session_timezone_blocker_counter;

SessionTimezoneBlockerInThread::SessionTimezoneBlockerInThread()
{
    session_timezone_blocker_counter = session_timezone_blocker_counter + 1;
}

SessionTimezoneBlockerInThread::~SessionTimezoneBlockerInThread()
{
    session_timezone_blocker_counter = session_timezone_blocker_counter - 1;
}

bool SessionTimezoneBlockerInThread::isBlocked()
{
    return session_timezone_blocker_counter > 0;
}
