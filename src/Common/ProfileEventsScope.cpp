#include <Common/ProfileEventsScope.h>

namespace DB
{
extern thread_local constinit ProfileEvents::Counters * subthread_profile_events;


ScopedProfileEvents::ScopedProfileEvents(ProfileEvents::Counters * performance_counters_scope_)
    : performance_counters_scope(performance_counters_scope_)
{
    UNUSED(attached);
    CurrentThread::get().attachProfileCountersScope(performance_counters_scope);
}

ScopedProfileEvents::~ScopedProfileEvents()
{
    subthread_profile_events = nullptr;
}


}
