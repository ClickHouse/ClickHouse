#include <Common/ProfileEventsScope.h>

namespace DB
{
extern thread_local constinit ProfileEvents::Counters * subthread_profile_events;


ScopedProfileEvents::ScopedProfileEvents()
    : performance_counters_holder(std::make_unique<ProfileEvents::Counters>())
    , performance_counters_scope(performance_counters_holder.get())
{
    CurrentThread::get().attachProfileCountersScope(performance_counters_scope);
}

ScopedProfileEvents::ScopedProfileEvents(ProfileEvents::Counters * performance_counters_scope_)
    : performance_counters_scope(performance_counters_scope_)
{
    CurrentThread::get().attachProfileCountersScope(performance_counters_scope);
}

std::shared_ptr<ProfileEvents::Counters::Snapshot> ScopedProfileEvents::getSnapshot()
{
    return std::make_shared<ProfileEvents::Counters::Snapshot>(performance_counters_scope->getPartiallyAtomicSnapshot());
}

ScopedProfileEvents::~ScopedProfileEvents()
{
    subthread_profile_events = nullptr;
}


}
