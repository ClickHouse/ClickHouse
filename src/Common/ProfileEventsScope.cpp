#include <Common/CurrentThread.h>
#include <Common/ProfileEventsScope.h>

namespace DB
{


ProfileEventsScope::ProfileEventsScope()
    : performance_counters_holder(std::make_unique<ProfileEvents::Counters>())
    , performance_counters_scope(performance_counters_holder.get())
    , previous_counters_scope(CurrentThread::get().attachProfileCountersScope(performance_counters_scope))
{
}

ProfileEventsScope::ProfileEventsScope(ProfileEvents::Counters * performance_counters_scope_)
    : performance_counters_scope(performance_counters_scope_)
    , previous_counters_scope(CurrentThread::get().attachProfileCountersScope(performance_counters_scope))
{
}

std::shared_ptr<ProfileEvents::Counters::Snapshot> ProfileEventsScope::getSnapshot()
{
    return std::make_shared<ProfileEvents::Counters::Snapshot>(performance_counters_scope->getPartiallyAtomicSnapshot());
}

ProfileEventsScope::~ProfileEventsScope()
{
    /// Restore previous performance counters
    CurrentThread::get().attachProfileCountersScope(previous_counters_scope);
}


}
