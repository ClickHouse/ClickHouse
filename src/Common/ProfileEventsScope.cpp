#include <Common/ProfileEventsScope.h>
#include <Common/CurrentThread.h>
#include <Common/VariableContext.h>

namespace DB
{

ProfileEventScopeExtension::ProfileEventScopeExtension(ProfileEventsScopePtr to)
    : scope(to)
{
    attach();
}

ProfileEventScopeExtension::~ProfileEventScopeExtension()
{
    detach();
}

void ProfileEventScopeExtension::attach()
{
    if (attached)
        return;

    CurrentThread::get().attachProfileCountersScope(scope->getCounters());
    attached = true;
}

void ProfileEventScopeExtension::detach()
{
    if (!attached)
        return;

    auto detached_scope = CurrentThread::get().detachProfileCountersScope();
    chassert(detached_scope == scope->getCounters());
    attached = false;
}

ProfileEventsScope::ProfileEventsScope()
    : performance_counters_holder(VariableContext::Scope)
{
    performance_counters_holder.setParent(nullptr);
}

std::shared_ptr<ProfileEventsScope> ProfileEventsScope::construct()
{
    struct ProfileEventsScopeCreator : public ProfileEventsScope {};
    return std::make_shared<ProfileEventsScopeCreator>();
}

ProfileEventScopeExtension ProfileEventsScope::startCollecting()
{
    return ProfileEventScopeExtension(shared_from_this());
}

ProfileEvents::CountersPtr ProfileEventsScope::getCounters()
{
    return ProfileEvents::CountersPtr(shared_from_this(), &performance_counters_holder);
}

std::shared_ptr<ProfileEvents::Counters::Snapshot> ProfileEventsScope::getSnapshot() const
{
    return std::make_shared<ProfileEvents::Counters::Snapshot>(performance_counters_holder.getPartiallyAtomicSnapshot());
}

}
