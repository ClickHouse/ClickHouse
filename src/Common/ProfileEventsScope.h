#pragma once

#include <Common/ProfileEvents.h>

#include <memory>

namespace DB
{

class ProfileEventsScope;
using ProfileEventsScopePtr = std::shared_ptr<ProfileEventsScope>;

class ProfileEventScopeExtension
{
public:
    explicit ProfileEventScopeExtension(ProfileEventsScopePtr to);
    ~ProfileEventScopeExtension();

    void attach();
    void detach();

private:
    ProfileEventsScopePtr scope;
    bool attached = false;
};

/// Use specific performance counters for current thread in the current scope.
class ProfileEventsScope : public std::enable_shared_from_this<ProfileEventsScope>
{
    explicit ProfileEventsScope();

public:
    static std::shared_ptr<ProfileEventsScope> construct();

    ProfileEventScopeExtension startCollecting();
    ProfileEvents::CountersPtr getCounters();
    std::shared_ptr<ProfileEvents::Counters::Snapshot> getSnapshot() const;

private:
    ProfileEvents::Counters performance_counters_holder;
};

}

