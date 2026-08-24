#pragma once

#include <Common/ProfileEvents.h>

#include <boost/noncopyable.hpp>

namespace DB
{

/// Use specific performance counters for current thread in the current scope.
class ProfileEventsScope : private boost::noncopyable
{
public:
    /// Counters are owned by this object.
    ProfileEventsScope();

    /// Shared counters are stored outside.
    /// Useful when we calculate metrics entering into some scope several times.
    explicit ProfileEventsScope(ProfileEvents::Counters * performance_counters_scope_);

    std::shared_ptr<ProfileEvents::Counters::Snapshot> getSnapshot();

    ~ProfileEventsScope();

private:
    /// If set, then performance_counters_scope is owned by this object.
    /// Otherwise, counters are passed to the constructor from outside.
    std::unique_ptr<ProfileEvents::Counters> performance_counters_holder;

    ProfileEvents::Counters * performance_counters_scope;
    ProfileEvents::Counters * previous_counters_scope;
};


}

