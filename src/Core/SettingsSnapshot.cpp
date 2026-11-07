#include <Core/SettingsSnapshot.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>

namespace DB
{

bool settingsAllocationIsServerOwned()
{
    if (MemoryTrackerBlockerInThread::isBlocked(VariableContext::User))
        return true;

    for (auto * tracker = CurrentThread::getMemoryTracker(); tracker; tracker = tracker->getParent())
    {
        if (tracker->level == VariableContext::Process || tracker->level == VariableContext::User)
            return false;
    }
    return true;
}

}
