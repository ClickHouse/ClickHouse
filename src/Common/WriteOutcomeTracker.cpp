#include <Common/WriteOutcomeTracker.h>


namespace DB
{

namespace
{

thread_local WriteOutcomeTracker::Scope * current_scope = nullptr;

std::atomic<uint64_t> next_tracker_id = 0;

}

WriteOutcomeTracker::WriteOutcomeTracker()
    : id(++next_tracker_id)
{
}

std::optional<WriteOutcome> WriteOutcomeTracker::getOutcome() const
{
    const auto value = outcome.load();
    if (value == not_set)
        return std::nullopt;
    return static_cast<WriteOutcome>(value);
}

WriteOutcomeTracker::Scope::Scope(std::shared_ptr<WriteOutcomeTracker> tracker_)
    : tracker(std::move(tracker_))
    , previous(current_scope)
{
    current_scope = this;
}

WriteOutcomeTracker::Scope::~Scope()
{
    current_scope = previous;
}

std::shared_ptr<WriteOutcomeTracker> WriteOutcomeTracker::current()
{
    return current_scope ? current_scope->tracker : nullptr;
}

WriteOutcomeTracker * WriteOutcomeTracker::currentRaw()
{
    return current_scope ? current_scope->tracker.get() : nullptr;
}

WriteOutcomeTracker::ExitGuard::ExitGuard(std::shared_ptr<WriteOutcomeTracker> tracker_)
    : tracker(std::move(tracker_))
{
    if (tracker)
        tracker->resetOutcome();
}

WriteOutcomeTracker::ExitGuard::~ExitGuard()
{
    if (tracker && !tracker->getOutcome())
        tracker->setOutcome(tracker->mayHaveBeenApplied() ? WriteOutcome::Unknown : WriteOutcome::NotApplied);
}

void WriteOutcomeTracker::ExitGuard::set(WriteOutcome value)
{
    if (tracker)
        tracker->setOutcome(value);
}

}
