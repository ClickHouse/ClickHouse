#include <Common/ActionLock.h>
#include <Common/ActionBlocker.h>


namespace DB
{

ActionLock::ActionLock(const ActionBlocker & blocker) : counter_ptr(blocker.counter)
{
    if (auto counter = counter_ptr.lock())
        ++(*counter);
}

ActionLock::ActionLock(ActionLock && other) noexcept : counter_ptr(std::move(other.counter_ptr))
{
}

ActionLock & ActionLock::operator=(ActionLock && other) noexcept
{
    auto lock_lhs = this->counter_ptr.lock();

    counter_ptr = std::move(other.counter_ptr);
    /// After move other.counter_ptr still points to counter, reset it explicitly
    other.counter_ptr.reset();

    if (lock_lhs)
        --(*lock_lhs);

    return *this;
}

}
