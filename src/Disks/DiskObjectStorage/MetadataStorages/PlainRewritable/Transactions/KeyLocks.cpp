#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/KeyLocks.h>

#include <Common/UniqueLock.h>

#include <utility>

namespace DB
{

KeyLocks::Guard::Guard(KeyLocks & locks_, Requests::iterator request_)
    : locks(&locks_)
    , request(request_)
{
}

KeyLocks::Guard::Guard(Guard && other) noexcept
    : locks(std::exchange(other.locks, nullptr))
    , request(other.request)
{
}

KeyLocks::Guard & KeyLocks::Guard::operator=(Guard && other) noexcept
{
    if (this != &other)
    {
        if (locks)
            locks->unlock(request);

        locks = std::exchange(other.locks, nullptr);
        request = other.request;
    }

    return *this;
}

KeyLocks::Guard::~Guard()
{
    if (locks)
        locks->unlock(request);
}

bool KeyLocks::conflicts(const Keys & lhs, const Keys & rhs)
{
    const auto & smaller = lhs.size() < rhs.size() ? lhs : rhs;
    const auto & larger = lhs.size() < rhs.size() ? rhs : lhs;

    for (const auto & key : smaller)
        if (larger.contains(key))
            return true;

    return false;
}

KeyLocks::Guard KeyLocks::lock(Keys keys)
{
    if (keys.empty())
        return {};

    UniqueLock lock(mutex);

    const auto request = requests.emplace(requests.end(), std::move(keys));

    released.wait(lock.getUnderlyingLock(), [&]() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        for (auto earlier = requests.begin(); earlier != request; ++earlier)
            if (conflicts(*earlier, *request))
                return false;

        return true;
    });

    return Guard(*this, request);
}

void KeyLocks::unlock(Requests::iterator request)
{
    {
        std::lock_guard guard(mutex);
        requests.erase(request);
    }

    released.notify_all();
}

}
