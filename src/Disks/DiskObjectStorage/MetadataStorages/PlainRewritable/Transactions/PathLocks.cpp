#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/PathLocks.h>

#include <Common/UniqueLock.h>

#include <algorithm>
#include <utility>

namespace DB
{

PathLocks::Guard::Guard(PathLocks & locks_, Requests::iterator request_)
    : locks(&locks_)
    , request(request_)
{
}

PathLocks::Guard::Guard(Guard && other) noexcept
    : locks(std::exchange(other.locks, nullptr))
    , request(other.request)
{
}

PathLocks::Guard & PathLocks::Guard::operator=(Guard && other) noexcept
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

PathLocks::Guard::~Guard()
{
    if (locks)
        locks->unlock(request);
}

bool PathLocks::conflicts(const std::string & lhs, const std::string & rhs)
{
    if (lhs.empty() || rhs.empty() || lhs == rhs)
        return true;

    const auto & shorter = lhs.size() < rhs.size() ? lhs : rhs;
    const auto & longer = lhs.size() < rhs.size() ? rhs : lhs;

    return longer.starts_with(shorter) && longer[shorter.size()] == '/';
}

bool PathLocks::conflicts(const Request & lhs, const Request & rhs)
{
    for (const auto & lhs_path : lhs.paths)
        for (const auto & rhs_path : rhs.paths)
            if (conflicts(lhs_path, rhs_path))
                return true;

    return false;
}

PathLocks::Guard PathLocks::lock(std::vector<std::string> paths)
{
    std::sort(paths.begin(), paths.end());
    paths.erase(std::unique(paths.begin(), paths.end()), paths.end());

    UniqueLock lock(mutex);

    const auto request = requests.emplace(requests.end(), Request{std::move(paths)});

    released.wait(lock.getUnderlyingLock(), [&]() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        for (auto earlier = requests.begin(); earlier != request; ++earlier)
            if (conflicts(*earlier, *request))
                return false;

        return true;
    });

    return Guard(*this, request);
}

PathLocks::Guard PathLocks::lockAll()
{
    return lock({std::string()});
}

std::optional<PathLocks::Guard> PathLocks::tryLockAll()
{
    UniqueLock lock(mutex);

    if (!requests.empty())
        return std::nullopt;

    const auto request = requests.emplace(requests.end(), Request{{std::string()}});
    return Guard(*this, request);
}

void PathLocks::unlock(Requests::iterator request)
{
    {
        std::lock_guard guard(mutex);
        requests.erase(request);
    }

    released.notify_all();
}

}
