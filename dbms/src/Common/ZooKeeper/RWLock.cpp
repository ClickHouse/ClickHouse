#include <Common/ZooKeeper/RWLock.h>
#include <Common/StringUtils.h>
#include <algorithm>
#include <iterator>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;
extern const int RWLOCK_ALREADY_HELD;
extern const int RWLOCK_NO_SUCH_LOCK;
extern const int ABORTED;

}

}

namespace zkutil
{

namespace
{

constexpr long wait_duration = 1000;
constexpr auto prefix_length = 2;

template <RWLock::Type>
struct Prefix;

template <>
struct Prefix<RWLock::Read>
{
    static constexpr auto name = "R-";
};

template <>
struct Prefix<RWLock::Write>
{
    static constexpr auto name = "W-";
};

inline bool nodeQueueCmp(const std::string & lhs, const std::string & rhs)
{
    return lhs.compare(prefix_length, std::string::npos, rhs, prefix_length, std::string::npos) < 0;
};

inline bool nodeQueueEquals(const std::string & lhs, const std::string & rhs)
{
    return lhs.compare(prefix_length, std::string::npos, rhs, prefix_length, std::string::npos) == 0;
};

}

RWLock::RWLock(GetZooKeeper get_zookeeper_, const std::string & path_)
    : get_zookeeper{get_zookeeper_}, path{path_}
{
    if (!get_zookeeper)
        throw DB::Exception{"No ZooKeeper accessor specified", DB::ErrorCodes::LOGICAL_ERROR};

    int32_t code = get_zookeeper()->tryCreate(path, "", CreateMode::Persistent);
    if ((code != ZOK) && (code != ZNODEEXISTS))
    {
        if (code == ZNONODE)
            throw DB::Exception{"No such lock", DB::ErrorCodes::RWLOCK_NO_SUCH_LOCK};
        else
            throw KeeperException{code};
    }
}

RWLock::operator bool() const
{
    return get_zookeeper && !path.empty();
}

void RWLock::setCancellationHook(CancellationHook cancellation_hook_)
{
    cancellation_hook = cancellation_hook_;
}

void RWLock::acquireRead(Mode mode)
{
    acquireImpl<RWLock::Read>(mode);
}

void RWLock::acquireWrite(Mode mode)
{
    acquireImpl<RWLock::Write>(mode);
}

void RWLock::release()
{
    __sync_synchronize();

    if (!*this)
    {
        owns_lock = false;
        return;
    }

    if (key.empty())
        throw DB::Exception{"RWLock: no lock is held", DB::ErrorCodes::LOGICAL_ERROR};

    get_zookeeper()->tryRemoveEphemeralNodeWithRetries(path + "/" + key);
    key.clear();
    owns_lock = false;
}

template <typename RWLock::Type lock_type>
void RWLock::acquireImpl(Mode mode)
{
    static_assert((lock_type == RWLock::Read) || (lock_type == RWLock::Write), "Invalid RWLock type");

    __sync_synchronize();

    if (!*this)
    {
        owns_lock = true;
        return;
    }

    if (!key.empty())
        throw DB::Exception{"RWLock: lock already held", DB::ErrorCodes::RWLOCK_ALREADY_HELD};

    try
    {
        /// Enqueue a new request for a lock.
        int32_t code = get_zookeeper()->tryCreate(path + "/" + Prefix<lock_type>::name,
            "", CreateMode::EphemeralSequential, key);
        if (code == ZNONODE)
            throw DB::Exception{"No such lock", DB::ErrorCodes::RWLOCK_NO_SUCH_LOCK};
        else if (code != ZOK)
            throw KeeperException(code);

        key = key.substr(path.length() + 1);

        while (true)
        {
            auto zookeeper = get_zookeeper();

            std::vector<std::string> children;
            int32_t code = zookeeper->tryGetChildren(path, children);
            if (code == ZNONODE)
                throw DB::Exception{"No such lock", DB::ErrorCodes::RWLOCK_NO_SUCH_LOCK};
            else if (code != ZOK)
                throw KeeperException{code};

            std::sort(children.begin(), children.end(), nodeQueueCmp);
            auto it = std::lower_bound(children.cbegin(), children.cend(), key, nodeQueueCmp);

            /// This should never happen.
            if ((it == children.cend()) || !nodeQueueEquals(*it, key))
                throw DB::Exception{"RWLock: corrupted lock request queue. Own request not found.",
                    DB::ErrorCodes::LOGICAL_ERROR};

            const std::string * observed_key = nullptr;

            if (lock_type == RWLock::Read)
            {
                /// Look for the first write lock request that is older than us.
                auto it2 = std::find_if(
                    std::make_reverse_iterator(it),
                    children.crend(),
                    [](const std::string & child)
                    {
                        return startsWith(child, Prefix<RWLock::Write>::name);
                    });
                if (it2 != children.crend())
                    observed_key = &*it2;
            }
            else if (lock_type == RWLock::Write)
            {
                if (it != children.cbegin())
                {
                    /// Take the next lock request that is older than us.
                    auto it2 = std::prev(it);
                    observed_key = &*it2;
                }
            }

            if (observed_key == nullptr)
            {
                /// We hold the lock.
                owns_lock = true;
                break;
            }

            if (mode == NonBlocking)
            {
                int32_t code = zookeeper->tryRemoveEphemeralNodeWithRetries(path + "/" + key);
                if (code == ZNONODE)
                    throw DB::Exception{"No such lock", DB::ErrorCodes::RWLOCK_NO_SUCH_LOCK};
                else if (code != ZOK)
                    throw KeeperException{code};

                key.clear();
                break;
            }

            abortIfRequested();

            /// Wait for our turn to come.
            if (zookeeper->exists(path + "/" + *observed_key, nullptr, event))
            {
                do
                {
                    abortIfRequested();
                }
                while (!event->tryWait(wait_duration));
            }
        }
    }
    catch (...)
    {
        try
        {
            if (!key.empty())
                get_zookeeper()->tryRemoveEphemeralNodeWithRetries(path + "/" + key);
        }
        catch (...)
        {
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        throw;
    }
}

bool RWLock::ownsLock() const
{
    return owns_lock;
}

void RWLock::abortIfRequested()
{
    if (cancellation_hook)
        cancellation_hook();
}

}
