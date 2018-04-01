#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/** The synchronization is primitive. Works as follows:
  * Creates a non-ephemeral incremental node and marks it as locked (LOCKED).
  * `unlock()` unlocks it (UNLOCKED).
  * When the destructor is called or the session ends in ZooKeeper, it goes into the ABANDONED state.
  * (Including when the program is halted).
  */
class AbandonableLockInZooKeeper : public boost::noncopyable
{
public:
    enum State
    {
        UNLOCKED,
        LOCKED,
        ABANDONED,
    };

    AbandonableLockInZooKeeper(
        const String & path_prefix_, const String & temp_path, zkutil::ZooKeeper & zookeeper_, zkutil::Requests * precheck_ops = nullptr)
        : zookeeper(&zookeeper_), path_prefix(path_prefix_)
    {
        String abandonable_path = temp_path + "/abandonable_lock-";

        /// Let's create an secondary ephemeral node.
        if (!precheck_ops || precheck_ops->empty())
        {
            holder_path = zookeeper->create(abandonable_path, "", zkutil::CreateMode::EphemeralSequential);
        }
        else
        {
            precheck_ops->emplace_back(zkutil::makeCreateRequest(abandonable_path, "", zkutil::CreateMode::EphemeralSequential));
            zkutil::Responses op_results = zookeeper->multi(*precheck_ops);
            holder_path = dynamic_cast<const zkutil::CreateResponse &>(*op_results.back()).path_created;
        }

        /// Write the path to the secondary node in the main node.
        path = zookeeper->create(path_prefix, holder_path, zkutil::CreateMode::PersistentSequential);

        if (path.size() <= path_prefix.size())
            throw Exception("Logical error: name of sequential node is shorter than prefix.", ErrorCodes::LOGICAL_ERROR);
    }

    AbandonableLockInZooKeeper() = default;

    AbandonableLockInZooKeeper(AbandonableLockInZooKeeper && rhs) noexcept
    {
        *this = std::move(rhs);
    }

    AbandonableLockInZooKeeper & operator=(AbandonableLockInZooKeeper && rhs) noexcept
    {
        zookeeper = rhs.zookeeper;
        rhs.zookeeper = nullptr;
        path_prefix = std::move(rhs.path_prefix);
        path = std::move(rhs.path);
        holder_path = std::move(rhs.holder_path);
        return *this;
    }

    bool isCreated() const
    {
        return zookeeper && !holder_path.empty() && !path.empty();
    }

    String getPath() const
    {
        checkCreated();
        return path;
    }

    /// Parse the number at the end of the path.
    UInt64 getNumber() const
    {
        checkCreated();
        return parse<UInt64>(path.c_str() + path_prefix.size(), path.size() - path_prefix.size());
    }

    void unlock()
    {
        checkCreated();
        zookeeper->remove(path);
        zookeeper->remove(holder_path);
        holder_path = "";
    }

    /// Adds actions equivalent to `unlock()` to the list.
    void getUnlockOps(zkutil::Requests & ops)
    {
        checkCreated();
        ops.emplace_back(zkutil::makeRemoveRequest(path, -1));
        ops.emplace_back(zkutil::makeRemoveRequest(holder_path, -1));
    }

    /// Do not delete nodes in destructor. You may call this method after 'getUnlockOps' and successful execution of these ops,
    ///  because the nodes will be already deleted.
    void assumeUnlocked()
    {
        holder_path.clear();
    }

    void checkCreated() const
    {
        if (!isCreated())
            throw Exception("AbandonableLock is not created", ErrorCodes::LOGICAL_ERROR);
    }

    ~AbandonableLockInZooKeeper()
    {
        if (!zookeeper || holder_path.empty())
            return;

        try
        {
            zookeeper->tryRemove(holder_path);
            zookeeper->trySet(path, ""); /// It's not necessary.
        }
        catch (...)
        {
            tryLogCurrentException("~AbandonableLockInZooKeeper");
        }
    }

    static State check(const String & path, zkutil::ZooKeeper & zookeeper)
    {
        String holder_path;

        /// If there is no main node, UNLOCKED.
        if (!zookeeper.tryGet(path, holder_path))
            return UNLOCKED;

        /// If there is no path to the secondary node in the main node, ABANDONED.
        if (holder_path.empty())
            return ABANDONED;

        /// If the secondary node is alive, LOCKED.
        if (zookeeper.exists(holder_path))
            return LOCKED;

        /// If there is no secondary node, you need to test again the existence of the main node,
        /// because during this time you might have time to call unlock().
        /// At the same time, we will remove the path to the secondary node from there.
        if (zookeeper.trySet(path, "") == ZooKeeperImpl::ZooKeeper::ZOK)
            return ABANDONED;

        return UNLOCKED;
    }

    static void createAbandonedIfNotExists(const String & path, zkutil::ZooKeeper & zookeeper)
    {
        zookeeper.createIfNotExists(path, "");
    }

private:
    zkutil::ZooKeeper * zookeeper = nullptr;
    String path_prefix;
    String path;
    String holder_path;
};

}
