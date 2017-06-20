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
class AbandonableLockInZooKeeper : private boost::noncopyable
{
public:
    enum State
    {
        UNLOCKED,
        LOCKED,
        ABANDONED,
    };

    AbandonableLockInZooKeeper(
        const String & path_prefix_, const String & temp_path, zkutil::ZooKeeper & zookeeper_)
        : zookeeper(zookeeper_), path_prefix(path_prefix_)
    {
        /// Let's create an secondary ephemeral node.
        holder_path = zookeeper.create(temp_path + "/abandonable_lock-", "", zkutil::CreateMode::EphemeralSequential);

        /// Write the path to the secondary node in the main node.
        path = zookeeper.create(path_prefix, holder_path, zkutil::CreateMode::PersistentSequential);

        if (path.size() <= path_prefix.size())
            throw Exception("Logical error: name of sequential node is shorter than prefix.", ErrorCodes::LOGICAL_ERROR);
    }

    AbandonableLockInZooKeeper(AbandonableLockInZooKeeper && rhs)
        : zookeeper(rhs.zookeeper)
    {
        std::swap(path_prefix, rhs.path_prefix);
        std::swap(path, rhs.path);
        std::swap(holder_path, rhs.holder_path);
    }

    String getPath() const
    {
        return path;
    }

    /// Parse the number at the end of the path.
    UInt64 getNumber() const
    {
        return parse<UInt64>(path.c_str() + path_prefix.size(), path.size() - path_prefix.size());
    }

    void unlock()
    {
        zookeeper.remove(path);
        zookeeper.remove(holder_path);
        holder_path = "";
    }

    /// Adds actions equivalent to `unlock()` to the list.
    void getUnlockOps(zkutil::Ops & ops)
    {
        ops.emplace_back(std::make_unique<zkutil::Op::Remove>(path, -1));
        ops.emplace_back(std::make_unique<zkutil::Op::Remove>(holder_path, -1));
    }

    ~AbandonableLockInZooKeeper()
    {
        if (holder_path.empty())
            return;

        try
        {
            zookeeper.tryRemoveEphemeralNodeWithRetries(holder_path);
            zookeeper.trySet(path, ""); /// It's not necessary.
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
        if (zookeeper.trySet(path, "") == ZOK)
            return ABANDONED;

        return UNLOCKED;
    }

    static void createAbandonedIfNotExists(const String & path, zkutil::ZooKeeper & zookeeper)
    {
        zookeeper.createIfNotExists(path, "");
    }

private:
    zkutil::ZooKeeper & zookeeper;
    String path_prefix;
    String path;
    String holder_path;
};

}
