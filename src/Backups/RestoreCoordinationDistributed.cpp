#include <Backups/RestoreCoordinationDistributed.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>


namespace DB
{

RestoreCoordinationDistributed::RestoreCoordinationDistributed(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_)
    : zookeeper_path(zookeeper_path_), get_zookeeper(get_zookeeper_)
{
    createRootNodes();
}

RestoreCoordinationDistributed::~RestoreCoordinationDistributed() = default;

void RestoreCoordinationDistributed::createRootNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
    zookeeper->createIfNotExists(zookeeper_path + "/paths_in_backup", "");
    zookeeper->createIfNotExists(zookeeper_path + "/acquired", "");
}

void RestoreCoordinationDistributed::removeAllNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->removeRecursive(zookeeper_path);
}

void RestoreCoordinationDistributed::setOrGetPathInBackupForZkPath(const String & zk_path_, String & path_in_backup_)
{
    {
        std::lock_guard lock{mutex};
        auto it = paths_in_backup_by_zk_path.find(zk_path_);
        if (it != paths_in_backup_by_zk_path.end())
        {
            path_in_backup_ = it->second;
            return;
        }
    }

    auto zookeeper = get_zookeeper();
    String combined_path = zookeeper_path + "/paths_in_backup/" + escapeForFileName(zk_path_);
    auto code = zookeeper->tryCreate(combined_path, path_in_backup_, zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, combined_path);

    if (code == Coordination::Error::ZNODEEXISTS)
        path_in_backup_ = zookeeper->get(combined_path);

    {
        std::lock_guard lock{mutex};
        paths_in_backup_by_zk_path[zk_path_] = path_in_backup_;
    }
}

bool RestoreCoordinationDistributed::acquireZkPathAndName(const String & zk_path_, const String & name_)
{
    std::pair<String, String> key{zk_path_, name_};

    {
        std::lock_guard lock{mutex};
        if (acquired.contains(key))
            return true;
    }

    auto zookeeper = get_zookeeper();
    String combined_path = zookeeper_path + "/acquired/" + escapeForFileName(zk_path_) + "|" + escapeForFileName(name_);
    auto code = zookeeper->tryCreate(combined_path, "", zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, combined_path);

    if (code == Coordination::Error::ZNODEEXISTS)
        return false;

    {
        std::lock_guard lock{mutex};
        acquired.emplace(key, std::nullopt);
        return true;
    }
}

void RestoreCoordinationDistributed::setResultForZkPathAndName(const String & zk_path_, const String & name_, Result res_)
{
    auto zookeeper = get_zookeeper();
    String combined_path = zookeeper_path + "/acquired/" + escapeForFileName(zk_path_) + "|" + escapeForFileName(name_);
    zookeeper->set(combined_path, (res_ == Result::SUCCEEDED) ? "1" : "0");

    {
        std::lock_guard lock{mutex};
        acquired[std::pair{zk_path_, name_}] = res_;
    }
}

bool RestoreCoordinationDistributed::getResultForZkPathAndName(const String & zk_path_, const String & name_, Result & res_, std::chrono::milliseconds timeout_) const
{
    {
        std::lock_guard lock{mutex};
        auto value = acquired[std::pair{zk_path_, name_}];
        if (value)
        {
            res_ = *value;
            return true;
        }
    }

    auto zookeeper = get_zookeeper();
    String combined_path = zookeeper_path + "/acquired/" + escapeForFileName(zk_path_) + "|" + escapeForFileName(name_);

    std::atomic<bool> changed = false;
    std::condition_variable changed_condvar;
    const auto watch = [&changed, &changed_condvar, zk_path_, name_](const Coordination::WatchResponse &)
    {
        changed = true;
        changed_condvar.notify_one();
    };

    String res_str = zookeeper->getWatch(combined_path, nullptr, watch);
    if (res_str.empty())
    {
        std::mutex dummy_mutex;
        std::unique_lock lock{dummy_mutex};
        changed_condvar.wait_for(lock, timeout_, [&changed] { return changed.load(); });
        res_str = zookeeper->get(combined_path);
    }

    if (res_str.empty())
        return false;

    res_ = (res_str == "1") ? Result::SUCCEEDED : Result::FAILED;

    {
        std::lock_guard lock{mutex};
        acquired[std::pair{zk_path_, name_}] = res_;
    }

    return true;

}

void RestoreCoordinationDistributed::drop()
{
    removeAllNodes();
}

}
