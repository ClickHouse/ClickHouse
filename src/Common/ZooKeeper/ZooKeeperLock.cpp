#include <Common/ZooKeeper/ZooKeeperLock.h>
#include <Common/logger_useful.h>
#include <Common/ErrorCodes.h>
#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

}

namespace fs = std::filesystem;

namespace zkutil
{

ZooKeeperLock::ZooKeeperLock(
    const ZooKeeperPtr & zookeeper_,
    const std::string & lock_prefix_,
    const std::string & lock_name_,
    const std::string & lock_message_,
    bool throw_if_lost_)
    : zookeeper(zookeeper_)
    , lock_path(fs::path(lock_prefix_) / lock_name_)
    , lock_message(lock_message_)
    , throw_if_lost(throw_if_lost_)
    , log(getLogger("zkutil::Lock"))
{
    zookeeper->createIfNotExists(lock_prefix_, "");
    LOG_TRACE(log, "Trying to create zookeeper lock on path {} for session {}", lock_path, zookeeper->getClientID());
}

ZooKeeperLock::~ZooKeeperLock()
{
    try
    {
        unlock();
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

bool ZooKeeperLock::isLocked() const
{
    return locked && !zookeeper->expired();
}

const std::string & ZooKeeperLock::getLockPath() const
{
    return lock_path;
}

void ZooKeeperLock::unlock()
{
    if (!locked)
    {
        LOG_TRACE(log, "Lock on path {} for session {} is not locked, exiting", lock_path, zookeeper->getClientID());
        return;
    }

    locked = false;

    if (zookeeper->expired())
    {
        LOG_WARNING(log, "Lock is lost, because session was expired. Path: {}, message: {}", lock_path, lock_message);
        return;
    }

    Coordination::Stat stat;
    /// NOTE It will throw if session expired after we checked it above
    bool result = zookeeper->exists(lock_path, &stat);

    if (result && stat.ephemeralOwner == zookeeper->getClientID())
    {
        zookeeper->remove(lock_path, -1);
        LOG_TRACE(log, "Lock on path {} for session {} is unlocked", lock_path, zookeeper->getClientID());
    }
    else if (result && throw_if_lost) /// NOTE: What if session expired exactly here?
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Lock is lost, it has another owner. Path: {}, message: {}, owner: {}, our id: {}",
                        lock_path, lock_message, stat.ephemeralOwner, zookeeper->getClientID());
    else if (throw_if_lost)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Lock is lost, node does not exist. Path: {}, message: {}, our id: {}",
                            lock_path, lock_message, zookeeper->getClientID());
    else
        LOG_INFO(log, "Lock is lost, node does not exist. Path: {}, message: {}, our id: {}",
                            lock_path, lock_message, zookeeper->getClientID());
}

bool ZooKeeperLock::tryLock()
{
    Coordination::Error code = zookeeper->tryCreate(lock_path, lock_message, zkutil::CreateMode::Ephemeral);

    if (code == Coordination::Error::ZOK)
    {
        locked = true;
    }
    else if (code != Coordination::Error::ZNODEEXISTS)
    {
        throw Coordination::Exception(code);
    }

    return locked;
}

std::unique_ptr<ZooKeeperLock> createSimpleZooKeeperLock(
    const ZooKeeperPtr & zookeeper, const String & lock_prefix, const String & lock_name, const String & lock_message, bool throw_if_lost)
{
    return std::make_unique<ZooKeeperLock>(zookeeper, lock_prefix, lock_name, lock_message, throw_if_lost);
}


}
