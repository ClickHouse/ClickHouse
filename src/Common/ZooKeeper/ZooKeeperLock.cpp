#include <Common/ZooKeeper/ZooKeeperLock.h>
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
    const std::string & lock_message_)
    : zookeeper(zookeeper_)
    , lock_path(fs::path(lock_prefix_) / lock_name_)
    , lock_message(lock_message_)
    , log(&Poco::Logger::get("zkutil::Lock"))
{
    zookeeper->createIfNotExists(lock_prefix_, "");
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

void ZooKeeperLock::unlock()
{
    if (!locked)
        return;

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
        zookeeper->remove(lock_path, -1);
    else if (result)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Lock is lost, it has another owner. Path: {}, message: {}, owner: {}, our id: {}",
                        lock_path, lock_message, stat.ephemeralOwner, zookeeper->getClientID());
    else
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Lock is lost, node does not exist. Path: {}, message: {}", lock_path, lock_message);
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
    const ZooKeeperPtr & zookeeper, const String & lock_prefix, const String & lock_name, const String & lock_message)
{
    return std::make_unique<ZooKeeperLock>(zookeeper, lock_prefix, lock_name, lock_message);
}


}
