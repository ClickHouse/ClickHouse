#include "KeeperException.h"
#include "Lock.h"


using namespace zkutil;

bool Lock::tryLock()
{
    auto zookeeper = zookeeper_holder->getZooKeeper();
    if (locked)
    {
        /// проверим, что нода создана и я ее владелец
        if (tryCheck() != Status::LOCKED_BY_ME)
            locked.reset(nullptr);
    }
    else
    {
        std::string dummy;
        int32_t code = zookeeper->tryCreate(lock_path, lock_message, zkutil::CreateMode::Ephemeral, dummy);

        if (code == Coordination::ZNODEEXISTS)
        {
            locked.reset(nullptr);
        }
        else if (code == Coordination::ZOK)
        {
            locked.reset(new ZooKeeperHandler(zookeeper));
        }
        else
        {
            throw Coordination::Exception(code);
        }
    }
    return bool(locked);
}

void Lock::unlock()
{
    if (locked)
    {
        auto zookeeper = zookeeper_holder->getZooKeeper();
        if (tryCheck() == Status::LOCKED_BY_ME)
            zookeeper->remove(lock_path, -1);
        locked.reset(nullptr);
    }
}

Lock::Status Lock::tryCheck() const
{
    auto zookeeper = zookeeper_holder->getZooKeeper();

    Status lock_status;
    Coordination::Stat stat;
    std::string dummy;
    bool result = zookeeper->tryGet(lock_path, dummy, &stat);
    if (!result)
        lock_status = UNLOCKED;
    else
    {
        if (stat.ephemeralOwner == zookeeper->getClientID())
            lock_status = LOCKED_BY_ME;
        else
            lock_status = LOCKED_BY_OTHER;
    }

    if (locked && lock_status != LOCKED_BY_ME)
        LOG_WARNING(log, "Lock is lost. It is normal if session was expired. Path: " << lock_path << "/" << lock_message);

    return lock_status;
}

void Lock::unlockAssumeLockNodeRemovedManually()
{
    locked.reset(nullptr);
}

