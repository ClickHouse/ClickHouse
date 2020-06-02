#pragma once

#include "ZooKeeperHolder.h"
#include <common/logger_useful.h>
#include <Common/Exception.h>

namespace zkutil
{
    class Lock
    {
    public:
        /// lock_prefix - относительный путь до блокировки в ZK. Начинается со слеша
        /// lock_name - имя ноды блокировки в ZK
        Lock(
            zkutil::ZooKeeperHolderPtr zookeeper_holder_,
            const std::string & lock_prefix_,
            const std::string & lock_name_,
            const std::string & lock_message_ = "",
            bool create_parent_path_ = false)
        :
            zookeeper_holder(zookeeper_holder_),
            lock_path(lock_prefix_ + "/" + lock_name_),
            lock_message(lock_message_),
            log(&Poco::Logger::get("zkutil::Lock"))
        {
            auto zookeeper = zookeeper_holder->getZooKeeper();
            if (create_parent_path_)
                zookeeper->createAncestors(lock_prefix_);

            zookeeper->createIfNotExists(lock_prefix_, "");
        }

        Lock(const Lock &) = delete;
        Lock(Lock && lock) = default;
        Lock & operator=(const Lock &) = delete;

        ~Lock()
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

        enum Status
        {
            UNLOCKED,
            LOCKED_BY_ME,
            LOCKED_BY_OTHER,
        };

        /// проверяет создана ли эфемерная нода и кто ее владелец.
        Status tryCheck() const;

        void unlock();
        void unlockAssumeLockNodeRemovedManually();

        bool tryLock();

        /// путь к ноде блокировки в zookeeper
        const std::string & getPath() { return lock_path; }

    private:
        zkutil::ZooKeeperHolderPtr zookeeper_holder;
        /// пока храним указатель на хендлер, никто не может переиницализировать сессию с zookeeper
        using ZooKeeperHandler = zkutil::ZooKeeperHolder::UnstorableZookeeperHandler;
        std::unique_ptr<ZooKeeperHandler> locked;

        std::string lock_path;
        std::string lock_message;
        Poco::Logger * log;

    };
}
