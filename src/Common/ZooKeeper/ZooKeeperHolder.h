#pragma once

#include "ZooKeeper.h"
#include <mutex>
#include <boost/noncopyable.hpp>

namespace zkutil
{
class Lock;

class ZooKeeperHolder : public boost::noncopyable
{
    friend class zkutil::Lock;

protected:
    class UnstorableZookeeperHandler;

public:
    ZooKeeperHolder() = default;

    /// вызывать из одного потока - не thread safe
    template <typename... Args>
    void init(Args &&... args);
    /// был ли класс инициализирован
    bool isInitialized() const { return ptr != nullptr; }

    /// Workaround for zkutil::Lock
    void initFromInstance(const ZooKeeper::Ptr & zookeeper_ptr);

    UnstorableZookeeperHandler getZooKeeper();
    bool replaceZooKeeperSessionToNewOne();

    bool isSessionExpired() const;

protected:
    /** Хендлер для подсчета количества используемых ссылок на ZooKeeper
    *
    *   Запрещается переинициализировать ZooKeeper пока, хранится хотя бы один хендлер на него.
    *   Большинство классов должны хранить хендлер на стеке и не хранить как член класса.
    *   Т.е. хранить holder и запрашивать хендлер перед каждым использованием.
    *   Поэтому класс специально объявлен, как protected.
    *
    *   Исключение - классы, работающие с эфимерными нодами. Пример: zkutil::Lock
    *
    *   Как использовать:
    *   auto zookeeper = zookeeper_holder->getZooKeeper();
    *   zookeeper->get(path);
    */
    class UnstorableZookeeperHandler
    {
    public:
        UnstorableZookeeperHandler(ZooKeeper::Ptr zk_ptr_);

        explicit operator bool() const { return bool(zk_ptr); }
        bool operator==(std::nullptr_t) const { return zk_ptr == nullptr; }
        bool operator!=(std::nullptr_t) const { return !(*this == nullptr); }

        /// в случае nullptr методы разыменования кидают исключение,
        /// с более подробным текстом, чем просто nullptr
        ZooKeeper * operator->();
        const ZooKeeper * operator->() const;
        ZooKeeper & operator*();
        const ZooKeeper & operator*() const;

    private:
        ZooKeeper::Ptr zk_ptr;
    };

private:
    mutable std::mutex mutex;
    ZooKeeper::Ptr ptr;

    Poco::Logger * log = &Poco::Logger::get("ZooKeeperHolder");

    static std::string nullptr_exception_message;
};

template <typename... Args>
void ZooKeeperHolder::init(Args &&... args)
{
    ptr = std::make_shared<ZooKeeper>(std::forward<Args>(args)...);
}

using ZooKeeperHolderPtr = std::shared_ptr<ZooKeeperHolder>;

}
