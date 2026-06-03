#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <Poco/Timespan.h>
#include <boost/noncopyable.hpp>

#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents
{
    extern const Event ConnectionPoolIsFullMicroseconds;
}

/** What is given to the user. */
template <typename TObject>
class PoolEntry
{
public:
    PoolEntry() = default;    /// For deferred initialization.

    /** The `Entry` object protects the resource from being used by another thread.
      * The following methods are forbidden for `rvalue`, so you can not write a similar to
      *
      * auto q = pool.get()->query("SELECT .."); // Oops, after this line Entry was destroyed
      * q.execute (); // Someone else can use this Connection
      */
    TObject * operator->() && = delete;
    const TObject * operator->() const && = delete;
    TObject & operator*() && = delete;
    const TObject & operator*() const && = delete;

    TObject * operator->() &             { return data->object; }
    const TObject * operator->() const & { return data->object; }
    TObject & operator*() &              { return *data->object; }
    const TObject & operator*() const &  { return *data->object; }

    /**
     * Expire an object to make it reallocated later.
     */
    void expire()
    {
        data->is_expired->store(true);
    }

    bool isNull() const { return data == nullptr; }

private:
    template <typename, typename, typename> friend class PoolBase;

    struct PoolEntryHelper
    {
        PoolEntryHelper(TObject * object_, std::atomic<bool> * is_expired_, std::function<void()> on_destroy_)
            : object(object_), is_expired(is_expired_), on_destroy(std::move(on_destroy_)) {}
        ~PoolEntryHelper() { on_destroy(); }

        TObject * object;
        std::atomic<bool> * is_expired;
        std::function<void()> on_destroy;
    };

    std::shared_ptr<PoolEntryHelper> data;

    explicit PoolEntry(std::shared_ptr<PoolEntryHelper> data_) : data(std::move(data_)) {}
};

/** A class from which you can inherit and get a pool of something. Used for database connection pools.
  * Descendant class must provide a method for creating a new object to place in the pool.
  */
template <typename TObject,
          typename TLocker = std::mutex,
          typename TWaiter = std::condition_variable>
class PoolBase : private boost::noncopyable
{
public:
    using Object = TObject;
    using ObjectPtr = std::shared_ptr<Object>;
    using Ptr = std::shared_ptr<PoolBase<TObject, TLocker, TWaiter>>;

    using Entry = PoolEntry<TObject>;

private:

    /** The object with the flag, whether it is currently used. */
    struct PooledObject
    {
        explicit PooledObject(ObjectPtr object_) : object(object_) {}

        ObjectPtr object;
        bool in_use = false;
        std::atomic<bool> is_expired = false;
    };

    using Objects = std::vector<std::shared_ptr<PooledObject>>;

public:
    virtual ~PoolBase() = default;

    /** Allocates the object. Wait for free object in pool for 'timeout'. With 'timeout' < 0, the timeout is infinite. */
    Entry get(Poco::Timespan::TimeDiff timeout)
    {
        std::unique_lock lock(mutex);

        while (true)
        {
            for (auto & item : items)
            {
                if (!item->in_use)
                {
                    item->in_use = true;

                    if (likely(!item->is_expired))
                    {
                        return makeEntry(*item);
                    }

                    expireObject(item->object);
                    item->object = allocObject();
                    item->is_expired = false;
                    return makeEntry(*item);
                }
            }
            if (items.size() < max_items)
            {
                ObjectPtr object = allocObject();
                auto & item = items.emplace_back(std::make_shared<PooledObject>(object));
                item->in_use = true;
                return makeEntry(*item);
            }

            Stopwatch blocked;
            if (timeout < 0)
            {
                LOG_INFO(log, "No free connections in pool. Waiting indefinitely.");
                available.wait(lock);
            }
            else
            {
                auto timeout_ms = std::chrono::milliseconds(timeout);
                LOG_INFO(log, "No free connections in pool. Waiting {} ms.", timeout_ms.count());
                if constexpr (requires { available.wait_for(lock, timeout_ms); })
                    available.wait_for(lock, timeout_ms);
                else
                    static_cast<void>(available.wait_for(lock, static_cast<uint64_t>(timeout_ms.count()) * 1'000'000ULL));
            }
            ProfileEvents::increment(ProfileEvents::ConnectionPoolIsFullMicroseconds, blocked.elapsedMicroseconds());
        }
    }

    void reserve(size_t count)
    {
        std::lock_guard lock(mutex);

        while (items.size() < count)
            items.emplace_back(std::make_shared<PooledObject>(allocObject()));
    }

    size_t size()
    {
        std::lock_guard lock(mutex);
        return items.size();
    }

private:
    Entry makeEntry(PooledObject & item)
    {
        bool * in_use = &item.in_use;
        return Entry(std::make_shared<typename Entry::PoolEntryHelper>(
            item.object.get(),
            &item.is_expired,
            [this, in_use]
            {
                std::lock_guard lock(mutex);
                *in_use = false;
                available.notify_one();
            }));
    }

    /** The maximum size of the pool. */
    unsigned max_items;

    /** Pool. */
    Objects items;

    /** Lock to access the pool. */
    TLocker mutex;
    TWaiter available;

protected:
    LoggerPtr log;

    PoolBase(unsigned max_items_, LoggerPtr log_)
       : max_items(max_items_), log(log_)
    {
        items.reserve(max_items);
    }

    /** Creates a new object to put into the pool. */
    virtual ObjectPtr allocObject() = 0;
    virtual void expireObject(ObjectPtr) {}
};
