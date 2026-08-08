#pragma once

#include <algorithm>
#include <mutex>
#include <condition_variable>
#include <Poco/Timespan.h>
#include <boost/noncopyable.hpp>

#include <Common/logger_useful.h>
#include <Common/CurrentThread.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents
{
    extern const Event ConnectionPoolIsFullMicroseconds;
}

namespace DB
{
    namespace ErrorCodes
    {
        extern const int LOGICAL_ERROR;
        extern const int NO_FREE_CONNECTION;
    }
}

/** A class from which you can inherit and get a pool of something. Used for database connection pools.
  * Descendant class must provide a method for creating a new object to place in the pool.
  */

template <typename TObject>
class PoolBase : private boost::noncopyable
{
public:
    using Object = TObject;
    using ObjectPtr = std::shared_ptr<Object>;
    using Ptr = std::shared_ptr<PoolBase<TObject>>;

private:

    /** The object with the flag, whether it is currently used. */
    struct PooledObject
    {
        PooledObject(ObjectPtr object_, PoolBase & pool_)
            : object(object_), pool(pool_)
        {
        }

        ObjectPtr object;
        bool in_use = false;
        std::atomic<bool> is_expired = false;
        PoolBase & pool;
    };

    using Objects = std::vector<std::shared_ptr<PooledObject>>;

    /** The helper, which sets the flag for using the object, and in the destructor - removes,
      *  and also notifies the event using condvar.
      */
    struct PoolEntryHelper
    {
        explicit PoolEntryHelper(PooledObject & data_) : data(data_) { data.in_use = true; }
        ~PoolEntryHelper()
        {
            std::lock_guard lock(data.pool.mutex);
            data.in_use = false;
            data.pool.available.notify_one();
        }

        PooledObject & data;
    };

public:
    /** What is given to the user. */
    class Entry
    {
    public:
        friend class PoolBase<Object>;

        Entry() = default;    /// For deferred initialization.

        /** The `Entry` object protects the resource from being used by another thread.
          * The following methods are forbidden for `rvalue`, so you can not write a similar to
          *
          * auto q = pool.get()->query("SELECT .."); // Oops, after this line Entry was destroyed
          * q.execute (); // Someone else can use this Connection
          */
        Object * operator->() && = delete;
        const Object * operator->() const && = delete;
        Object & operator*() && = delete;
        const Object & operator*() const && = delete;

        Object * operator->() &             { return &*data->data.object; }
        const Object * operator->() const & { return &*data->data.object; }
        Object & operator*() &              { return *data->data.object; }
        const Object & operator*() const &  { return *data->data.object; }

        /**
         * Expire an object to make it reallocated later.
         */
        void expire()
        {
            data->data.is_expired = true;
        }

        bool isNull() const { return data == nullptr; }

        PoolBase * getPool() const
        {
            if (!data)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Attempt to get pool from uninitialized entry");
            return &data->data.pool;
        }

    private:
        std::shared_ptr<PoolEntryHelper> data;

        explicit Entry(PooledObject & object) : data(std::make_shared<PoolEntryHelper>(object)) {}
    };

    virtual ~PoolBase() = default;

    /** Allocates the object. Wait for free object in pool for 'timeout'. With 'timeout' < 0, the timeout is infinite. */
    Entry get(Poco::Timespan::TimeDiff timeout)
    {
        std::unique_lock lock(mutex);

        /// One absolute deadline for the whole call, so a caller that goes round the loop again
        /// cannot restart its timeout. Clamped because the sum would otherwise wrap into the past.
        const bool has_deadline = timeout >= 0;
        const auto deadline = std::chrono::steady_clock::now()
            + std::chrono::milliseconds(std::clamp<Poco::Timespan::TimeDiff>(timeout, 0, max_wait_ms));

        while (true)
        {
            for (auto & item : items)
            {
                if (!item->in_use)
                {
                    if (likely(!item->is_expired))
                    {
                        return Entry(*item);
                    }

                    expireObject(item->object);
                    item->object = allocObject();
                    item->is_expired = false;
                    return Entry(*item);
                }
            }
            if (items.size() < max_items)
            {
                ObjectPtr object = allocObject();
                items.emplace_back(std::make_shared<PooledObject>(object, *this));
                return Entry(*items.back());
            }

            /// Accounted by an RAII guard, so the time spent blocked is still reported when this
            /// scope is left by the deadline throw or by a cancellation.
            DB::ProfileEventTimeIncrement<DB::Time::Microseconds> blocked(ProfileEvents::ConnectionPoolIsFullMicroseconds);

            /// Waiting in slices is what makes cancellation observable: `available` is only notified
            /// when an object is returned, so a caller holding one would otherwise pin this thread
            /// for the whole wait. checkIfNotCancelled() is a no-op off-query.
            auto object_available = [this] { return hasAvailableObjectUnlocked(); };

            if (has_deadline)
                /// A finite wait wakes once per slice, so an unlimited log here floods.
                LOG_INFO(LogFrequencyLimiter(log, 10), "No free connections in pool. Waiting {} ms.", timeout);
            else
                LOG_INFO(log, "No free connections in pool. Waiting indefinitely.");

            while (true)
            {
                DB::CurrentThread::checkIfNotCancelled();

                const auto now = std::chrono::steady_clock::now();
                auto slice = std::chrono::duration_cast<std::chrono::steady_clock::duration>(wait_slice);
                if (has_deadline)
                {
                    if (now >= deadline)
                        throw DB::Exception(
                            DB::ErrorCodes::NO_FREE_CONNECTION,
                            "No free connection in pool of size {} after waiting {} ms",
                            max_items,
                            timeout);
                    /// Clamped, so the total wait cannot overrun the deadline by up to a slice.
                    slice = std::min(slice, deadline - now);
                }

                if (available.wait_for(lock, slice, object_available))
                    break;
            }
        }
    }

    void reserve(size_t count)
    {
        std::lock_guard lock(mutex);

        while (items.size() < count)
            items.emplace_back(std::make_shared<PooledObject>(allocObject(), *this));
    }

    size_t size()
    {
        std::lock_guard lock(mutex);
        return items.size();
    }

private:
    /** Length of one wait slice. Bounds how long a queued caller can stay unaware of its own
      * cancellation; short enough to be prompt, long enough that an idle pool barely wakes.
      */
    static constexpr auto wait_slice = std::chrono::seconds(1);

    /** Largest wait a steady_clock::time_point can represent, so `now() + timeout` cannot wrap. */
    static constexpr Poco::Timespan::TimeDiff max_wait_ms
        = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::duration::max()).count() / 2;

    /** Whether get() could return without waiting. Called under `mutex`. */
    bool hasAvailableObjectUnlocked() const
    {
        if (items.size() < max_items)
            return true;
        return std::any_of(items.begin(), items.end(), [](const auto & item) { return !item->in_use; });
    }

    /** The maximum size of the pool. */
    unsigned max_items;

    /** Pool. */
    Objects items;

    /** Lock to access the pool. */
    std::mutex mutex;
    std::condition_variable available;

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
