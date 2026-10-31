#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <Poco/Timespan.h>
#include <boost/noncopyable.hpp>

#include <Common/logger_useful.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/saturatedDuration.h>
#include <Common/waitCancellable.h>

namespace ProfileEvents
{
    extern const Event ConnectionPoolIsFullMicroseconds;
}

namespace DB
{
    namespace ErrorCodes
    {
        extern const int NO_FREE_CONNECTION;
    }
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
    virtual ~PoolBase()
    {
        auto blocker = blockQueryTrackingIfPoolOutlivesQuery();
        items.clear();
    }

    /** Allocates the object. Wait for free object in pool for 'timeout'. With 'timeout' < 0, the timeout is infinite. */
    Entry get(Poco::Timespan::TimeDiff timeout)
    {
        std::unique_lock lock(mutex);

        /// One absolute deadline for the whole call, so a caller that goes round the loop again
        /// cannot restart its timeout. Clamped because the sum would otherwise wrap into the past.
        const bool has_deadline = timeout >= 0;
        const auto max_wait = DB::saturatedMilliseconds(timeout);
        const auto deadline = std::chrono::steady_clock::now() + max_wait;

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

                    {
                        auto blocker = blockQueryTrackingIfPoolOutlivesQuery();
                        expireObject(item->object);
                        item->object = allocObject();
                    }
                    item->is_expired = false;
                    return makeEntry(*item);
                }
            }
            if (items.size() < max_items)
            {
                PooledObject * item = nullptr;
                {
                    auto blocker = blockQueryTrackingIfPoolOutlivesQuery();
                    item = items.emplace_back(std::make_shared<PooledObject>(allocObject())).get();
                }
                item->in_use = true;
                return makeEntry(*item);
            }

            /// Accounted by an RAII guard, so the time spent blocked is still reported when this
            /// scope is left by the deadline throw or by a cancellation.
            DB::ProfileEventTimeIncrement<DB::Time::Microseconds> blocked(ProfileEvents::ConnectionPoolIsFullMicroseconds);

            if (has_deadline)
                /// A finite wait wakes once per slice, so an unlimited log here floods.
                LOG_INFO(LogFrequencyLimiter(log, 10), "No free connections in pool. Waiting {} ms.", max_wait.count());
            else
                LOG_INFO(log, "No free connections in pool. Waiting indefinitely.");

            /// Waiting in slices is what makes cancellation observable: `available` is only notified
            /// when an object is returned, so a caller holding one would otherwise pin this thread
            /// for the whole wait.
            /// TWaiter is only required to offer the two-argument wait_for(), so the predicate
            /// is tested here rather than passed in.
            auto wait_available = [&](auto slice)
            {
                available.wait_for(lock, slice);
                return hasAvailableObjectUnlocked();
            };
            if (!DB::waitCancellable(wait_available, has_deadline ? std::optional(deadline) : std::nullopt, wait_slice))
                throw DB::Exception(
                    DB::ErrorCodes::NO_FREE_CONNECTION,
                    "No free connection in pool of size {} after waiting {} ms",
                    max_items,
                    max_wait.count());
        }
    }

    void reserve(size_t count)
    {
        std::lock_guard lock(mutex);

        auto blocker = blockQueryTrackingIfPoolOutlivesQuery();
        while (items.size() < count)
            items.emplace_back(std::make_shared<PooledObject>(allocObject()));
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

    /** Whether get() could return without waiting. Called under `mutex`. */
    bool hasAvailableObjectUnlocked() const
    {
        if (items.size() < max_items)
            return true;
        return std::any_of(items.begin(), items.end(), [](const auto & item) { return !item->in_use; });
    }

    /// A pooled object lives as long as the pool, so a long-lived pool does not charge it to the query that happens to create it.
    std::optional<MemoryTrackerBlockerInThread> blockQueryTrackingIfPoolOutlivesQuery() const
    {
        if (!pool_outlives_query)
            return {};
        return std::optional<MemoryTrackerBlockerInThread>(std::in_place);
    }

    Entry makeEntry(PooledObject & item)
    {
        bool * in_use = &item.in_use;
        return Entry(std::make_shared<typename Entry::PoolEntryHelper>(
            item.object.get(),
            &item.is_expired,
            [this, in_use, object = item.object.get()]
            {
                releaseObject(*object);
                std::lock_guard lock(mutex);
                *in_use = false;
                available.notify_one();
            }));
    }

    /** The maximum size of the pool. */
    unsigned max_items;

    const bool pool_outlives_query;

    /** Pool. */
    Objects items;

    /** Lock to access the pool. */
    TLocker mutex;
    TWaiter available;

protected:
    LoggerPtr log;

    PoolBase(unsigned max_items_, LoggerPtr log_, bool pool_outlives_query_)
       : max_items(max_items_), pool_outlives_query(pool_outlives_query_), log(log_)
    {
        items.reserve(max_items);
    }

    /** Creates a new object to put into the pool. */
    virtual ObjectPtr allocObject() = 0;
    virtual void expireObject(ObjectPtr) {}
    /// Called by the thread that used the object, when it gives it back.
    virtual void releaseObject(Object &) noexcept {}
};
