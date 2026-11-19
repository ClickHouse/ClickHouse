#pragma once

#include <Common/SharedMutex.h>
#include <chrono>
#include <mutex>
#include <optional>


namespace DB
{

/// Allows executing DDL query only in one thread.
/// Puts an element into the map, locks tables's mutex, counts how much threads run parallel query on the table,
/// when counter is 0 erases element in the destructor.
/// If the element already exists in the map, waits when ddl query will be finished in other thread.
class DDLGuard
{
public:
    struct Entry
    {
      std::unique_ptr<std::timed_mutex> mutex;
      UInt32 counter;
    };

    /// Element name -> (mutex, counter).
    /// NOTE: using std::map here (and not std::unordered_map) to avoid iterator invalidation on insertion.
    using Map = std::map<String, Entry>;

    /// With no try_timeout blocks until the guard is acquired. With try_timeout waits on the table
    /// mutex at most that long, never sleeps and never throws on contention.
    DDLGuard(
        Map & map_,
        SharedMutex & db_mutex_,
        std::unique_lock<std::mutex> guards_lock_,
        const String & elem,
        const String & database_name,
        std::optional<std::chrono::milliseconds> try_timeout = {});
    ~DDLGuard();

    /// True when the guard was fully acquired. Only ever false when constructed with try_timeout.
    bool ownsTableLock() const { return table_lock.owns_lock(); }

    /// True when acquisition failed on the database-level lock (an exclusive database DDL is running).
    bool databaseLockBusy() const { return database_lock_busy; }

    /// Unlocks table name, keeps holding read lock for database name
    void releaseTableLock() noexcept;

private:
    Map & map;
    SharedMutex & db_mutex;
    Map::iterator it;
    std::unique_lock<std::mutex> guards_lock;
    std::unique_lock<std::timed_mutex> table_lock;
    bool table_lock_removed = false;
    bool is_database_guard = false;
    bool db_mutex_held = false;
    bool database_lock_busy = false;
};

using DDLGuardPtr = std::unique_ptr<DDLGuard>;

}
