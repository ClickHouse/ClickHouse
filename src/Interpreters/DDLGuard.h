#pragma once

#include <Common/SharedMutex.h>
#include <mutex>


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
      std::unique_ptr<std::mutex> mutex;
      UInt32 counter;
    };

    /// Element name -> (mutex, counter).
    /// NOTE: using std::map here (and not std::unordered_map) to avoid iterator invalidation on insertion.
    using Map = std::map<String, Entry>;

    DDLGuard(
        Map & map_,
        SharedMutex & db_mutex_,
        std::unique_lock<std::mutex> guards_lock_,
        const String & elem,
        const String & database_name,
        bool try_lock = false);
    ~DDLGuard();

    /// True when the per-table mutex was successfully acquired.
    /// Only ever false when the guard was constructed with try_lock = true and the mutex was busy.
    bool ownsTableLock() const { return table_lock.owns_lock(); }

    /// Unlocks table name, keeps holding read lock for database name
    void releaseTableLock() noexcept;

private:
    Map & map;
    SharedMutex & db_mutex;
    Map::iterator it;
    std::unique_lock<std::mutex> guards_lock;
    std::unique_lock<std::mutex> table_lock;
    bool table_lock_removed = false;
    bool is_database_guard = false;
    bool db_mutex_held = false;
};

using DDLGuardPtr = std::unique_ptr<DDLGuard>;

}
