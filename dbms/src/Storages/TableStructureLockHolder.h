#pragma once

#include <mutex>
#include <shared_mutex>


namespace DB
{

/// Structs that hold table structure (columns, their types, default values etc.) locks when executing queries.
/// See IStorage::lock* methods for comments.

struct TableStructureWriteLockHolder
{
    void release()
    {
        *this = {};
    }

    void releaseAllExceptAlterIntention()
    {
        if (structure_unique_lock)
            structure_unique_lock.unlock();
        else if (structure_shared_lock)
            structure_shared_lock.unlock();
    }

private:
    friend class IStorage;

    /// Order is important.
    std::unique_lock<std::mutex> alter_lock;

    std::shared_lock<std::shared_mutex> structure_shared_lock;
    std::unique_lock<std::shared_mutex> structure_unique_lock;
};

struct TableStructureReadLockHolder
{
    void release()
    {
        *this = {};
    }

private:
    friend class IStorage;

    std::shared_ptr<std::shared_lock<std::shared_mutex>> structure_shared_lock;
    std::shared_ptr<std::unique_lock<std::shared_mutex>> structure_unique_lock;
};

}
