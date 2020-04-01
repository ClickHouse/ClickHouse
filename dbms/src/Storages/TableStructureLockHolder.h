#pragma once

#include <Common/RWLock.h>

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
        structure_lock.reset();
    }

private:
    friend class IStorage;

    /// Order is important.
    std::unique_lock<std::mutex> alter_lock;
    RWLockImpl::LockHolder structure_lock;
};

struct TableStructureReadLockHolder
{
    void release()
    {
        *this = {};
    }

private:
    friend class IStorage;

    /// Order is important.
    RWLockImpl::LockHolder structure_lock;
};

}
