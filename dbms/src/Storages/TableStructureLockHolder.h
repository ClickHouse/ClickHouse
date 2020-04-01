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
        *this = TableStructureWriteLockHolder();
    }

    void releaseAllExceptAlterIntention()
    {
        new_data_structure_lock.reset();
        structure_lock.reset();
    }

private:
    friend class IStorage;

    /// Order is important.
    RWLockImpl::LockHolder alter_intention_lock;
    RWLockImpl::LockHolder new_data_structure_lock;
    RWLockImpl::LockHolder structure_lock;
};

struct TableStructureReadLockHolder
{
    void release()
    {
        *this = TableStructureReadLockHolder();
    }

private:
    friend class IStorage;

    /// Order is important.
    RWLockImpl::LockHolder new_data_structure_lock;
    RWLockImpl::LockHolder structure_lock;
};

}
