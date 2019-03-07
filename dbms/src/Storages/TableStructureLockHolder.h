#pragma once

#include <Common/RWLock.h>

namespace DB
{

struct TableStructureWriteLockHolder
{
    void release()
    {
        *this = TableStructureWriteLockHolder();
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
private:
    friend class IStorage;

    /// Order is important.
    RWLockImpl::LockHolder new_data_structure_lock;
    RWLockImpl::LockHolder structure_lock;
};

}
