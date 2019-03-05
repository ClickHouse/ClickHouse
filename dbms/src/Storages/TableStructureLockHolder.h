#pragma once

#include <Common/RWLock.h>

namespace DB
{

struct TableStructureLockHolder
{
    /// Order is important.
    RWLockImpl::LockHolder alter_intention_lock;
    RWLockImpl::LockHolder new_data_structure_lock;
    RWLockImpl::LockHolder structure_lock;

    void release()
    {
        structure_lock.reset();
        new_data_structure_lock.reset();
        alter_intention_lock.reset();
    }
};

}
