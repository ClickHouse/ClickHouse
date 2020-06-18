#pragma once

#include <Common/RWLock.h>

namespace DB
{

struct TableExclusiveLockHolder
{
    void release() { *this = TableExclusiveLockHolder(); }

private:
    friend class IStorage;

    /// Order is important.
    RWLockImpl::LockHolder alter_lock;
    RWLockImpl::LockHolder drop_lock;
};

using TableLockHolder = RWLockImpl::LockHolder;
}
