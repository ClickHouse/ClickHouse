#pragma once
#include "Disks/IDisk.h"
#include "VFSLogItem.h"

namespace DB
{
class DiskObjectStorageVFS;

// Group Zookeeper operations on VFS disk happening in the thread this object was instantiated in.
// Operations are written in object destructor.
// Caveat: you can't use this object if a thread switch (via coroutine/thread pool) can happen
// throughout its lifetime.
struct VFSTransactionGroup : VFSLogItem
{
    explicit VFSTransactionGroup(DiskPtr disk_);
    ~VFSTransactionGroup();
    VFSTransactionGroup(const VFSTransactionGroup &) = delete;
    VFSTransactionGroup(VFSTransactionGroup &&) = default;

private:
    DiskObjectStorageVFS * disk;
};
}
