#pragma once
#include "Disks/IDisk.h"
#include "VFSLogItem.h"

namespace DB
{
class DiskObjectStorageVFS;
struct VFSTransactionGroup : private VFSLogItem
{
    explicit VFSTransactionGroup(DiskPtr disk_);
    ~VFSTransactionGroup();
    VFSTransactionGroup(const VFSTransactionGroup &) = delete;
    VFSTransactionGroup(VFSTransactionGroup &&) = delete;

private:
    friend class DiskObjectStorageVFS; // We'd need public inheritance from VFSLogItem otherwise
    DiskObjectStorageVFS * disk;
};
}
