#pragma once
#include "Disks/IDisk.h"
#include "VFSLogItem.h"

namespace DB
{
class DiskObjectStorageVFS;
struct VFSTransactionGroup : VFSLogItem
{
    explicit VFSTransactionGroup(DiskPtr disk_);
    ~VFSTransactionGroup();
    VFSTransactionGroup(const VFSTransactionGroup &) = delete;
    VFSTransactionGroup(VFSTransactionGroup &&) = delete;

private:
    DiskObjectStorageVFS * disk;
};
}
