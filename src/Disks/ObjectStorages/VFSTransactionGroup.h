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

private:
    DiskObjectStorageVFS * const disk;
};
}
