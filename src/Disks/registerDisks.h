#pragma once
#include <Core/Types.h>

namespace DB
{
enum class DiskStartupFlags
{
    /// skip access check regardless .skip_access_check config directive (used for clickhouse-disks)
    GLOBAL_SKIP_ACCESS_CHECK = 1,
    /// ObjectStorageVFS is allowed (used e.g. for Keeper)
    ALLOW_VFS = 1 << 1,
    /// ObjectStorageVFS Garbage Collector is allowed (used for clickhouse-disks)
    ALLOW_VFS_GC = 1 << 2,
};

bool is_set(DiskStartupFlags flag1, DiskStartupFlags flag2);

void registerDisks(DiskStartupFlags disk_flags);
}
