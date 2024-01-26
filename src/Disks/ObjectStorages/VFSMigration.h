#pragma once
#include "DiskObjectStorageVFS.h"

namespace DB
{
struct VFSMigration
{
    DiskObjectStorageVFS & disk;
    ObjectStorageVFSGCThread & gc;
    ContextMutablePtr ctx;
    LoggerPtr log = getLogger("VFSMigration");
    void migrate() const;
};
}
