#pragma once
#include "Common/Logger.h"
#include "Interpreters/Context_fwd.h"

namespace DB
{
class DiskObjectStorageVFS;

struct VFSMigration : WithContext
{
    explicit VFSMigration(DiskObjectStorageVFS & disk_, ContextWeakPtr ctx);
    DiskObjectStorageVFS & disk;
    LoggerPtr log;

    void migrate() const;
};
}
