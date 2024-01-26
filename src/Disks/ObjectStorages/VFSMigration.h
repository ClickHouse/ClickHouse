#pragma once
#include "Common/Logger.h"
#include "Interpreters/Context_fwd.h"
#include "Storages/IStorage_fwd.h"

namespace DB
{
class InterpreterSystemQuery;
class DiskObjectStorageVFS;

struct VFSMigration
{
    DiskObjectStorageVFS & disk;
    ContextMutablePtr ctx;
    LoggerPtr log = getLogger("VFSMigration");
    void migrate() const;
    void migrateTable(StoragePtr table) const;
};
}
