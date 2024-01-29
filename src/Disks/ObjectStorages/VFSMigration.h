#pragma once
#include "Common/Logger.h"
#include "Interpreters/Context_fwd.h"
#include "Storages/IStorage_fwd.h"

namespace zkutil
{
class ZooKeeper;
}
namespace DB
{
class DiskObjectStorageVFS;

struct VFSMigration : WithContext
{
    explicit VFSMigration(DiskObjectStorageVFS & disk_, ContextWeakPtr ctx);
    DiskObjectStorageVFS & disk;
    LoggerPtr log;
    void migrate() const;
    void migrateTable(StoragePtr table_ptr, const Context & ctx) const;
    bool fromZeroCopy(IStorage & table, zkutil::ZooKeeper & zookeeper) const;
};
}
