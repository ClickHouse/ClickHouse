#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <ext/shared_ptr_helper.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Databases/MySQL/MaterializeMySQLSettings.h>
#include <Databases/MySQL/MaterializeMySQLSyncThread.h>
#include <Disks/IDisk.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMemory.h>

#include <mutex>
#include <string>
#include <vector>

namespace DB
{

class StorageMySQLReplica final
    : public  ext::shared_ptr_helper<StorageMySQLReplica>
    , public IStorage
{
    friend struct ext::shared_ptr_helper<StorageMySQLReplica>;

public:
    std::string getName() const override {
        return "MySQLReplica";
    }

    bool supportsSettings() const override { return true; }

    void startup() override;
    void shutdown() override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    Strings getDataPaths() const override { return {fullPath(disk, table_path)}; }

protected:
    StorageMySQLReplica(
        const StorageID & table_id_,
        Context & context_,
        ColumnsDescription columns_description_,
        ConstraintsDescription constraints_,
        /* slave data */
        const String & mysql_hostname_and_port,
        const String & mysql_database_name_,
        const String & mysql_table_name_,
        const String & mysql_user_name,
        const String & mysql_user_password,
        MaterializeMySQLSettingsPtr settings_,
        DiskPtr disk_,
        const String & relative_path_);

public:
    // TODO: delete this
    /// The data itself. `list` - so that when inserted to the end, the existing iterators are not invalidated.
    BlocksList data;
    mutable std::mutex mutex;


private:
    Context global_context;

    std::string mysql_table_name;
    MaterializeMySQLSettingsPtr settings;

    DiskPtr disk;
    String table_path;

    Poco::Logger * log;

    MaterializeMySQLSyncThreadPtr materialize_thread;
};

}

#endif
