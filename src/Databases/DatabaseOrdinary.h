#pragma once

#include <Databases/DatabaseOnDisk.h>
#include <Common/ThreadPool.h>


namespace DB
{

/** Default engine of databases.
  * It stores tables list in filesystem using list of .sql files,
  *  that contain declaration of table represented by SQL ATTACH TABLE query.
  */
class DatabaseOrdinary : public DatabaseOnDisk
{
public:
    DatabaseOrdinary(const String & name_, const String & metadata_path_, ContextPtr context);
    DatabaseOrdinary(
        const String & name_, const String & metadata_path_, const String & data_path_, const String & logger, ContextPtr context_);

    String getEngineName() const override { return "Ordinary"; }

    void loadStoredObjects(ContextPtr context, bool has_force_restore_data_flag, bool force_attach) override;

    void alterTable(
        ContextPtr context,
        const StorageID & table_id,
        const StorageInMemoryMetadata & metadata) override;

protected:
    virtual void commitAlterTable(
        const StorageID & table_id,
        const String & table_metadata_tmp_path,
        const String & table_metadata_path,
        const String & statement,
        ContextPtr query_context);

    void startupTables(ThreadPool & thread_pool);
};

}
