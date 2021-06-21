#pragma once

#include <Databases/DatabaseWithDictionaries.h>
#include <Common/ThreadPool.h>


namespace DB
{

/** Default engine of databases.
  * It stores tables list in filesystem using list of .sql files,
  *  that contain declaration of table represented by SQL ATTACH TABLE query.
  */
class DatabaseOrdinary : public DatabaseWithDictionaries
{
public:
    DatabaseOrdinary(const String & name_, const String & metadata_path_, const Context & context);
    DatabaseOrdinary(const String & name_, const String & metadata_path_, const String & data_path_, const String & logger, const Context & context_);

    String getEngineName() const override { return "Ordinary"; }

    void loadStoredObjects(
        Context & context,
        bool has_force_restore_data_flag,
        bool force_attach) override;

    void alterTable(
        const Context & context,
        const StorageID & table_id,
        const StorageInMemoryMetadata & metadata) override;

protected:
    virtual void commitAlterTable(const StorageID & table_id, const String & table_metadata_tmp_path, const String & table_metadata_path);

    void startupTables(ThreadPool & thread_pool);
};

}
