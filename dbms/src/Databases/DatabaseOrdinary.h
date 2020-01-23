#pragma once

#include <Databases/DatabaseWithDictionaries.h>
#include <Common/ThreadPool.h>


namespace DB
{

/** Default engine of databases.
  * It stores tables list in filesystem using list of .sql files,
  *  that contain declaration of table represented by SQL ATTACH TABLE query.
  */
class DatabaseOrdinary : public DatabaseWithDictionaries //DatabaseWithOwnTablesBase
{
public:
    DatabaseOrdinary(const String & name_, const String & metadata_path_, const Context & context);

    String getEngineName() const override { return "Ordinary"; }

    void loadStoredObjects(
        Context & context,
        bool has_force_restore_data_flag) override;

    void alterTable(
        const Context & context,
        const String & name,
        const StorageInMemoryMetadata & metadata) override;

protected:

    void startupTables(ThreadPool & thread_pool);
};

}
