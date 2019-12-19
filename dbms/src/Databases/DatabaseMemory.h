#pragma once

#include <Databases/DatabasesCommon.h>


namespace Poco { class Logger; }


namespace DB
{

/** A non-persistent database to store temporary data.
  * It doesn't make any manipulations with filesystem.
  * All tables are created by calling code.
  * TODO: Maybe DatabaseRuntime is more suitable class name.
  */
class DatabaseMemory : public DatabaseWithOwnTablesBase
{
public:
    DatabaseMemory(String name_);

    String getDatabaseName() const override;

    String getEngineName() const override { return "Memory"; }

    void loadStoredObjects(
        Context & context,
        bool has_force_restore_data_flag) override;

    void createTable(
        const Context & context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void createDictionary(
        const Context & context,
        const String & dictionary_name,
        const ASTPtr & query) override;

    void attachDictionary(
        const String & name,
        const Context & context) override;

    void removeTable(
        const Context & context,
        const String & table_name) override;

    void removeDictionary(
        const Context & context,
        const String & dictionary_name) override;

    void detachDictionary(
        const String & name,
        const Context & context) override;

    time_t getObjectMetadataModificationTime(const Context & context, const String & table_name) override;

    ASTPtr getCreateTableQuery(const Context & context, const String & table_name) const override;
    ASTPtr getCreateDictionaryQuery(const Context & context, const String & table_name) const override;
    ASTPtr tryGetCreateTableQuery(const Context &, const String &) const override { return nullptr; }
    ASTPtr tryGetCreateDictionaryQuery(const Context &, const String &) const override { return nullptr; }

    ASTPtr getCreateDatabaseQuery(const Context & context) const override;

private:
    Poco::Logger * log;
};

}
