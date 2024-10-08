#pragma once

#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Databases/DatabasesCommon.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;
std::pair<String, StoragePtr> createTableFromAST(
    ASTCreateQuery ast_create_query,
    const String & database_name,
    const String & table_data_path_relative,
    ContextMutablePtr context,
    LoadingStrictnessLevel mode);

/** Get the string with the table definition based on the CREATE query.
  * It is an ATTACH query that you can execute to create a table from the correspondent database.
  * See the implementation.
  */
String getObjectDefinitionFromCreateQuery(const ASTPtr & query);


/* Class to provide basic operations with tables when metadata is stored on disk in .sql files.
 */
class DatabaseOnDisk : public DatabaseWithOwnTablesBase
{
public:
    DatabaseOnDisk(const String & name, const String & metadata_path_, const String & data_path_, const String & logger, ContextPtr context);

    void shutdown() override;

    void createTable(
        ContextPtr context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void detachTablePermanently(ContextPtr context, const String & table_name) override;

    void dropTable(
        ContextPtr context,
        const String & table_name,
        bool sync) override;

    void renameTable(
        ContextPtr context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name,
        bool exchange,
        bool dictionary) override;

    ASTPtr getCreateDatabaseQuery() const override;

    void drop(ContextPtr context) override;

    String getObjectMetadataPath(const String & object_name) const override;

    time_t getObjectMetadataModificationTime(const String & object_name) const override;

    String getDataPath() const override { return data_path; }
    String getTableDataPath(const String & table_name) const override { return std::filesystem::path(data_path) / escapeForFileName(table_name) / ""; }
    String getTableDataPath(const ASTCreateQuery & query) const override { return getTableDataPath(query.getTable()); }
    String getMetadataPath() const override { return metadata_path; }

    static ASTPtr parseQueryFromMetadata(LoggerPtr log, ContextPtr context, const String & metadata_file_path, bool throw_on_error = true, bool remove_empty = false);

    /// will throw when the table we want to attach already exists (in active / detached / detached permanently form)
    void checkMetadataFilenameAvailability(const String & to_table_name) const override;
    void checkMetadataFilenameAvailabilityUnlocked(const String & to_table_name) const TSA_REQUIRES(mutex);

    void modifySettingsMetadata(const SettingsChanges & settings_changes, ContextPtr query_context);

protected:
    static constexpr const char * create_suffix = ".tmp";
    static constexpr const char * drop_suffix = ".tmp_drop";
    static constexpr const char * detached_suffix = ".detached";

    using IteratingFunction = std::function<void(const String &)>;

    void iterateMetadataFiles(const IteratingFunction & process_metadata_file) const;

    ASTPtr getCreateTableQueryImpl(
        const String & table_name,
        ContextPtr context,
        bool throw_on_error) const override;

    ASTPtr getCreateQueryFromMetadata(const String & metadata_path, bool throw_on_error) const;
    ASTPtr getCreateQueryFromStorage(const String & table_name, const StoragePtr & storage, bool throw_on_error) const;

    virtual void commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                                   const String & table_metadata_tmp_path, const String & table_metadata_path, ContextPtr query_context);

    virtual void removeDetachedPermanentlyFlag(ContextPtr context, const String & table_name, const String & table_metadata_path, bool attach);
    virtual void setDetachedTableNotInUseForce(const UUID & /*uuid*/) {}

    const String metadata_path;
    const String data_path;
};

}
