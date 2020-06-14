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
    Context & context,
    bool has_force_restore_data_flag);

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
    DatabaseOnDisk(const String & name, const String & metadata_path_, const String & data_path_, const String & logger, const Context & context);

    void createTable(
        const Context & context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void dropTable(
        const Context & context,
        const String & table_name,
        bool no_delay) override;

    void renameTable(
        const Context & context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name,
        bool exchange) override;

    ASTPtr getCreateDatabaseQuery() const override;

    void drop(const Context & context) override;

    String getObjectMetadataPath(const String & object_name) const override;

    time_t getObjectMetadataModificationTime(const String & object_name) const override;

    String getDataPath() const override { return data_path; }
    String getTableDataPath(const String & table_name) const override { return data_path + escapeForFileName(table_name) + "/"; }
    String getTableDataPath(const ASTCreateQuery & query) const override { return getTableDataPath(query.table); }
    String getMetadataPath() const override { return metadata_path; }

    static ASTPtr parseQueryFromMetadata(Poco::Logger * log, const Context & context, const String & metadata_file_path, bool throw_on_error = true, bool remove_empty = false);

protected:
    static constexpr const char * create_suffix = ".tmp";
    static constexpr const char * drop_suffix = ".tmp_drop";

    using IteratingFunction = std::function<void(const String &)>;

    void iterateMetadataFiles(const Context & context, const IteratingFunction & process_metadata_file) const;

    ASTPtr getCreateTableQueryImpl(
        const String & table_name,
        const Context & context,
        bool throw_on_error) const override;

    ASTPtr getCreateQueryFromMetadata(const String & metadata_path, bool throw_on_error) const;

    virtual void commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                                   const String & table_metadata_tmp_path, const String & table_metadata_path);

    const String metadata_path;
    const String data_path;
};

}
