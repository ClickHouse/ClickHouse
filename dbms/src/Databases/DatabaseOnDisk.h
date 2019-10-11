#pragma once

#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Databases/DatabasesCommon.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>


namespace DB
{

namespace detail
{
    String getTableMetadataPath(const String & base_path, const String & table_name);
    String getDatabaseMetadataPath(const String & base_path);
    ASTPtr getQueryFromMetadata(const String & metadata_path, bool throw_on_error = true);
    ASTPtr getCreateQueryFromMetadata(const String & metadata_path, const String & database, bool throw_on_error);
}

/* Class to provide basic operations with tables when metadata is stored on disk in .sql files.
 */
class DatabaseOnDisk
{
public:
    static void createTable(
        IDatabase & database,
        const Context & context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query);

    static void removeTable(
        IDatabase & database,
        const Context & context,
        const String & table_name,
        Poco::Logger * log);

    template <typename Database>
    static void renameTable(
        IDatabase & database,
        const Context & context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name,
        TableStructureWriteLockHolder & lock);

    static ASTPtr getCreateTableQuery(
        const IDatabase & database,
        const Context & context,
        const String & table_name);

    static ASTPtr tryGetCreateTableQuery(
        const IDatabase & database,
        const Context & context,
        const String & table_name);

    static ASTPtr getCreateDatabaseQuery(
        const IDatabase & database,
        const Context & context);

    static void drop(const IDatabase & database);

    static String getTableMetadataPath(
        const IDatabase & database,
        const String & table_name);

    static time_t getTableMetadataModificationTime(
        const IDatabase & database,
        const String & table_name);


    using IteratingFunction = std::function<void(const String &)>;
    static void iterateTableFiles(const IDatabase & database, Poco::Logger * log, const IteratingFunction & iterating_function);

private:
    static ASTPtr getCreateTableQueryImpl(
        const IDatabase & database,
        const Context & context,
        const String & table_name,
        bool throw_on_error);
};


namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TABLE;
    extern const int FILE_DOESNT_EXIST;
}

template <typename Database>
void DatabaseOnDisk::renameTable(
    IDatabase & database,
    const Context & context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name,
    TableStructureWriteLockHolder & lock)
{
    Database * to_database_concrete = typeid_cast<Database *>(&to_database);

    if (!to_database_concrete)
        throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);

    StoragePtr table = database.tryGetTable(context, table_name);

    if (!table)
        throw Exception("Table " + backQuote(database.getDatabaseName()) + "." + backQuote(table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    /// Notify the table that it is renamed. If the table does not support renaming, exception is thrown.
    try
    {
        table->rename(context.getPath() + "/data/" + escapeForFileName(to_database_concrete->getDatabaseName()) + "/",
            to_database_concrete->getDatabaseName(),
            to_table_name, lock);
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (const Poco::Exception & e)
    {
        /// Better diagnostics.
        throw Exception{Exception::CreateFromPoco, e};
    }

    ASTPtr ast = detail::getQueryFromMetadata(detail::getTableMetadataPath(database.getMetadataPath(), table_name));
    if (!ast)
        throw Exception("There is no metadata file for table " + backQuote(table_name) + ".", ErrorCodes::FILE_DOESNT_EXIST);
    ast->as<ASTCreateQuery &>().table = to_table_name;

    /// NOTE Non-atomic.
    to_database_concrete->createTable(context, to_table_name, table, ast);
    database.removeTable(context, table_name);
}

}
