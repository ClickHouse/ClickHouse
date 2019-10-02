#include <Common/escapeForFileName.h>
#include <Databases/DatabaseOnDisk.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Event.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int SYNTAX_ERROR;
}


namespace detail
{
    String getTableMetadataPath(const String & base_path, const String & table_name)
    {
        return base_path + (endsWith(base_path, "/") ? "" : "/") + escapeForFileName(table_name) + ".sql";
    }

    String getDatabaseMetadataPath(const String & base_path)
    {
        return (endsWith(base_path, "/") ? base_path.substr(0, base_path.size() - 1) : base_path) + ".sql";
    }
}

namespace
{
    ASTPtr getQueryFromMetadata(const String & metadata_path, bool throw_on_error = true)
    {
        String query;

        try
        {
            ReadBufferFromFile in(metadata_path, 4096);
            readStringUntilEOF(query, in);
        }
        catch (const Exception & e)
        {
            if (!throw_on_error && e.code() == ErrorCodes::FILE_DOESNT_EXIST)
                return nullptr;
            else
                throw;
        }

        ParserCreateQuery parser;
        const char * pos = query.data();
        std::string error_message;
        auto ast = tryParseQuery(parser, pos, pos + query.size(), error_message, /* hilite = */ false,
                                "in file " + metadata_path, /* allow_multi_statements = */ false, 0);

        if (!ast && throw_on_error)
            throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);

        return ast;
    }

    ASTPtr getCreateQueryFromMetadata(const String & metadata_path, const String & database, bool throw_on_error)
    {
        ASTPtr ast = getQueryFromMetadata(metadata_path, throw_on_error);

        if (ast)
        {
            auto & ast_create_query = ast->as<ASTCreateQuery &>();
            ast_create_query.attach = false;
            ast_create_query.database = database;
        }

        return ast;
    }
}

void DatabaseOnDisk::createTable(
    const Context & context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    const auto & settings = context.getSettingsRef();

    /// Create a file with metadata if necessary - if the query is not ATTACH.
    /// Write the query of `ATTACH table` to it.

    /** The code is based on the assumption that all threads share the same order of operations
      * - creating the .sql.tmp file;
      * - adding a table to `tables`;
      * - rename .sql.tmp to .sql.
      */

    /// A race condition would be possible if a table with the same name is simultaneously created using CREATE and using ATTACH.
    /// But there is protection from it - see using DDLGuard in InterpreterCreateQuery.

    if (isTableExist(context, table_name))
        throw Exception("Table " + getDatabaseName() + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);

    String table_metadata_path = getTableMetadataPath(table_name);
    String table_metadata_tmp_path = table_metadata_path + ".tmp";
    String statement;

    {
        statement = getTableDefinitionFromCreateQuery(query);

        /// Exclusive flags guarantees, that table is not created right now in another thread. Otherwise, exception will be thrown.
        WriteBufferFromFile out(table_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (settings.fsync_metadata)
            out.sync();
        out.close();
    }

    try
    {
        /// Add a table to the map of known tables.
        attachTable(table_name, table);

        /// If it was ATTACH query and file with table metadata already exist
        /// (so, ATTACH is done after DETACH), then rename atomically replaces old file with new one.
        Poco::File(table_metadata_tmp_path).renameTo(table_metadata_path);
    }
    catch (...)
    {
        Poco::File(table_metadata_tmp_path).remove();
        throw;
    }
}


void DatabaseOnDisk::removeTable(
    const Context & /*context*/,
    const String & table_name)
{
    StoragePtr res = detachTable(table_name);

    String table_metadata_path = getTableMetadataPath(table_name);

    try
    {
        Poco::File(table_metadata_path).remove();
    }
    catch (...)
    {
        try
        {
            Poco::File(table_metadata_path + ".tmp_drop").remove();
            return;
        }
        catch (...)
        {
            LOG_WARNING(getLogger(), getCurrentExceptionMessage(__PRETTY_FUNCTION__));
        }
        attachTable(table_name, res);
        throw;
    }
}

void DatabaseOnDisk::renameTable(
    const Context & context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name,
    TableStructureWriteLockHolder & lock)
{
    DatabaseOnDisk * to_database_concrete = typeid_cast<DatabaseOnDisk *>(&to_database);

    if (!to_database_concrete)
        throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);

    StoragePtr table = tryGetTable(context, table_name);

    if (!table)
        throw Exception("Table " + getDatabaseName() + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

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

    ASTPtr ast = getQueryFromMetadata(detail::getTableMetadataPath(getMetadataPath(), table_name));
    if (!ast)
        throw Exception("There is no metadata file for table " + table_name, ErrorCodes::FILE_DOESNT_EXIST);
    ast->as<ASTCreateQuery &>().table = to_table_name;

    /// NOTE Non-atomic.
    to_database_concrete->createTable(context, to_table_name, table, ast);
    removeTable(context, table_name);
}

ASTPtr DatabaseOnDisk::getCreateTableQueryImpl(const Context & context,
                                                 const String & table_name, bool throw_on_error) const
{
    ASTPtr ast;

    auto table_metadata_path = detail::getTableMetadataPath(getMetadataPath(), table_name);
    ast = getCreateQueryFromMetadata(table_metadata_path, getDatabaseName(), throw_on_error);
    if (!ast && throw_on_error)
    {
        /// Handle system.* tables for which there are no table.sql files.
        bool has_table = tryGetTable(context, table_name) != nullptr;

        auto msg = has_table
                   ? "There is no CREATE TABLE query for table "
                   : "There is no metadata file for table ";

        throw Exception(msg + table_name, ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
    }

    return ast;
}

ASTPtr DatabaseOnDisk::getCreateTableQuery(const Context & context, const String & table_name) const
{
    return getCreateTableQueryImpl(context, table_name, true);
}

ASTPtr DatabaseOnDisk::tryGetCreateTableQuery(const Context & context, const String & table_name) const
{
    return getCreateTableQueryImpl(context, table_name, false);
}

ASTPtr DatabaseOnDisk::getCreateDatabaseQuery(const Context & /*context*/) const
{
    ASTPtr ast;

    auto database_metadata_path = detail::getDatabaseMetadataPath(getMetadataPath());
    ast = getCreateQueryFromMetadata(database_metadata_path, getDatabaseName(), true);
    if (!ast)
    {
        /// Handle databases (such as default) for which there are no database.sql files.
        String query = "CREATE DATABASE " + backQuoteIfNeed(getDatabaseName()) + " ENGINE = " + getEngineName();
        ParserCreateQuery parser;
        ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0);
    }

    return ast;
}

void DatabaseOnDisk::drop()
{
    Poco::File(getDataPath()).remove(false);
    Poco::File(getMetadataPath()).remove(false);
}

String DatabaseOnDisk::getTableMetadataPath(const String & table_name) const
{
    return detail::getTableMetadataPath(getMetadataPath(), table_name);
}

}