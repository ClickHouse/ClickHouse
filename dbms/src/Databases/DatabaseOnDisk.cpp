#include <Databases/DatabaseOnDisk.h>

#include <Common/escapeForFileName.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Storages/IStorage.h>

#include <common/logger_useful.h>
#include <Poco/DirectoryIterator.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int FILE_DOESNT_EXIST;
    extern const int INCORRECT_FILE_NAME;
    extern const int SYNTAX_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
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

    ASTPtr getQueryFromMetadata(const String & metadata_path, bool throw_on_error)
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
    IDatabase & database,
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

    if (database.isTableExist(context, table_name))
        throw Exception("Table " + backQuote(database.getDatabaseName()) + "." + backQuote(table_name) + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);

    String table_metadata_path = database.getTableMetadataPath(table_name);
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
        database.attachTable(table_name, table);

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
    IDatabase & database,
    const Context & /* context */,
    const String & table_name,
    Poco::Logger * log)
{
    StoragePtr res = database.detachTable(table_name);

    String table_metadata_path = database.getTableMetadataPath(table_name);

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
            LOG_WARNING(log, getCurrentExceptionMessage(__PRETTY_FUNCTION__));
        }
        database.attachTable(table_name, res);
        throw;
    }
}

ASTPtr DatabaseOnDisk::getCreateTableQueryImpl(const IDatabase & database, const Context & context,
                                                 const String & table_name, bool throw_on_error)
{
    ASTPtr ast;

    auto table_metadata_path = detail::getTableMetadataPath(database.getMetadataPath(), table_name);
    ast = detail::getCreateQueryFromMetadata(table_metadata_path, database.getDatabaseName(), throw_on_error);
    if (!ast && throw_on_error)
    {
        /// Handle system.* tables for which there are no table.sql files.
        bool has_table = database.tryGetTable(context, table_name) != nullptr;

        auto msg = has_table
                   ? "There is no CREATE TABLE query for table "
                   : "There is no metadata file for table ";

        throw Exception(msg + backQuote(table_name), ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
    }

    return ast;
}

ASTPtr DatabaseOnDisk::getCreateTableQuery(const IDatabase & database, const Context & context, const String & table_name)
{
    return getCreateTableQueryImpl(database, context, table_name, true);
}

ASTPtr DatabaseOnDisk::tryGetCreateTableQuery(const IDatabase & database, const Context & context, const String & table_name)
{
    return getCreateTableQueryImpl(database, context, table_name, false);
}

ASTPtr DatabaseOnDisk::getCreateDatabaseQuery(const IDatabase & database, const Context & /*context*/)
{
    ASTPtr ast;

    auto database_metadata_path = detail::getDatabaseMetadataPath(database.getMetadataPath());
    ast = detail::getCreateQueryFromMetadata(database_metadata_path, database.getDatabaseName(), true);
    if (!ast)
    {
        /// Handle databases (such as default) for which there are no database.sql files.
        String query = "CREATE DATABASE " + backQuoteIfNeed(database.getDatabaseName()) + " ENGINE = Lazy";
        ParserCreateQuery parser;
        ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0);
    }

    return ast;
}

void DatabaseOnDisk::drop(const IDatabase & database)
{
    Poco::File(database.getDataPath()).remove(false);
    Poco::File(database.getMetadataPath()).remove(false);
}

String DatabaseOnDisk::getTableMetadataPath(const IDatabase & database, const String & table_name)
{
    return detail::getTableMetadataPath(database.getMetadataPath(), table_name);
}

time_t DatabaseOnDisk::getTableMetadataModificationTime(
    const IDatabase & database,
    const String & table_name)
{
    String table_metadata_path = getTableMetadataPath(database, table_name);
    Poco::File meta_file(table_metadata_path);

    if (meta_file.exists())
    {
        return meta_file.getLastModified().epochTime();
    }
    else
    {
        return static_cast<time_t>(0);
    }
}

void DatabaseOnDisk::iterateTableFiles(const IDatabase & database, Poco::Logger * log, const IteratingFunction & iterating_function)
{
    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(database.getMetadataPath()); dir_it != dir_end; ++dir_it)
    {
        /// For '.svn', '.gitignore' directory and similar.
        if (dir_it.name().at(0) == '.')
            continue;

        /// There are .sql.bak files - skip them.
        if (endsWith(dir_it.name(), ".sql.bak"))
            continue;

        // There are files that we tried to delete previously
        static const char * tmp_drop_ext = ".sql.tmp_drop";
        if (endsWith(dir_it.name(), tmp_drop_ext))
        {
            const std::string table_name = dir_it.name().substr(0, dir_it.name().size() - strlen(tmp_drop_ext));
            if (Poco::File(database.getDataPath() + '/' + table_name).exists())
            {
                Poco::File(dir_it->path()).renameTo(table_name + ".sql");
                LOG_WARNING(log, "Table " << backQuote(table_name) << " was not dropped previously");
            }
            else
            {
                LOG_INFO(log, "Removing file " << dir_it->path());
                Poco::File(dir_it->path()).remove();
            }
            continue;
        }

        /// There are files .sql.tmp - delete
        if (endsWith(dir_it.name(), ".sql.tmp"))
        {
            LOG_INFO(log, "Removing file " << dir_it->path());
            Poco::File(dir_it->path()).remove();
            continue;
        }

        /// The required files have names like `table_name.sql`
        if (endsWith(dir_it.name(), ".sql"))
        {
            iterating_function(dir_it.name());
        }
        else
            throw Exception("Incorrect file extension: " + dir_it.name() + " in metadata directory " + database.getMetadataPath(),
                ErrorCodes::INCORRECT_FILE_NAME);
    }
}

}
