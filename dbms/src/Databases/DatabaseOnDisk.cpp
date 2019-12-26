#include <Databases/DatabaseOnDisk.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalLoaderPresetConfigRepository.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/escapeForFileName.h>

#include <common/logger_useful.h>
#include <ext/scope_guard.h>
#include <Poco/DirectoryIterator.h>



namespace DB
{

static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

namespace ErrorCodes
{
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int CANNOT_GET_CREATE_DICTIONARY_QUERY;
    extern const int FILE_DOESNT_EXIST;
    extern const int INCORRECT_FILE_NAME;
    extern const int SYNTAX_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int DICTIONARY_ALREADY_EXISTS;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
}


namespace detail
{
    String getObjectMetadataPath(const String & base_path, const String & table_name)
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


ASTPtr parseCreateQueryFromMetadataFile(const String & filepath, Poco::Logger * log)
{
    String definition;
    {
        char in_buf[METADATA_FILE_BUFFER_SIZE];
        ReadBufferFromFile in(filepath, METADATA_FILE_BUFFER_SIZE, -1, in_buf);
        readStringUntilEOF(definition, in);
    }

    /** Empty files with metadata are generated after a rough restart of the server.
      * Remove these files to slightly reduce the work of the admins on startup.
      */
    if (definition.empty())
    {
        LOG_ERROR(log, "File " << filepath << " is empty. Removing.");
        Poco::File(filepath).remove();
        return nullptr;
    }

    ParserCreateQuery parser_create;
    ASTPtr result = parseQuery(parser_create, definition, "in file " + filepath, 0);
    return result;
}



std::pair<String, StoragePtr> createTableFromAST(
    ASTCreateQuery ast_create_query,
    const String & database_name,
    const String & database_data_path_relative,
    Context & context,
    bool has_force_restore_data_flag)
{
    ast_create_query.attach = true;
    ast_create_query.database = database_name;

    if (ast_create_query.as_table_function)
    {
        const auto & table_function = ast_create_query.as_table_function->as<ASTFunction &>();
        const auto & factory = TableFunctionFactory::instance();
        StoragePtr storage = factory.get(table_function.name, context)->execute(ast_create_query.as_table_function, context, ast_create_query.table);
        return {ast_create_query.table, storage};
    }
    /// We do not directly use `InterpreterCreateQuery::execute`, because
    /// - the database has not been created yet;
    /// - the code is simpler, since the query is already brought to a suitable form.
    if (!ast_create_query.columns_list || !ast_create_query.columns_list->columns)
        throw Exception("Missing definition of columns.", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

    ColumnsDescription columns = InterpreterCreateQuery::getColumnsDescription(*ast_create_query.columns_list->columns, context);
    ConstraintsDescription constraints = InterpreterCreateQuery::getConstraintsDescription(ast_create_query.columns_list->constraints);

    String table_data_path_relative = database_data_path_relative + escapeForFileName(ast_create_query.table) + '/';
    return
    {
        ast_create_query.table,
        StorageFactory::instance().get(
            ast_create_query,
            table_data_path_relative, ast_create_query.table, database_name, context, context.getGlobalContext(),
            columns, constraints,
            true, has_force_restore_data_flag)
    };
}


String getObjectDefinitionFromCreateQuery(const ASTPtr & query)
{
    ASTPtr query_clone = query->clone();
    auto * create = query_clone->as<ASTCreateQuery>();

    if (!create)
    {
        std::ostringstream query_stream;
        formatAST(*query, query_stream, true);
        throw Exception("Query '" + query_stream.str() + "' is not CREATE query", ErrorCodes::LOGICAL_ERROR);
    }

    if (!create->is_dictionary)
        create->attach = true;

    /// We remove everything that is not needed for ATTACH from the query.
    create->database.clear();
    create->as_database.clear();
    create->as_table.clear();
    create->if_not_exists = false;
    create->is_populate = false;
    create->replace_view = false;

    /// For views it is necessary to save the SELECT query itself, for the rest - on the contrary
    if (!create->is_view && !create->is_materialized_view && !create->is_live_view)
        create->select = nullptr;

    create->format = nullptr;
    create->out_file = nullptr;

    std::ostringstream statement_stream;
    formatAST(*create, statement_stream, false);
    statement_stream << '\n';
    return statement_stream.str();
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

    if (database.isDictionaryExist(context, table_name))
        throw Exception("Dictionary " + backQuote(database.getDatabaseName()) + "." + backQuote(table_name) + " already exists.",
            ErrorCodes::DICTIONARY_ALREADY_EXISTS);

    if (database.isTableExist(context, table_name))
        throw Exception("Table " + backQuote(database.getDatabaseName()) + "." + backQuote(table_name) + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);

    String table_metadata_path = database.getObjectMetadataPath(table_name);
    String table_metadata_tmp_path = table_metadata_path + ".tmp";
    String statement;

    {
        statement = getObjectDefinitionFromCreateQuery(query);

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


void DatabaseOnDisk::createDictionary(
    IDatabase & database,
    const Context & context,
    const String & dictionary_name,
    const ASTPtr & query)
{
    const auto & settings = context.getSettingsRef();

    /** The code is based on the assumption that all threads share the same order of operations:
      * - create the .sql.tmp file;
      * - add the dictionary to ExternalDictionariesLoader;
      * - load the dictionary in case dictionaries_lazy_load == false;
      * - attach the dictionary;
      * - rename .sql.tmp to .sql.
      */

    /// A race condition would be possible if a dictionary with the same name is simultaneously created using CREATE and using ATTACH.
    /// But there is protection from it - see using DDLGuard in InterpreterCreateQuery.
    if (database.isDictionaryExist(context, dictionary_name))
        throw Exception("Dictionary " + backQuote(database.getDatabaseName()) + "." + backQuote(dictionary_name) + " already exists.", ErrorCodes::DICTIONARY_ALREADY_EXISTS);

    /// A dictionary with the same full name could be defined in *.xml config files.
    String full_name = database.getDatabaseName() + "." + dictionary_name;
    auto & external_loader = const_cast<ExternalDictionariesLoader &>(context.getExternalDictionariesLoader());
    if (external_loader.getCurrentStatus(full_name) != ExternalLoader::Status::NOT_EXIST)
        throw Exception(
            "Dictionary " + backQuote(database.getDatabaseName()) + "." + backQuote(dictionary_name) + " already exists.",
            ErrorCodes::DICTIONARY_ALREADY_EXISTS);

    if (database.isTableExist(context, dictionary_name))
        throw Exception("Table " + backQuote(database.getDatabaseName()) + "." + backQuote(dictionary_name) + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);

    String dictionary_metadata_path = database.getObjectMetadataPath(dictionary_name);
    String dictionary_metadata_tmp_path = dictionary_metadata_path + ".tmp";
    String statement = getObjectDefinitionFromCreateQuery(query);

    {
        /// Exclusive flags guarantees, that table is not created right now in another thread. Otherwise, exception will be thrown.
        WriteBufferFromFile out(dictionary_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (settings.fsync_metadata)
            out.sync();
        out.close();
    }

    bool succeeded = false;
    SCOPE_EXIT({
        if (!succeeded)
            Poco::File(dictionary_metadata_tmp_path).remove();
    });

    /// Add a temporary repository containing the dictionary.
    /// We need this temp repository to try loading the dictionary before actually attaching it to the database.
    static std::atomic<size_t> counter = 0;
    String temp_repository_name = String(IExternalLoaderConfigRepository::INTERNAL_REPOSITORY_NAME_PREFIX) + " creating " + full_name + " "
        + std::to_string(++counter);
    external_loader.addConfigRepository(
        temp_repository_name,
        std::make_unique<ExternalLoaderPresetConfigRepository>(
            std::vector{std::pair{dictionary_metadata_tmp_path,
                                  getDictionaryConfigurationFromAST(query->as<const ASTCreateQuery &>(), database.getDatabaseName())}}));
    SCOPE_EXIT({ external_loader.removeConfigRepository(temp_repository_name); });

    bool lazy_load = context.getConfigRef().getBool("dictionaries_lazy_load", true);
    if (!lazy_load)
    {
        /// load() is called here to force loading the dictionary, wait until the loading is finished,
        /// and throw an exception if the loading is failed.
        external_loader.load(full_name);
    }

    database.attachDictionary(dictionary_name, context);
    SCOPE_EXIT({
        if (!succeeded)
            database.detachDictionary(dictionary_name, context);
    });

    /// If it was ATTACH query and file with dictionary metadata already exist
    /// (so, ATTACH is done after DETACH), then rename atomically replaces old file with new one.
    Poco::File(dictionary_metadata_tmp_path).renameTo(dictionary_metadata_path);

    /// ExternalDictionariesLoader doesn't know we renamed the metadata path.
    /// So we have to manually call reloadConfig() here.
    external_loader.reloadConfig(database.getDatabaseName(), full_name);

    /// Everything's ok.
    succeeded = true;
}


void DatabaseOnDisk::removeTable(
    IDatabase & database,
    const Context & /* context */,
    const String & table_name,
    Poco::Logger * log)
{
    StoragePtr res = database.detachTable(table_name);

    String table_metadata_path = database.getObjectMetadataPath(table_name);

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


void DatabaseOnDisk::removeDictionary(
    IDatabase & database,
    const Context & context,
    const String & dictionary_name,
    Poco::Logger * /*log*/)
{
    database.detachDictionary(dictionary_name, context);

    String dictionary_metadata_path = database.getObjectMetadataPath(dictionary_name);
    if (Poco::File(dictionary_metadata_path).exists())
    {
        try
        {
            Poco::File(dictionary_metadata_path).remove();
        }
        catch (...)
        {
            /// If remove was not possible for some reason
            database.attachDictionary(dictionary_name, context);
            throw;
        }
    }
}


ASTPtr DatabaseOnDisk::getCreateTableQueryImpl(
    const IDatabase & database,
    const Context & context,
    const String & table_name,
    bool throw_on_error)
{
    ASTPtr ast;

    auto table_metadata_path = detail::getObjectMetadataPath(database.getMetadataPath(), table_name);
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


ASTPtr DatabaseOnDisk::getCreateDictionaryQueryImpl(
    const IDatabase & database,
    const Context & context,
    const String & dictionary_name,
    bool throw_on_error)
{
    ASTPtr ast;

    auto dictionary_metadata_path = detail::getObjectMetadataPath(database.getMetadataPath(), dictionary_name);
    ast = detail::getCreateQueryFromMetadata(dictionary_metadata_path, database.getDatabaseName(), throw_on_error);
    if (!ast && throw_on_error)
    {
        /// Handle system.* tables for which there are no table.sql files.
        bool has_dictionary = database.isDictionaryExist(context, dictionary_name);

        auto msg = has_dictionary ? "There is no CREATE DICTIONARY query for table " : "There is no metadata file for dictionary ";

        throw Exception(msg + backQuote(dictionary_name), ErrorCodes::CANNOT_GET_CREATE_DICTIONARY_QUERY);
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


ASTPtr DatabaseOnDisk::getCreateDictionaryQuery(const IDatabase & database, const Context & context, const String & dictionary_name)
{
    return getCreateDictionaryQueryImpl(database, context, dictionary_name, true);
}

ASTPtr DatabaseOnDisk::tryGetCreateDictionaryQuery(const IDatabase & database, const Context & context, const String & dictionary_name)
{
    return getCreateDictionaryQueryImpl(database, context, dictionary_name, false);
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

void DatabaseOnDisk::drop(const IDatabase & database, const Context & context)
{
    Poco::File(context.getPath() + database.getDataPath()).remove(false);
    Poco::File(database.getMetadataPath()).remove(false);
}

String DatabaseOnDisk::getObjectMetadataPath(const IDatabase & database, const String & table_name)
{
    return detail::getObjectMetadataPath(database.getMetadataPath(), table_name);
}

time_t DatabaseOnDisk::getObjectMetadataModificationTime(
    const IDatabase & database,
    const String & table_name)
{
    String table_metadata_path = getObjectMetadataPath(database, table_name);
    Poco::File meta_file(table_metadata_path);

    if (meta_file.exists())
        return meta_file.getLastModified().epochTime();
    else
        return static_cast<time_t>(0);
}

void DatabaseOnDisk::iterateMetadataFiles(const IDatabase & database, Poco::Logger * log, const Context & context, const IteratingFunction & iterating_function)
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
            const std::string object_name = dir_it.name().substr(0, dir_it.name().size() - strlen(tmp_drop_ext));
            if (Poco::File(context.getPath() + database.getDataPath() + '/' + object_name).exists())
            {
                /// TODO maybe complete table drop and remove all table data (including data on other volumes and metadata in ZK)
                Poco::File(dir_it->path()).renameTo(database.getMetadataPath() + object_name + ".sql");
                LOG_WARNING(log, "Object " << backQuote(object_name) << " was not dropped previously and will be restored");
                iterating_function(object_name + ".sql");
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
