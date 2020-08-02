#include <Databases/DatabaseOnDisk.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/escapeForFileName.h>

#include <common/logger_useful.h>
#include <Poco/DirectoryIterator.h>

#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseAtomic.h>


namespace DB
{

static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

namespace ErrorCodes
{
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int FILE_DOESNT_EXIST;
    extern const int INCORRECT_FILE_NAME;
    extern const int SYNTAX_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int DICTIONARY_ALREADY_EXISTS;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
}


std::pair<String, StoragePtr> createTableFromAST(
    ASTCreateQuery ast_create_query,
    const String & database_name,
    const String & table_data_path_relative,
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
        storage->renameInMemory(ast_create_query);
        return {ast_create_query.table, storage};
    }
    /// We do not directly use `InterpreterCreateQuery::execute`, because
    /// - the database has not been loaded yet;
    /// - the code is simpler, since the query is already brought to a suitable form.
    if (!ast_create_query.columns_list || !ast_create_query.columns_list->columns)
        throw Exception("Missing definition of columns.", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

    ColumnsDescription columns = InterpreterCreateQuery::getColumnsDescription(*ast_create_query.columns_list->columns, context, false);
    ConstraintsDescription constraints = InterpreterCreateQuery::getConstraintsDescription(ast_create_query.columns_list->constraints);

    return
    {
        ast_create_query.table,
        StorageFactory::instance().get(
            ast_create_query,
            table_data_path_relative,
            context,
            context.getGlobalContext(),
            columns,
            constraints,
            has_force_restore_data_flag)
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

    if (create->uuid != UUIDHelpers::Nil)
        create->table = TABLE_WITH_UUID_NAME_PLACEHOLDER;

    std::ostringstream statement_stream;
    formatAST(*create, statement_stream, false);
    statement_stream << '\n';
    return statement_stream.str();
}

DatabaseOnDisk::DatabaseOnDisk(
    const String & name,
    const String & metadata_path_,
    const String & data_path_,
    const String & logger,
    const Context & context)
    : DatabaseWithOwnTablesBase(name, logger, context)
    , metadata_path(metadata_path_)
    , data_path(data_path_)
{
    Poco::File(context.getPath() + data_path).createDirectories();
    Poco::File(metadata_path).createDirectories();
}


void DatabaseOnDisk::createTable(
    const Context & context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    const auto & settings = context.getSettingsRef();
    const auto & create = query->as<ASTCreateQuery &>();
    assert(getDatabaseName() == create.database && table_name == create.table);

    /// Create a file with metadata if necessary - if the query is not ATTACH.
    /// Write the query of `ATTACH table` to it.

    /** The code is based on the assumption that all threads share the same order of operations
      * - creating the .sql.tmp file;
      * - adding a table to `tables`;
      * - rename .sql.tmp to .sql.
      */

    /// A race condition would be possible if a table with the same name is simultaneously created using CREATE and using ATTACH.
    /// But there is protection from it - see using DDLGuard in InterpreterCreateQuery.

    if (isDictionaryExist(table_name))
        throw Exception("Dictionary " + backQuote(getDatabaseName()) + "." + backQuote(table_name) + " already exists.",
            ErrorCodes::DICTIONARY_ALREADY_EXISTS);

    if (isTableExist(table_name, global_context))
        throw Exception("Table " + backQuote(getDatabaseName()) + "." + backQuote(table_name) + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);

    if (create.attach_short_syntax)
    {
        /// Metadata already exists, table was detached
        attachTable(table_name, table, getTableDataPath(create));
        return;
    }

    String table_metadata_path = getObjectMetadataPath(table_name);
    String table_metadata_tmp_path = table_metadata_path + create_suffix;
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

    commitCreateTable(create, table, table_metadata_tmp_path, table_metadata_path);
}

void DatabaseOnDisk::commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                                       const String & table_metadata_tmp_path, const String & table_metadata_path)
{
    try
    {
        /// Add a table to the map of known tables.
        attachTable(query.table, table, getTableDataPath(query));

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

void DatabaseOnDisk::dropTable(const Context & context, const String & table_name, bool /*no_delay*/)
{
    String table_metadata_path = getObjectMetadataPath(table_name);
    String table_metadata_path_drop = table_metadata_path + drop_suffix;
    String table_data_path_relative = getTableDataPath(table_name);
    assert(!table_data_path_relative.empty());

    StoragePtr table = detachTable(table_name);
    bool renamed = false;
    try
    {
        Poco::File(table_metadata_path).renameTo(table_metadata_path_drop);
        renamed = true;
        table->drop();
        table->is_dropped = true;

        Poco::File table_data_dir{context.getPath() + table_data_path_relative};
        if (table_data_dir.exists())
            table_data_dir.remove(true);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessage(__PRETTY_FUNCTION__));
        attachTable(table_name, table, table_data_path_relative);
        if (renamed)
            Poco::File(table_metadata_path_drop).renameTo(table_metadata_path);
        throw;
    }

    Poco::File(table_metadata_path_drop).remove();
}

void DatabaseOnDisk::renameTable(
        const Context & context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name,
        bool exchange)
{
    if (exchange)
        throw Exception("Tables can be exchanged only in Atomic databases", ErrorCodes::NOT_IMPLEMENTED);

    bool from_ordinary_to_atomic = false;
    bool from_atomic_to_ordinary = false;
    if (typeid(*this) != typeid(to_database))
    {
        if (typeid_cast<DatabaseOrdinary *>(this) && typeid_cast<DatabaseAtomic *>(&to_database))
            from_ordinary_to_atomic = true;
        else if (typeid_cast<DatabaseAtomic *>(this) && typeid_cast<DatabaseOrdinary *>(&to_database))
            from_atomic_to_ordinary = true;
        else
            throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);
    }

    auto table_data_relative_path = getTableDataPath(table_name);
    TableExclusiveLockHolder table_lock;
    String table_metadata_path;
    ASTPtr attach_query;
    /// DatabaseLazy::detachTable may return nullptr even if table exists, so we need tryGetTable for this case.
    StoragePtr table = tryGetTable(table_name, global_context);
    detachTable(table_name);
    try
    {
        table_lock = table->lockExclusively(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);

        table_metadata_path = getObjectMetadataPath(table_name);
        attach_query = parseQueryFromMetadata(log, context, table_metadata_path);
        auto & create = attach_query->as<ASTCreateQuery &>();
        create.database = to_database.getDatabaseName();
        create.table = to_table_name;
        if (from_ordinary_to_atomic)
            create.uuid = UUIDHelpers::generateV4();
        if (from_atomic_to_ordinary)
            create.uuid = UUIDHelpers::Nil;

        /// Notify the table that it is renamed. It will move data to new path (if it stores data on disk) and update StorageID
        table->rename(to_database.getTableDataPath(create), StorageID(create));
    }
    catch (const Exception &)
    {
        attachTable(table_name, table, table_data_relative_path);
        throw;
    }
    catch (const Poco::Exception & e)
    {
        attachTable(table_name, table, table_data_relative_path);
        /// Better diagnostics.
        throw Exception{Exception::CreateFromPocoTag{}, e};
    }

    /// Now table data are moved to new database, so we must add metadata and attach table to new database
    to_database.createTable(context, to_table_name, table, attach_query);

    Poco::File(table_metadata_path).remove();
}

ASTPtr DatabaseOnDisk::getCreateTableQueryImpl(const String & table_name, const Context &, bool throw_on_error) const
{
    ASTPtr ast;
    bool has_table = tryGetTable(table_name, global_context) != nullptr;
    auto table_metadata_path = getObjectMetadataPath(table_name);
    try
    {
        ast = getCreateQueryFromMetadata(table_metadata_path, throw_on_error);
    }
    catch (const Exception & e)
    {
        if (!has_table && e.code() == ErrorCodes::FILE_DOESNT_EXIST && throw_on_error)
            throw Exception{"Table " + backQuote(table_name) + " doesn't exist",
                            ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY};
        else if (throw_on_error)
            throw;
    }
    return ast;
}

ASTPtr DatabaseOnDisk::getCreateDatabaseQuery() const
{
    ASTPtr ast;

    auto settings = global_context.getSettingsRef();
    auto metadata_dir_path = getMetadataPath();
    auto database_metadata_path = metadata_dir_path.substr(0, metadata_dir_path.size() - 1) + ".sql";
    ast = getCreateQueryFromMetadata(database_metadata_path, true);
    if (!ast)
    {
        /// Handle databases (such as default) for which there are no database.sql files.
        /// If database.sql doesn't exist, then engine is Ordinary
        String query = "CREATE DATABASE " + backQuoteIfNeed(getDatabaseName()) + " ENGINE = Ordinary";
        ParserCreateQuery parser;
        ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings.max_parser_depth);
    }

    return ast;
}

void DatabaseOnDisk::drop(const Context & context)
{
    Poco::File(context.getPath() + getDataPath()).remove(false);
    Poco::File(getMetadataPath()).remove(false);
}

String DatabaseOnDisk::getObjectMetadataPath(const String & object_name) const
{
    return getMetadataPath() + escapeForFileName(object_name) + ".sql";
}

time_t DatabaseOnDisk::getObjectMetadataModificationTime(const String & object_name) const
{
    String table_metadata_path = getObjectMetadataPath(object_name);
    Poco::File meta_file(table_metadata_path);

    if (meta_file.exists())
        return meta_file.getLastModified().epochTime();
    else
        return static_cast<time_t>(0);
}

void DatabaseOnDisk::iterateMetadataFiles(const Context & context, const IteratingFunction & process_metadata_file) const
{
    auto process_tmp_drop_metadata_file = [&](const String & file_name)
    {
        assert(getEngineName() != "Atomic");
        static const char * tmp_drop_ext = ".sql.tmp_drop";
        const std::string object_name = file_name.substr(0, file_name.size() - strlen(tmp_drop_ext));
        if (Poco::File(context.getPath() + getDataPath() + '/' + object_name).exists())
        {
            Poco::File(getMetadataPath() + file_name).renameTo(getMetadataPath() + object_name + ".sql");
            LOG_WARNING(log, "Object {} was not dropped previously and will be restored", backQuote(object_name));
            process_metadata_file(object_name + ".sql");
        }
        else
        {
            LOG_INFO(log, "Removing file {}", getMetadataPath() + file_name);
            Poco::File(getMetadataPath() + file_name).remove();
        }
    };

    /// Metadata files to load: name and flag for .tmp_drop files
    std::set<std::pair<String, bool>> metadata_files;

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(getMetadataPath()); dir_it != dir_end; ++dir_it)
    {
        /// For '.svn', '.gitignore' directory and similar.
        if (dir_it.name().at(0) == '.')
            continue;

        /// There are .sql.bak files - skip them.
        if (endsWith(dir_it.name(), ".sql.bak"))
            continue;

        static const char * tmp_drop_ext = ".sql.tmp_drop";
        if (endsWith(dir_it.name(), tmp_drop_ext))
        {
            /// There are files that we tried to delete previously
            metadata_files.emplace(dir_it.name(), false);
        }
        else if (endsWith(dir_it.name(), ".sql.tmp"))
        {
            /// There are files .sql.tmp - delete
            LOG_INFO(log, "Removing file {}", dir_it->path());
            Poco::File(dir_it->path()).remove();
        }
        else if (endsWith(dir_it.name(), ".sql"))
        {
            /// The required files have names like `table_name.sql`
            metadata_files.emplace(dir_it.name(), true);
        }
        else
            throw Exception("Incorrect file extension: " + dir_it.name() + " in metadata directory " + getMetadataPath(),
                ErrorCodes::INCORRECT_FILE_NAME);
    }

    /// Read and parse metadata in parallel
    ThreadPool pool;
    for (const auto & file : metadata_files)
    {
        pool.scheduleOrThrowOnError([&]()
        {
            if (file.second)
                process_metadata_file(file.first);
            else
                process_tmp_drop_metadata_file(file.first);
        });
    }
    pool.wait();
}

ASTPtr DatabaseOnDisk::parseQueryFromMetadata(Poco::Logger * logger, const Context & context, const String & metadata_file_path, bool throw_on_error /*= true*/, bool remove_empty /*= false*/)
{
    String query;

    try
    {
        ReadBufferFromFile in(metadata_file_path, METADATA_FILE_BUFFER_SIZE);
        readStringUntilEOF(query, in);
    }
    catch (const Exception & e)
    {
        if (!throw_on_error && e.code() == ErrorCodes::FILE_DOESNT_EXIST)
            return nullptr;
        else
            throw;
    }

    /** Empty files with metadata are generated after a rough restart of the server.
      * Remove these files to slightly reduce the work of the admins on startup.
      */
    if (remove_empty && query.empty())
    {
        LOG_ERROR(logger, "File {} is empty. Removing.", metadata_file_path);
        Poco::File(metadata_file_path).remove();
        return nullptr;
    }

    auto settings = context.getSettingsRef();
    ParserCreateQuery parser;
    const char * pos = query.data();
    std::string error_message;
    auto ast = tryParseQuery(parser, pos, pos + query.size(), error_message, /* hilite = */ false,
                             "in file " + metadata_file_path, /* allow_multi_statements = */ false, 0, settings.max_parser_depth);

    if (!ast && throw_on_error)
        throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);
    else if (!ast)
        return nullptr;

    auto & create = ast->as<ASTCreateQuery &>();
    if (create.uuid != UUIDHelpers::Nil)
    {
        String table_name = Poco::Path(metadata_file_path).makeFile().getBaseName();
        table_name = unescapeForFileName(table_name);

        if (create.table != TABLE_WITH_UUID_NAME_PLACEHOLDER)
            LOG_WARNING(logger, "File {} contains both UUID and table name. Will use name `{}` instead of `{}`", metadata_file_path, table_name, create.table);
        create.table = table_name;
    }

    return ast;
}

ASTPtr DatabaseOnDisk::getCreateQueryFromMetadata(const String & database_metadata_path, bool throw_on_error) const
{
    ASTPtr ast = parseQueryFromMetadata(log, global_context, database_metadata_path, throw_on_error);

    if (ast)
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.attach = false;
        ast_create_query.database = database_name;
    }

    return ast;
}

}
