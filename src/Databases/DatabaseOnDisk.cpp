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
#include <Common/assert_cast.h>


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
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
}


std::pair<String, StoragePtr> createTableFromAST(
    ASTCreateQuery ast_create_query,
    const String & database_name,
    const String & table_data_path_relative,
    ContextPtr context,
    bool has_force_restore_data_flag)
{
    ast_create_query.attach = true;
    ast_create_query.database = database_name;

    if (ast_create_query.as_table_function)
    {
        const auto & factory = TableFunctionFactory::instance();
        auto table_function = factory.get(ast_create_query.as_table_function, context);
        ColumnsDescription columns;
        if (ast_create_query.columns_list && ast_create_query.columns_list->columns)
            columns = InterpreterCreateQuery::getColumnsDescription(*ast_create_query.columns_list->columns, context, true);
        StoragePtr storage = table_function->execute(ast_create_query.as_table_function, context, ast_create_query.table, std::move(columns));
        storage->renameInMemory(ast_create_query);
        return {ast_create_query.table, storage};
    }

    ColumnsDescription columns;
    ConstraintsDescription constraints;

    if (!ast_create_query.is_dictionary)
    {
        /// We do not directly use `InterpreterCreateQuery::execute`, because
        /// - the database has not been loaded yet;
        /// - the code is simpler, since the query is already brought to a suitable form.
        if (!ast_create_query.columns_list || !ast_create_query.columns_list->columns)
            throw Exception("Missing definition of columns.", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

        columns = InterpreterCreateQuery::getColumnsDescription(*ast_create_query.columns_list->columns, context, true);
        constraints = InterpreterCreateQuery::getConstraintsDescription(ast_create_query.columns_list->constraints);
    }

    return
    {
        ast_create_query.table,
        StorageFactory::instance().get(
            ast_create_query,
            table_data_path_relative,
            context,
            context->getGlobalContext(),
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
        WriteBufferFromOwnString query_buf;
        formatAST(*query, query_buf, true);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query '{}' is not CREATE query", query_buf.str());
    }

    if (!create->is_dictionary)
        create->attach = true;

    /// We remove everything that is not needed for ATTACH from the query.
    assert(!create->temporary);
    create->database.clear();
    create->as_database.clear();
    create->as_table.clear();
    create->if_not_exists = false;
    create->is_populate = false;
    create->replace_view = false;
    create->replace_table = false;
    create->create_or_replace = false;

    /// For views it is necessary to save the SELECT query itself, for the rest - on the contrary
    if (!create->isView())
        create->select = nullptr;

    create->format = nullptr;
    create->out_file = nullptr;

    if (create->uuid != UUIDHelpers::Nil)
        create->table = TABLE_WITH_UUID_NAME_PLACEHOLDER;

    WriteBufferFromOwnString statement_buf;
    formatAST(*create, statement_buf, false);
    writeChar('\n', statement_buf);
    return statement_buf.str();
}

void applyMetadataChangesToCreateQuery(const ASTPtr & query, const StorageInMemoryMetadata & metadata)
{
    auto & ast_create_query = query->as<ASTCreateQuery &>();

    bool has_structure = ast_create_query.columns_list && ast_create_query.columns_list->columns;
    if (ast_create_query.as_table_function && !has_structure)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot alter table {} because it was created AS table function"
                                                     " and doesn't have structure in metadata", backQuote(ast_create_query.table));

    assert(has_structure);
    ASTPtr new_columns = InterpreterCreateQuery::formatColumns(metadata.columns);
    ASTPtr new_indices = InterpreterCreateQuery::formatIndices(metadata.secondary_indices);
    ASTPtr new_constraints = InterpreterCreateQuery::formatConstraints(metadata.constraints);
    ASTPtr new_projections = InterpreterCreateQuery::formatProjections(metadata.projections);

    ast_create_query.columns_list->replace(ast_create_query.columns_list->columns, new_columns);
    ast_create_query.columns_list->setOrReplace(ast_create_query.columns_list->indices, new_indices);
    ast_create_query.columns_list->setOrReplace(ast_create_query.columns_list->constraints, new_constraints);
    ast_create_query.columns_list->setOrReplace(ast_create_query.columns_list->projections, new_projections);

    if (metadata.select.select_query)
    {
        query->replace(ast_create_query.select, metadata.select.select_query);
    }

    /// MaterializedView is one type of CREATE query without storage.
    if (ast_create_query.storage)
    {
        ASTStorage & storage_ast = *ast_create_query.storage;

        bool is_extended_storage_def
            = storage_ast.partition_by || storage_ast.primary_key || storage_ast.order_by || storage_ast.sample_by || storage_ast.settings;

        if (is_extended_storage_def)
        {
            if (metadata.sorting_key.definition_ast)
                storage_ast.set(storage_ast.order_by, metadata.sorting_key.definition_ast);

            if (metadata.primary_key.definition_ast)
                storage_ast.set(storage_ast.primary_key, metadata.primary_key.definition_ast);

            if (metadata.sampling_key.definition_ast)
                storage_ast.set(storage_ast.sample_by, metadata.sampling_key.definition_ast);

            if (metadata.table_ttl.definition_ast)
                storage_ast.set(storage_ast.ttl_table, metadata.table_ttl.definition_ast);
            else if (storage_ast.ttl_table != nullptr) /// TTL was removed
                storage_ast.ttl_table = nullptr;

            if (metadata.settings_changes)
                storage_ast.set(storage_ast.settings, metadata.settings_changes);
        }
    }
}


DatabaseOnDisk::DatabaseOnDisk(
    const String & name,
    const String & metadata_path_,
    const String & data_path_,
    const String & logger,
    ContextPtr local_context)
    : DatabaseWithOwnTablesBase(name, logger, local_context)
    , metadata_path(metadata_path_)
    , data_path(data_path_)
{
    Poco::File(local_context->getPath() + data_path).createDirectories();
    Poco::File(metadata_path).createDirectories();
}


void DatabaseOnDisk::createTable(
    ContextPtr local_context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    const auto & settings = local_context->getSettingsRef();
    const auto & create = query->as<ASTCreateQuery &>();
    assert(table_name == create.table);

    /// Create a file with metadata if necessary - if the query is not ATTACH.
    /// Write the query of `ATTACH table` to it.

    /** The code is based on the assumption that all threads share the same order of operations
      * - creating the .sql.tmp file;
      * - adding a table to `tables`;
      * - rename .sql.tmp to .sql.
      */

    /// A race condition would be possible if a table with the same name is simultaneously created using CREATE and using ATTACH.
    /// But there is protection from it - see using DDLGuard in InterpreterCreateQuery.

    if (isTableExist(table_name, getContext()))
        throw Exception(
            ErrorCodes::TABLE_ALREADY_EXISTS, "Table {}.{} already exists", backQuote(getDatabaseName()), backQuote(table_name));

    String table_metadata_path = getObjectMetadataPath(table_name);

    if (create.attach_short_syntax)
    {
        /// Metadata already exists, table was detached
        removeDetachedPermanentlyFlag(local_context, table_name, table_metadata_path, true);
        attachTable(table_name, table, getTableDataPath(create));
        return;
    }

    if (!create.attach)
        checkMetadataFilenameAvailability(table_name);

    if (create.attach && Poco::File(table_metadata_path).exists())
    {
        ASTPtr ast_detached = parseQueryFromMetadata(log, local_context, table_metadata_path);
        auto & create_detached = ast_detached->as<ASTCreateQuery &>();

        // either both should be Nil, either values should be equal
        if (create.uuid != create_detached.uuid)
            throw Exception(
                    ErrorCodes::TABLE_ALREADY_EXISTS,
                    "Table {}.{} already exist (detached permanently). To attach it back "
                    "you need to use short ATTACH syntax or a full statement with the same UUID",
                    backQuote(getDatabaseName()), backQuote(table_name));
    }

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

    commitCreateTable(create, table, table_metadata_tmp_path, table_metadata_path, local_context);

    removeDetachedPermanentlyFlag(local_context, table_name, table_metadata_path, false);
}

/// If the table was detached permanently we will have a flag file with
/// .sql.detached extension, is not needed anymore since we attached the table back
void DatabaseOnDisk::removeDetachedPermanentlyFlag(ContextPtr, const String & table_name, const String & table_metadata_path, bool) const
{
    try
    {
        auto detached_permanently_flag = Poco::File(table_metadata_path + detached_suffix);

        if (detached_permanently_flag.exists())
            detached_permanently_flag.remove();
    }
    catch (Exception & e)
    {
        e.addMessage("while trying to remove permanently detached flag. Table {}.{} may still be marked as permanently detached, and will not be reattached during server restart.", backQuote(getDatabaseName()), backQuote(table_name));
        throw;
    }
}

void DatabaseOnDisk::commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                                       const String & table_metadata_tmp_path, const String & table_metadata_path,
                                       ContextPtr /*query_context*/)
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

void DatabaseOnDisk::detachTablePermanently(ContextPtr, const String & table_name)
{
    auto table = detachTable(table_name);

    Poco::File detached_permanently_flag(getObjectMetadataPath(table_name) + detached_suffix);
    try
    {
        detached_permanently_flag.createFile();
    }
    catch (Exception & e)
    {
        e.addMessage("while trying to set permanently detached flag. Table {}.{} may be reattached during server restart.", backQuote(getDatabaseName()), backQuote(table_name));
        throw;
    }
}

void DatabaseOnDisk::dropTable(ContextPtr local_context, const String & table_name, bool /*no_delay*/)
{
    String table_metadata_path = getObjectMetadataPath(table_name);
    String table_metadata_path_drop = table_metadata_path + drop_suffix;
    String table_data_path_relative = getTableDataPath(table_name);
    if (table_data_path_relative.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Path is empty");

    StoragePtr table = detachTable(table_name);

    /// This is possible for Lazy database.
    if (!table)
        return;

    bool renamed = false;
    try
    {
        Poco::File(table_metadata_path).renameTo(table_metadata_path_drop);
        renamed = true;
        table->drop();
        table->is_dropped = true;

        Poco::File table_data_dir{local_context->getPath() + table_data_path_relative};
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

void DatabaseOnDisk::checkMetadataFilenameAvailability(const String & to_table_name) const
{
    std::unique_lock lock(mutex);
    checkMetadataFilenameAvailabilityUnlocked(to_table_name, lock);
}

void DatabaseOnDisk::checkMetadataFilenameAvailabilityUnlocked(const String & to_table_name, std::unique_lock<std::mutex> &) const
{
    String table_metadata_path = getObjectMetadataPath(to_table_name);

    if (Poco::File(table_metadata_path).exists())
    {
        auto detached_permanently_flag = Poco::File(table_metadata_path + detached_suffix);

        if (detached_permanently_flag.exists())
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {}.{} already exists (detached permanently)", backQuote(database_name), backQuote(to_table_name));
        else
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {}.{} already exists (detached)", backQuote(database_name), backQuote(to_table_name));
    }
}

void DatabaseOnDisk::renameTable(
        ContextPtr local_context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name,
        bool exchange,
        bool dictionary)
{
    if (exchange)
        throw Exception("Tables can be exchanged only in Atomic databases", ErrorCodes::NOT_IMPLEMENTED);
    if (dictionary)
        throw Exception("Dictionaries can be renamed only in Atomic databases", ErrorCodes::NOT_IMPLEMENTED);

    bool from_ordinary_to_atomic = false;
    bool from_atomic_to_ordinary = false;
    if (typeid(*this) != typeid(to_database))
    {
        if (typeid_cast<DatabaseOrdinary *>(this) && typeid_cast<DatabaseAtomic *>(&to_database))
            from_ordinary_to_atomic = true;
        else if (typeid_cast<DatabaseAtomic *>(this) && typeid_cast<DatabaseOrdinary *>(&to_database))
            from_atomic_to_ordinary = true;
        else if (dynamic_cast<DatabaseAtomic *>(this) && typeid_cast<DatabaseOrdinary *>(&to_database) && getEngineName() == "Replicated")
            from_atomic_to_ordinary = true;
        else
            throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);
    }

    auto table_data_relative_path = getTableDataPath(table_name);
    TableExclusiveLockHolder table_lock;
    String table_metadata_path;
    ASTPtr attach_query;
    /// DatabaseLazy::detachTable may return nullptr even if table exists, so we need tryGetTable for this case.
    StoragePtr table = tryGetTable(table_name, getContext());
    detachTable(table_name);
    UUID prev_uuid = UUIDHelpers::Nil;
    try
    {
        table_lock = table->lockExclusively(
            local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);

        table_metadata_path = getObjectMetadataPath(table_name);
        attach_query = parseQueryFromMetadata(log, local_context, table_metadata_path);
        auto & create = attach_query->as<ASTCreateQuery &>();
        create.database = to_database.getDatabaseName();
        create.table = to_table_name;
        if (from_ordinary_to_atomic)
            create.uuid = UUIDHelpers::generateV4();
        if (from_atomic_to_ordinary)
            std::swap(create.uuid, prev_uuid);

        if (auto * target_db = dynamic_cast<DatabaseOnDisk *>(&to_database))
            target_db->checkMetadataFilenameAvailability(to_table_name);

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
    to_database.createTable(local_context, to_table_name, table, attach_query);

    Poco::File(table_metadata_path).remove();

    if (from_atomic_to_ordinary)
    {
        auto & atomic_db = dynamic_cast<DatabaseAtomic &>(*this);
        /// Special case: usually no actions with symlinks are required when detaching/attaching table,
        /// but not when moving from Atomic database to Ordinary
        if (table->storesDataOnDisk())
            atomic_db.tryRemoveSymlink(table_name);
        /// Forget about UUID, now it's possible to reuse it for new table
        DatabaseCatalog::instance().removeUUIDMappingFinally(prev_uuid);
        atomic_db.setDetachedTableNotInUseForce(prev_uuid);
    }
}


/// It returns create table statement (even if table is detached)
ASTPtr DatabaseOnDisk::getCreateTableQueryImpl(const String & table_name, ContextPtr, bool throw_on_error) const
{
    ASTPtr ast;
    bool has_table = tryGetTable(table_name, getContext()) != nullptr;
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

    auto settings = getContext()->getSettingsRef();
    {
        std::lock_guard lock(mutex);
        auto database_metadata_path = getContext()->getPath() + "metadata/" + escapeForFileName(database_name) + ".sql";
        ast = parseQueryFromMetadata(log, getContext(), database_metadata_path, true);
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.attach = false;
        ast_create_query.database = database_name;
    }
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

void DatabaseOnDisk::drop(ContextPtr local_context)
{
    assert(tables.empty());
    Poco::File(local_context->getPath() + getDataPath()).remove(false);
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

void DatabaseOnDisk::iterateMetadataFiles(ContextPtr local_context, const IteratingFunction & process_metadata_file) const
{
    auto process_tmp_drop_metadata_file = [&](const String & file_name)
    {
        assert(getUUID() == UUIDHelpers::Nil);
        static const char * tmp_drop_ext = ".sql.tmp_drop";
        const std::string object_name = file_name.substr(0, file_name.size() - strlen(tmp_drop_ext));
        if (Poco::File(local_context->getPath() + getDataPath() + '/' + object_name).exists())
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

        /// Permanently detached table flag
        if (endsWith(dir_it.name(), ".sql.detached"))
            continue;

        if (endsWith(dir_it.name(), ".sql.tmp_drop"))
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

ASTPtr DatabaseOnDisk::parseQueryFromMetadata(
    Poco::Logger * logger,
    ContextPtr local_context,
    const String & metadata_file_path,
    bool throw_on_error /*= true*/,
    bool remove_empty /*= false*/)
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
        if (logger)
            LOG_ERROR(logger, "File {} is empty. Removing.", metadata_file_path);
        Poco::File(metadata_file_path).remove();
        return nullptr;
    }

    auto settings = local_context->getSettingsRef();
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
    if (!create.table.empty() && create.uuid != UUIDHelpers::Nil)
    {
        String table_name = Poco::Path(metadata_file_path).makeFile().getBaseName();
        table_name = unescapeForFileName(table_name);

        if (create.table != TABLE_WITH_UUID_NAME_PLACEHOLDER && logger)
            LOG_WARNING(
                logger,
                "File {} contains both UUID and table name. Will use name `{}` instead of `{}`",
                metadata_file_path,
                table_name,
                create.table);
        create.table = table_name;
    }

    return ast;
}

ASTPtr DatabaseOnDisk::getCreateQueryFromMetadata(const String & database_metadata_path, bool throw_on_error) const
{
    ASTPtr ast = parseQueryFromMetadata(log, getContext(), database_metadata_path, throw_on_error);

    if (ast)
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.attach = false;
        ast_create_query.database = getDatabaseName();
    }

    return ast;
}

}
