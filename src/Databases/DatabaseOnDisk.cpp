#include <Databases/DatabaseOnDisk.h>

#include <filesystem>
#include <iterator>
#include <memory>
#include <span>
#include <Core/Settings.h>
#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseOrdinary.h>
#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/assert_cast.h>
#include <Common/computeMaxTableNameLength.h>
#include <Common/escapeForFileName.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>


namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric DatabaseOnDiskThreads;
    extern const Metric DatabaseOnDiskThreadsActive;
    extern const Metric DatabaseOnDiskThreadsScheduled;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool force_remove_data_recursively_on_drop;
    extern const SettingsBool fsync_metadata;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int CANNOT_RMDIR;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int INCORRECT_FILE_NAME;
    extern const int SYNTAX_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int DATABASE_NOT_EMPTY;
    extern const int INCORRECT_QUERY;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


std::pair<String, StoragePtr> createTableFromAST(
    ASTCreateQuery ast_create_query,
    const String & database_name,
    const String & table_data_path_relative,
    ContextMutablePtr context,
    LoadingStrictnessLevel mode)
{
    ast_create_query.attach = true;
    ast_create_query.setDatabase(database_name);

    if (ast_create_query.select && ast_create_query.isView())
        ApplyWithSubqueryVisitor(context).visit(*ast_create_query.select);

    if (ast_create_query.as_table_function)
    {
        const auto & factory = TableFunctionFactory::instance();
        auto table_function_ast = ast_create_query.as_table_function->ptr();
        auto table_function = factory.get(table_function_ast, context);
        ColumnsDescription columns;
        if (ast_create_query.columns_list && ast_create_query.columns_list->columns)
            columns = InterpreterCreateQuery::getColumnsDescription(*ast_create_query.columns_list->columns, context, mode);
        StoragePtr storage = table_function->execute(table_function_ast, context, ast_create_query.getTable(), std::move(columns));
        storage->renameInMemory(ast_create_query);
        return {ast_create_query.getTable(), storage};
    }

    ColumnsDescription columns;
    ConstraintsDescription constraints;

    bool has_columns = true;
    if (ast_create_query.is_dictionary)
        has_columns = false;
    if (ast_create_query.isParameterizedView())
        has_columns = false;

    if (has_columns)
    {
        /// We do not directly use `InterpreterCreateQuery::execute`, because
        /// - the database has not been loaded yet;
        /// - the code is simpler, since the query is already brought to a suitable form.
        if (!ast_create_query.columns_list || !ast_create_query.columns_list->columns)
        {
            if (!ast_create_query.storage || !ast_create_query.storage->engine)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid storage definition in metadata file: "
                                                           "it's a bug or result of manual intervention in metadata files");

            if (!StorageFactory::instance().getStorageFeatures(ast_create_query.storage->engine->name).supports_schema_inference)
                throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED, "Missing definition of columns.");
            /// Leave columns empty.
        }
        else
        {
            columns = InterpreterCreateQuery::getColumnsDescription(*ast_create_query.columns_list->columns, context, mode);
            constraints = InterpreterCreateQuery::getConstraintsDescription(ast_create_query.columns_list->constraints, columns, context);
        }
    }

    /// Before 24.10 it was possible for query settings to be stored with the .sql definition with some engines, which would ignore them
    /// Later (breaking) changes to table storages made the engines throw, which now prevents attaching old definitions which include
    /// those query settings
    /// In order to ignore them now we call `applySettingsFromQuery` which will move the settings from engine to query level
    auto ast = make_intrusive<ASTCreateQuery>(std::move(ast_create_query));
    auto set_context = Context::createCopy(context);
    InterpreterSetQuery::applySettingsFromQuery(ast, set_context);

    return {
        ast->getTable(),
        StorageFactory::instance().get(*ast, table_data_path_relative, context, context->getGlobalContext(), columns, constraints, mode)};
}


String getObjectDefinitionFromCreateQuery(const ASTPtr & query)
{
    ASTPtr query_clone = query->clone();
    auto * create = query_clone->as<ASTCreateQuery>();

    if (!create)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query '{}' is not CREATE query", query->formatForErrorMessage());

    /// Clean the query from temporary flags.
    cleanupObjectDefinitionFromTemporaryFlags(*create);

    if (!create->is_dictionary)
        create->attach = true;

    /// We remove everything that is not needed for ATTACH from the query.
    assert(!create->isTemporary());
    create->database.reset();

    if (create->uuid != UUIDHelpers::Nil)
        create->setTable(TABLE_WITH_UUID_NAME_PLACEHOLDER);

    WriteBufferFromOwnString statement_buf;
    IAST::FormatSettings format_settings(/*one_line=*/false);
    create->format(statement_buf, format_settings);
    writeChar('\n', statement_buf);
    return statement_buf.str();
}


DatabaseOnDisk::DatabaseOnDisk(
    const String & name, const String & metadata_path_, const String & data_path_, const String & logger, ContextPtr local_context)
    : DatabaseWithOwnTablesBase(name, logger, local_context), metadata_path(metadata_path_), data_path(data_path_)
{
}


void DatabaseOnDisk::createDirectories()
{
    std::lock_guard lock(mutex);
    createDirectoriesUnlocked();
}

void DatabaseOnDisk::createDirectoriesUnlocked()
{
    auto db_disk = getDisk();
    db_disk->createDirectories(metadata_path);
    db_disk->createDirectories(data_path);
}


void DatabaseOnDisk::shutdown()
{
    stopLoading();
    DatabaseWithOwnTablesBase::shutdown();
}


void DatabaseOnDisk::createTable(
    ContextPtr local_context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    auto db_disk = getDisk();
    createDirectories();

    const auto & create = query->as<ASTCreateQuery &>();
    assert(table_name == create.getTable());

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

    waitDatabaseStarted();

    String table_metadata_path = getObjectMetadataPath(table_name);

    if (create.attach_short_syntax)
    {
        /// Metadata already exists, table was detached
        assert(db_disk->existsFileOrDirectory(getObjectMetadataPath(table_name)));
        removeDetachedPermanentlyFlag(local_context, table_name, table_metadata_path, true);
        attachTable(local_context, table_name, table, getTableDataPath(create));
        return;
    }

    if (!create.attach)
        checkMetadataFilenameAvailability(table_name);

    if (create.attach && db_disk->existsFileOrDirectory(table_metadata_path))
    {
        ASTPtr ast_detached = parseQueryFromMetadata(log, local_context, db_disk, table_metadata_path);
        auto & create_detached = ast_detached->as<ASTCreateQuery &>();

        // either both should be Nil, either values should be equal
        if (create.uuid != create_detached.uuid)
            throw Exception(
                    ErrorCodes::TABLE_ALREADY_EXISTS,
                    "Table {}.{} already exist (detached or detached permanently). To attach it back "
                    "you need to use short ATTACH syntax (ATTACH TABLE {}.{};)",
                    backQuote(getDatabaseName()), backQuote(table_name),
                    backQuote(getDatabaseName()), backQuote(table_name));
    }

    String table_metadata_tmp_path = table_metadata_path + create_suffix;

    {
        String statement = getObjectDefinitionFromCreateQuery(query);

        /// Exclusive flags guarantees, that table is not created right now in another thread. Otherwise, exception will be thrown.
        const auto & settings = local_context->getSettingsRef();
        writeMetadataFile(
            db_disk,
            /*file_path=*/table_metadata_tmp_path,
            /*content=*/statement,
            /*fsync_metadata=*/settings[Setting::fsync_metadata]);
    }

    commitCreateTable(create, table, table_metadata_tmp_path, table_metadata_path, local_context);
    removeDetachedPermanentlyFlag(local_context, table_name, table_metadata_path, false);
}

/// If the table was detached permanently we will have a flag file with
/// .sql.detached extension, is not needed anymore since we attached the table back
void DatabaseOnDisk::removeDetachedPermanentlyFlag(ContextPtr, const String & table_name, const String & table_metadata_path, bool)
{
    auto db_disk = getDisk();
    try
    {
        fs::path detached_permanently_flag(table_metadata_path + detached_suffix);
        db_disk->removeFileIfExists(detached_permanently_flag);
    }
    catch (Exception & e)
    {
        e.addMessage("while trying to remove permanently detached flag. Table {}.{} may still be marked as permanently detached, and will not be reattached during server restart.", backQuote(getDatabaseName()), backQuote(table_name));
        throw;
    }
}

void DatabaseOnDisk::commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                                       const String & table_metadata_tmp_path, const String & table_metadata_path,
                                       ContextPtr query_context)
{
    auto db_disk = getDisk();
    try
    {
        createDirectories();

        /// Add a table to the map of known tables.
        attachTable(query_context, query.getTable(), table, getTableDataPath(query));

        /// If it was ATTACH query and file with table metadata already exist
        /// (so, ATTACH is done after DETACH), then rename atomically replaces old file with new one.
        db_disk->replaceFile(table_metadata_tmp_path, table_metadata_path);
    }
    catch (...)
    {
        db_disk->removeFileIfExists(table_metadata_tmp_path);
        throw;
    }
}

void DatabaseOnDisk::detachTablePermanently(ContextPtr query_context, const String & table_name)
{
    waitDatabaseStarted();

    auto table = detachTable(query_context, table_name);

    fs::path detached_permanently_flag(getObjectMetadataPath(table_name) + detached_suffix);
    try
    {
        auto db_disk = getDisk();
        db_disk->createFile(detached_permanently_flag);

        std::lock_guard lock(mutex);
        const auto it = snapshot_detached_tables.find(table_name);
        if (it == snapshot_detached_tables.end())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Snapshot doesn't contain info about detached table `{}`", table_name);
        }

        it->second.is_permanently = true;
    }
    catch (Exception & e)
    {
        e.addMessage("while trying to set permanently detached flag. Table {}.{} may be reattached during server restart.", backQuote(getDatabaseName()), backQuote(table_name));
        throw;
    }
}

void DatabaseOnDisk::dropTable(ContextPtr local_context, const String & table_name, bool /*sync*/)
{
    waitDatabaseStarted();

    String table_metadata_path = getObjectMetadataPath(table_name);
    String table_metadata_path_drop = table_metadata_path + drop_suffix;
    String table_data_path_relative = getTableDataPath(table_name);
    if (table_data_path_relative.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Path is empty");

    StoragePtr table = detachTable(local_context, table_name);

    auto db_disk = getDisk();
    bool renamed = false;
    try
    {
        db_disk->replaceFile(table_metadata_path, table_metadata_path_drop);
        renamed = true;
        // The table might be not loaded for Lazy database engine.
        if (table)
        {
            table->drop();
            table->is_dropped = true;
        }
        std::lock_guard lock(mutex);
        snapshot_detached_tables.erase(table_name);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
        if (table)
            attachTable(local_context, table_name, table, table_data_path_relative);
        if (renamed)
            db_disk->replaceFile(table_metadata_path_drop, table_metadata_path);
        throw;
    }

    for (const auto & [disk_name, disk] : getContext()->getDisksMap())
    {
        if (disk->isReadOnly() || !disk->existsDirectory(table_data_path_relative))
            continue;

        LOG_INFO(log, "Removing data directory from disk {} with path {} for dropped table {} ", disk_name, table_data_path_relative, table_name);
        disk->removeRecursive(table_data_path_relative);
    }
    db_disk->removeFileIfExists(table_metadata_path_drop);
}

void DatabaseOnDisk::checkMetadataFilenameAvailability(const String & to_table_name) const
{
    std::lock_guard lock(mutex);
    checkMetadataFilenameAvailabilityUnlocked(to_table_name);
}

void DatabaseOnDisk::checkMetadataFilenameAvailabilityUnlocked(const String & to_table_name) const
{
    auto db_disk = getDisk();
    const String table_metadata_path = getObjectMetadataPath(to_table_name);
    if (db_disk->existsFile(table_metadata_path))
    {
        fs::path detached_permanently_flag(table_metadata_path + detached_suffix);

        if (db_disk->existsFile(detached_permanently_flag))
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS,
                            "Table {}.{} already exists (detached permanently)",
                            backQuote(database_name), backQuote(to_table_name));
        else
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS,
                            "Table {}.{} already exists (detached)",
                            backQuote(database_name), backQuote(to_table_name));
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
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Tables can be exchanged only in Atomic databases");

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
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Moving tables between databases of different engines is not supported");
    }

    createDirectories();
    waitDatabaseStarted();

    auto table_data_relative_path = getTableDataPath(table_name);
    TableExclusiveLockHolder table_lock;
    String table_metadata_path;
    ASTPtr attach_query;
    /// NOTE: the table can be concurrently dropped, and getTable will throw UNKNOWN_TABLE.
    StoragePtr table = getTable(table_name, local_context);
    if (dictionary && !table->isDictionary())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Use RENAME/EXCHANGE TABLE (instead of RENAME/EXCHANGE DICTIONARY) for tables");

    table_lock = table->lockExclusively(local_context->getCurrentQueryId(), local_context->getSettingsRef()[Setting::lock_acquire_timeout]);

    detachTable(local_context, table_name);

    UUID prev_uuid = UUIDHelpers::Nil;
    auto db_disk = getDisk();
    try
    {
        table_metadata_path = getObjectMetadataPath(table_name);
        attach_query = parseQueryFromMetadata(log, local_context, db_disk, table_metadata_path);
        auto & create = attach_query->as<ASTCreateQuery &>();
        create.setDatabase(to_database.getDatabaseName());
        create.setTable(to_table_name);
        if (from_ordinary_to_atomic)
            create.uuid = UUIDHelpers::generateV4();
        if (from_atomic_to_ordinary)
            std::swap(create.uuid, prev_uuid);

        if (auto * target_db = dynamic_cast<DatabaseOnDisk *>(&to_database))
            target_db->checkMetadataFilenameAvailability(to_table_name);

        /// This place is actually quite dangerous. Since data directory is moved to store/
        /// DatabaseCatalog may try to clean it up as unused. We add UUID mapping to avoid this.
        /// However, we may fail after data directory move, but before metadata file creation in the destination db.
        /// In this case nothing will protect data directory (except 30-days timeout).
        /// But this situation (when table in Ordinary database is partially renamed) require manual intervention anyway.
        if (from_ordinary_to_atomic)
        {
            DatabaseCatalog::instance().addUUIDMapping(create.uuid);
            if (table->storesDataOnDisk())
                LOG_INFO(log, "Moving table from {} to {}", table_data_relative_path, to_database.getTableDataPath(create));
        }

        /// Notify the table that it is renamed. It will move data to new path (if it stores data on disk) and update StorageID
        table->rename(to_database.getTableDataPath(create), StorageID(create));
    }
    catch (const Exception &)
    {
        setDetachedTableNotInUseForce(prev_uuid);
        attachTable(local_context, table_name, table, table_data_relative_path);
        throw;
    }
    catch (const Poco::Exception & e)
    {
        setDetachedTableNotInUseForce(prev_uuid);
        attachTable(local_context, table_name, table, table_data_relative_path);
        /// Better diagnostics.
        throw Exception{Exception::CreateFromPocoTag{}, e};
    }

    /// Now table data are moved to new database, so we must add metadata and attach table to new database
    to_database.createTable(local_context, to_table_name, table, attach_query);

    db_disk->removeFileIfExists(table_metadata_path);

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


/// It returns the create table statement (even if table is detached)
ASTPtr DatabaseOnDisk::getCreateTableQueryImpl(const String & table_name, ContextPtr, bool throw_on_error) const
{
    ASTPtr ast;
    StoragePtr storage = tryGetTable(table_name, getContext());
    bool has_table = storage != nullptr;
    bool is_system_storage = false;
    if (has_table)
        is_system_storage = storage->isSystemStorage();

    try
    {
        ast = getCreateQueryFromMetadata(table_name, throw_on_error);
    }
    catch (const Exception & e)
    {
        if (!has_table && e.code() == ErrorCodes::FILE_DOESNT_EXIST && throw_on_error)
            throw Exception(ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY, "Table {} doesn't exist", backQuote(table_name));
        if (!is_system_storage && throw_on_error)
            throw;
    }
    if (!ast && is_system_storage)
        ast = getCreateQueryFromStorage(table_name, storage, throw_on_error);
    return ast;
}

ASTPtr DatabaseOnDisk::getCreateDatabaseQueryImpl() const
{
    auto default_db_disk = getContext()->getDatabaseDisk();
    ASTPtr ast;

    const auto & settings = getContext()->getSettingsRef();
    {
        auto metadata_file_path = DatabaseCatalog::getMetadataFilePath(database_name);
        ast = parseQueryFromMetadata(log, getContext(), default_db_disk, metadata_file_path, true);
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.attach = false;
        ast_create_query.setDatabase(database_name);
    }
    if (!ast)
    {
        /// Handle databases (such as default) for which there are no database.sql files.
        /// If database.sql doesn't exist, then engine is Ordinary
        String query = "CREATE DATABASE " + backQuoteIfNeed(database_name) + " ENGINE = Ordinary";
        ParserCreateQuery parser;
        ast = parseQuery(
            parser, query.data(), query.data() + query.size(), "", 0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
    }

    if (!comment.empty())
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.set(ast_create_query.comment, make_intrusive<ASTLiteral>(comment));
    }

    return ast;
}

void DatabaseOnDisk::drop(ContextPtr local_context)
{
    waitDatabaseStarted();

    auto db_disk = getDisk();
    {
        std::lock_guard lock(mutex);
        assert(tables.empty());
    }
    if (local_context->getSettingsRef()[Setting::force_remove_data_recursively_on_drop])
    {
        db_disk->removeRecursive(data_path);
        db_disk->removeRecursive(getMetadataPath());
    }
    else
    {
        try
        {
            db_disk->removeDirectoryIfExists(data_path);
            db_disk->removeDirectoryIfExists(getMetadataPath());
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::CANNOT_RMDIR)
                throw;
            throw Exception(
                ErrorCodes::DATABASE_NOT_EMPTY,
                "Cannot drop: {}. "
                "Probably database contain some detached tables or metadata leftovers from Ordinary engine. "
                "If you want to remove all data anyway, try to attach database back and drop it again "
                "with enabled force_remove_data_recursively_on_drop setting",
                e.what());
        }
    }
}

String DatabaseOnDisk::getObjectMetadataPath(const String & object_name) const
{
    return getMetadataPath() + escapeForFileName(object_name) + ".sql";
}

time_t DatabaseOnDisk::getObjectMetadataModificationTime(const String & object_name) const
{
    auto db_disk = getDisk();
    String table_metadata_path = getObjectMetadataPath(object_name);
    if (!db_disk->existsFileOrDirectory(table_metadata_path))
        return static_cast<time_t>(0);

    try
    {
        return db_disk->getLastModified(table_metadata_path).epochTime();
    }
    catch (const fs::filesystem_error & e)
    {
        if (e.code() == std::errc::no_such_file_or_directory)
        {
            return static_cast<time_t>(0);
        }
        throw;
    }
}

void DatabaseOnDisk::iterateMetadataFiles(const IteratingFunction & process_metadata_file) const
{
    auto db_disk = getDisk();
    if (!db_disk->existsDirectory(metadata_path))
        return;

    auto process_tmp_drop_metadata_file = [&](const String & file_name)
    {
        assert(getUUID() == UUIDHelpers::Nil);
        static const char * tmp_drop_ext = ".sql.tmp_drop";
        const std::string object_name = file_name.substr(0, file_name.size() - strlen(tmp_drop_ext));

        if (db_disk->existsFileOrDirectory(fs::path(data_path) / object_name))
        {
            db_disk->replaceFile(getMetadataPath() + file_name, getMetadataPath() + object_name + ".sql");
            LOG_WARNING(log, "Object {} was not dropped previously and will be restored", backQuote(object_name));
            process_metadata_file(object_name + ".sql");
        }
        else
        {
            LOG_INFO(log, "Removing file {}", getMetadataPath() + file_name);
            db_disk->removeFileIfExists(getMetadataPath() + file_name);
        }
    };

    /// Metadata files to load: name and flag for .tmp_drop files
    std::vector<std::pair<String, bool>> metadata_files;

    for (const auto it = db_disk->iterateDirectory(metadata_path); it->isValid(); it->next())
    {
        auto sub_path = fs::path(it->path());
        String file_name = it->name();
        /// For '.svn', '.gitignore' directory and similar.
        if (!file_name.empty() && file_name.at(0) == '.')
            continue;

        /// There are .sql.bak files - skip them.
        if (endsWith(file_name, ".sql.bak"))
            continue;

        /// Permanently detached table flag
        if (endsWith(file_name, ".sql.detached"))
            continue;

        if (endsWith(file_name, ".sql.tmp_drop"))
        {
            /// There are files that we tried to delete previously
            metadata_files.emplace_back(file_name, false);
        }
        else if (endsWith(file_name, ".tmp_move_from") || endsWith(file_name, ".tmp_move_to"))
        {
            /// There are temp files generated in MetadataStorageFromPlainObjectStorageMoveFileOperation
            LOG_INFO(log, "Removing file {}", sub_path.string());
            db_disk->removeFileIfExists(sub_path);
        }
        else if (endsWith(file_name, ".sql.tmp"))
        {
            /// There are files .sql.tmp - delete
            LOG_INFO(log, "Removing file {}", sub_path.string());
            db_disk->removeFileIfExists(sub_path);
        }
        else if (endsWith(file_name, ".sql"))
        {
            /// The required files have names like `table_name.sql`
            metadata_files.emplace_back(file_name, true);
        }
        else
            throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Incorrect file extension: {} in metadata directory {}", file_name, getMetadataPath());
    }

    std::sort(metadata_files.begin(), metadata_files.end());
    metadata_files.erase(std::unique(metadata_files.begin(), metadata_files.end()), metadata_files.end());

    /// Read and parse metadata in parallel
    ThreadPool pool(CurrentMetrics::DatabaseOnDiskThreads, CurrentMetrics::DatabaseOnDiskThreadsActive, CurrentMetrics::DatabaseOnDiskThreadsScheduled);
    const auto batch_size = metadata_files.size() / pool.getMaxThreads() + 1;
    for (auto it = metadata_files.begin(); it < metadata_files.end(); std::advance(it, batch_size))
    {
        std::span batch{it, std::min(std::next(it, batch_size), metadata_files.end())};
        pool.scheduleOrThrow(
            [batch, &process_metadata_file, &process_tmp_drop_metadata_file]() mutable
            {
                DB::setThreadName(ThreadName::DATABASE_ON_DISK);
                for (const auto & file : batch)
                    if (file.second)
                        process_metadata_file(file.first);
                    else
                        process_tmp_drop_metadata_file(file.first);
            },
            Priority{},
            getContext()->getSettingsRef()[Setting::lock_acquire_timeout].totalMicroseconds());
    }
    pool.wait();
}

ASTPtr DatabaseOnDisk::parseQueryFromMetadata(
    LoggerPtr logger,
    ContextPtr local_context,
    DiskPtr disk,
    const String & metadata_file_path,
    bool throw_on_error /*= true*/,
    bool remove_empty /*= false*/)
{
    if (!disk->existsFile(metadata_file_path))
    {
        if (!throw_on_error)
            return nullptr;
        int ec = ErrorCodes::FILE_DOESNT_EXIST;
        if (auto disk_local = std::dynamic_pointer_cast<DiskLocal>(disk))
            ec = errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE;
        ErrnoException::throwFromPath(ec, metadata_file_path, "Cannot open file {}", metadata_file_path);
    }

    String query = readMetadataFile(disk, metadata_file_path);

    /** Empty files with metadata are generated after a rough restart of the server.
      * Remove these files to slightly reduce the work of the admins on startup.
      */
    if (remove_empty && query.empty())
    {
        if (logger)
            LOG_ERROR(logger, "File {} is empty. Removing.", metadata_file_path);

        disk->removeFileIfExists(metadata_file_path);
        return nullptr;
    }

    return parseQueryFromMetadata(logger, local_context, metadata_file_path, query, throw_on_error);
}

ASTPtr DatabaseOnDisk::parseQueryFromMetadata(
    LoggerPtr logger,
    ContextPtr local_context,
    const String & metadata_file_path,
    const String & query,
    bool throw_on_error /*= true*/)
{
    const auto & settings = local_context->getSettingsRef();
    ParserCreateQuery parser;
    const char * pos = query.data();
    std::string error_message;
    auto ast = tryParseQuery(
        parser,
        pos,
        pos + query.size(),
        error_message,
        /* hilite = */ false,
        "in file " + metadata_file_path,
        /* allow_multi_statements = */ false,
        0,
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks],
        true);

    if (!ast && throw_on_error)
        throw Exception::createDeprecated(error_message, ErrorCodes::SYNTAX_ERROR);
    if (!ast)
        return nullptr;

    auto & create = ast->as<ASTCreateQuery &>();
    if (create.table && create.uuid != UUIDHelpers::Nil)
    {
        String table_name = unescapeForFileName(fs::path(metadata_file_path).stem());

        if (create.getTable() != TABLE_WITH_UUID_NAME_PLACEHOLDER && logger)
            LOG_WARNING(
                logger,
                "File {} contains both UUID and table name. Will use name `{}` instead of `{}`",
                metadata_file_path,
                table_name,
                create.getTable());
        create.setTable(table_name);
    }

    return ast;
}

ASTPtr DatabaseOnDisk::getCreateQueryFromMetadata(const String & table_name, bool throw_on_error) const
{
    auto db_disk = getDisk();
    ASTPtr ast = parseQueryFromMetadata(log, getContext(), db_disk, getObjectMetadataPath(table_name), throw_on_error);

    if (ast)
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.attach = false;
        ast_create_query.setDatabase(getDatabaseName());
    }

    return ast;
}

ASTPtr DatabaseOnDisk::getCreateQueryFromStorage(const String & table_name, const StoragePtr & storage, bool throw_on_error) const
{
    auto metadata_ptr = storage->getInMemoryMetadataPtr();
    if (metadata_ptr == nullptr)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY, "Cannot get metadata of {}.{}",
                            backQuote(getDatabaseName()), backQuote(table_name));
        return nullptr;
    }

    /// setup create table query storage info.
    auto ast_engine = make_intrusive<ASTFunction>();
    ast_engine->name = storage->getName();
    ast_engine->setNoEmptyArgs(true);
    auto ast_storage = make_intrusive<ASTStorage>();
    ast_storage->set(ast_storage->engine, ast_engine);

    const Settings & settings = getContext()->getSettingsRef();
    auto create_table_query = DB::getCreateQueryFromStorage(
        storage,
        ast_storage,
        false,
        static_cast<unsigned>(settings[Setting::max_parser_depth]),
        static_cast<unsigned>(settings[Setting::max_parser_backtracks]),
        throw_on_error);

    create_table_query->set(create_table_query->as<ASTCreateQuery>()->comment,
                            make_intrusive<ASTLiteral>(storage->getInMemoryMetadata().comment));

    return create_table_query;
}

void DatabaseOnDisk::modifySettingsMetadata(const SettingsChanges & settings_changes, ContextPtr)
{
    auto create_query = getCreateDatabaseQuery()->clone();
    auto * create = create_query->as<ASTCreateQuery>();
    auto * settings = create->storage->settings;
    if (settings)
    {
        auto & storage_settings = settings->changes;
        for (const auto & change : settings_changes)
        {
            auto it = std::find_if(storage_settings.begin(), storage_settings.end(),
                                   [&](const auto & prev){ return prev.name == change.name; });
            if (it != storage_settings.end())
                it->value = change.value;
            else
                storage_settings.push_back(change);
        }
    }
    else
    {
        auto storage_settings = make_intrusive<ASTSetQuery>();
        storage_settings->is_standalone = false;
        storage_settings->changes = settings_changes;
        create->storage->set(create->storage->settings, storage_settings->clone());
    }

    create->attach = true;
    create->if_not_exists = false;

    WriteBufferFromOwnString statement_buf;
    IAST::FormatSettings format_settings(/*one_line=*/false);
    create->format(statement_buf, format_settings);
    writeChar('\n', statement_buf);
    String statement = statement_buf.str();

    auto metadata_file_path = DatabaseCatalog::getMetadataFilePath(TSA_SUPPRESS_WARNING_FOR_READ(database_name));   /// FIXME
    auto metadata_tmp_file_path = DatabaseCatalog::getMetadataTmpFilePath(TSA_SUPPRESS_WARNING_FOR_READ(database_name));

    auto default_db_disk = getContext()->getDatabaseDisk();
    writeMetadataFile(
        default_db_disk,
        /*file_path=*/metadata_tmp_file_path,
        /*content=*/statement,
        getContext()->getSettingsRef()[Setting::fsync_metadata]);

    default_db_disk->replaceFile(metadata_tmp_file_path, metadata_file_path);
}

void DatabaseOnDisk::checkTableNameLength(const String & table_name) const
{
    std::lock_guard lock(mutex);
    checkTableNameLengthUnlocked(database_name, table_name, getContext());
}

void DatabaseOnDisk::checkTableNameLengthUnlocked(const String & database_name_, const String & table_name, ContextPtr context_)
{
    const size_t allowed_max_length = computeMaxTableNameLength(database_name_, context_);
    const size_t escaped_name_length = escapeForFileName(table_name).length();
    if (escaped_name_length > allowed_max_length)
    {
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "The max length of table name for database {} is {}, current length is {}",
            database_name_, allowed_max_length, escaped_name_length);
    }
}

}
