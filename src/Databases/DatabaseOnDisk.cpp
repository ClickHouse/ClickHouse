#include <Databases/DatabaseOnDisk.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/escapeForFileName.h>
#include <base/logger_useful.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseAtomic.h>
#include <Common/assert_cast.h>
#include <filesystem>
#include <Common/filesystemHelpers.h>

namespace fs = std::filesystem;

namespace DB
{

static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

namespace ErrorCodes
{
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
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
}


std::pair<String, StoragePtr> createTableFromAST(
    ASTCreateQuery ast_create_query,
    const String & database_name,
    const String & table_data_path_relative,
    ContextMutablePtr context,
    bool force_restore)
{
    ast_create_query.attach = true;
    ast_create_query.setDatabase(database_name);

    if (ast_create_query.select && ast_create_query.isView())
        ApplyWithSubqueryVisitor().visit(*ast_create_query.select);

    if (ast_create_query.as_table_function)
    {
        const auto & factory = TableFunctionFactory::instance();
        auto table_function = factory.get(ast_create_query.as_table_function, context);
        ColumnsDescription columns;
        if (ast_create_query.columns_list && ast_create_query.columns_list->columns)
            columns = InterpreterCreateQuery::getColumnsDescription(*ast_create_query.columns_list->columns, context, true);
        StoragePtr storage = table_function->execute(ast_create_query.as_table_function, context, ast_create_query.getTable(), std::move(columns));
        storage->renameInMemory(ast_create_query);
        return {ast_create_query.getTable(), storage};
    }

    ColumnsDescription columns;
    ConstraintsDescription constraints;

    if (!ast_create_query.is_dictionary)
    {
        /// We do not directly use `InterpreterCreateQuery::execute`, because
        /// - the database has not been loaded yet;
        /// - the code is simpler, since the query is already brought to a suitable form.
        if (!ast_create_query.columns_list || !ast_create_query.columns_list->columns)
        {
            if (!ast_create_query.storage || !ast_create_query.storage->engine)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid storage definition in metadata file: "
                                                           "it's a bug or result of manual intervention in metadata files");

            if (!StorageFactory::instance().checkIfStorageSupportsSchemaInterface(ast_create_query.storage->engine->name))
                throw Exception("Missing definition of columns.", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);
            /// Leave columns empty.
        }
        else
        {
            columns = InterpreterCreateQuery::getColumnsDescription(*ast_create_query.columns_list->columns, context, true);
            constraints = InterpreterCreateQuery::getConstraintsDescription(ast_create_query.columns_list->constraints);
        }
    }

    return
    {
        ast_create_query.getTable(),
        StorageFactory::instance().get(
            ast_create_query,
            table_data_path_relative,
            context,
            context->getGlobalContext(),
            columns,
            constraints,
            force_restore)
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
    create->database.reset();
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
        create->setTable(TABLE_WITH_UUID_NAME_PLACEHOLDER);

    WriteBufferFromOwnString statement_buf;
    formatAST(*create, statement_buf, false);
    writeChar('\n', statement_buf);
    return statement_buf.str();
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
    fs::create_directories(local_context->getPath() + data_path);
    fs::create_directories(metadata_path);
}


void DatabaseOnDisk::createTable(
    ContextPtr local_context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    const auto & settings = local_context->getSettingsRef();
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

    String table_metadata_path = getObjectMetadataPath(table_name);

    if (create.attach_short_syntax)
    {
        /// Metadata already exists, table was detached
        removeDetachedPermanentlyFlag(local_context, table_name, table_metadata_path, true);
        attachTable(local_context, table_name, table, getTableDataPath(create));
        return;
    }

    if (!create.attach)
        checkMetadataFilenameAvailability(table_name);

    if (create.attach && fs::exists(table_metadata_path))
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
        fs::path detached_permanently_flag(table_metadata_path + detached_suffix);

        if (fs::exists(detached_permanently_flag))
            fs::remove(detached_permanently_flag);
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
    try
    {
        /// Add a table to the map of known tables.
        attachTable(query_context, query.getTable(), table, getTableDataPath(query));

        /// If it was ATTACH query and file with table metadata already exist
        /// (so, ATTACH is done after DETACH), then rename atomically replaces old file with new one.
        fs::rename(table_metadata_tmp_path, table_metadata_path);
    }
    catch (...)
    {
        fs::remove(table_metadata_tmp_path);
        throw;
    }
}

void DatabaseOnDisk::detachTablePermanently(ContextPtr query_context, const String & table_name)
{
    auto table = detachTable(query_context, table_name);

    fs::path detached_permanently_flag(getObjectMetadataPath(table_name) + detached_suffix);
    try
    {
        FS::createFile(detached_permanently_flag);
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

    StoragePtr table = detachTable(local_context, table_name);

    /// This is possible for Lazy database.
    if (!table)
        return;

    bool renamed = false;
    try
    {
        fs::rename(table_metadata_path, table_metadata_path_drop);
        renamed = true;
        table->drop();
        table->is_dropped = true;

        fs::path table_data_dir(local_context->getPath() + table_data_path_relative);
        if (fs::exists(table_data_dir))
            fs::remove_all(table_data_dir);
    }
    catch (...)
    {
        LOG_WARNING(log, fmt::runtime(getCurrentExceptionMessage(__PRETTY_FUNCTION__)));
        attachTable(local_context, table_name, table, table_data_path_relative);
        if (renamed)
            fs::rename(table_metadata_path_drop, table_metadata_path);
        throw;
    }

    fs::remove(table_metadata_path_drop);
}

void DatabaseOnDisk::checkMetadataFilenameAvailability(const String & to_table_name) const
{
    std::unique_lock lock(mutex);
    checkMetadataFilenameAvailabilityUnlocked(to_table_name, lock);
}

void DatabaseOnDisk::checkMetadataFilenameAvailabilityUnlocked(const String & to_table_name, std::unique_lock<std::mutex> &) const
{
    String table_metadata_path = getObjectMetadataPath(to_table_name);

    if (fs::exists(table_metadata_path))
    {
        fs::path detached_permanently_flag(table_metadata_path + detached_suffix);

        if (fs::exists(detached_permanently_flag))
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
    StoragePtr table = tryGetTable(table_name, local_context);
    if (dictionary && table && !table->isDictionary())
        throw Exception("Use RENAME/EXCHANGE TABLE (instead of RENAME/EXCHANGE DICTIONARY) for tables", ErrorCodes::INCORRECT_QUERY);

    detachTable(local_context, table_name);

    UUID prev_uuid = UUIDHelpers::Nil;
    try
    {
        table_lock = table->lockExclusively(
            local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);

        table_metadata_path = getObjectMetadataPath(table_name);
        attach_query = parseQueryFromMetadata(log, local_context, table_metadata_path);
        auto & create = attach_query->as<ASTCreateQuery &>();
        create.setDatabase(to_database.getDatabaseName());
        create.setTable(to_table_name);
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

    fs::remove(table_metadata_path);

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
    StoragePtr storage = tryGetTable(table_name, getContext());
    bool has_table = storage != nullptr;
    bool is_system_storage = false;
    if (has_table)
        is_system_storage = storage->isSystemStorage();
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
        else if (is_system_storage)
            ast = getCreateQueryFromStorage(table_name, storage, throw_on_error);
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
        ast_create_query.setDatabase(database_name);
    }
    if (!ast)
    {
        /// Handle databases (such as default) for which there are no database.sql files.
        /// If database.sql doesn't exist, then engine is Ordinary
        String query = "CREATE DATABASE " + backQuoteIfNeed(getDatabaseName()) + " ENGINE = Ordinary";
        ParserCreateQuery parser;
        ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings.max_parser_depth);
    }

    if (const auto database_comment = getDatabaseComment(); !database_comment.empty())
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.set(ast_create_query.comment, std::make_shared<ASTLiteral>(database_comment));
    }

    return ast;
}

void DatabaseOnDisk::drop(ContextPtr local_context)
{
    assert(tables.empty());
    if (local_context->getSettingsRef().force_remove_data_recursively_on_drop)
    {
        fs::remove_all(local_context->getPath() + getDataPath());
        fs::remove_all(getMetadataPath());
    }
    else
    {
        try
        {
            fs::remove(local_context->getPath() + getDataPath());
            fs::remove(getMetadataPath());
        }
        catch (const fs::filesystem_error & e)
        {
            if (e.code() != std::errc::directory_not_empty)
                throw Exception(Exception::CreateFromSTDTag{}, e);
            throw Exception(ErrorCodes::DATABASE_NOT_EMPTY, "Cannot drop: {}. "
                "Probably database contain some detached tables or metadata leftovers from Ordinary engine. "
                "If you want to remove all data anyway, try to attach database back and drop it again "
                "with enabled force_remove_data_recursively_on_drop setting", e.what());
        }
    }
}

String DatabaseOnDisk::getObjectMetadataPath(const String & object_name) const
{
    return getMetadataPath() + escapeForFileName(object_name) + ".sql";
}

time_t DatabaseOnDisk::getObjectMetadataModificationTime(const String & object_name) const
{
    String table_metadata_path = getObjectMetadataPath(object_name);

    if (fs::exists(table_metadata_path))
        return FS::getModificationTime(table_metadata_path);
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

        if (fs::exists(local_context->getPath() + getDataPath() + '/' + object_name))
        {
            fs::rename(getMetadataPath() + file_name, getMetadataPath() + object_name + ".sql");
            LOG_WARNING(log, "Object {} was not dropped previously and will be restored", backQuote(object_name));
            process_metadata_file(object_name + ".sql");
        }
        else
        {
            LOG_INFO(log, "Removing file {}", getMetadataPath() + file_name);
            fs::remove(getMetadataPath() + file_name);
        }
    };

    /// Metadata files to load: name and flag for .tmp_drop files
    std::set<std::pair<String, bool>> metadata_files;

    fs::directory_iterator dir_end;
    for (fs::directory_iterator dir_it(getMetadataPath()); dir_it != dir_end; ++dir_it)
    {
        String file_name = dir_it->path().filename();
        /// For '.svn', '.gitignore' directory and similar.
        if (file_name.at(0) == '.')
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
            metadata_files.emplace(file_name, false);
        }
        else if (endsWith(file_name, ".sql.tmp"))
        {
            /// There are files .sql.tmp - delete
            LOG_INFO(log, "Removing file {}", dir_it->path().string());
            fs::remove(dir_it->path());
        }
        else if (endsWith(file_name, ".sql"))
        {
            /// The required files have names like `table_name.sql`
            metadata_files.emplace(file_name, true);
        }
        else
            throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Incorrect file extension: {} in metadata directory {}", file_name, getMetadataPath());
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

    int metadata_file_fd = ::open(metadata_file_path.c_str(), O_RDONLY | O_CLOEXEC);

    if (metadata_file_fd == -1)
    {
        if (errno == ENOENT && !throw_on_error)
            return nullptr;

        throwFromErrnoWithPath("Cannot open file " + metadata_file_path, metadata_file_path,
                               errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
    }

    ReadBufferFromFile in(metadata_file_fd, metadata_file_path, METADATA_FILE_BUFFER_SIZE);
    readStringUntilEOF(query, in);

    /** Empty files with metadata are generated after a rough restart of the server.
      * Remove these files to slightly reduce the work of the admins on startup.
      */
    if (remove_empty && query.empty())
    {
        if (logger)
            LOG_ERROR(logger, "File {} is empty. Removing.", metadata_file_path);
        fs::remove(metadata_file_path);
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

ASTPtr DatabaseOnDisk::getCreateQueryFromMetadata(const String & database_metadata_path, bool throw_on_error) const
{
    ASTPtr ast = parseQueryFromMetadata(log, getContext(), database_metadata_path, throw_on_error);

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
            throw Exception(ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY, "Cannot get metadata of {}.{}", backQuote(getDatabaseName()), backQuote(table_name));
        else
            return nullptr;
    }

    /// setup create table query storage info.
    auto ast_engine = std::make_shared<ASTFunction>();
    ast_engine->name = storage->getName();
    auto ast_storage = std::make_shared<ASTStorage>();
    ast_storage->set(ast_storage->engine, ast_engine);

    auto create_table_query = DB::getCreateQueryFromStorage(storage, ast_storage, false,
                                                            getContext()->getSettingsRef().max_parser_depth, throw_on_error);

    create_table_query->set(create_table_query->as<ASTCreateQuery>()->comment,
                            std::make_shared<ASTLiteral>("SYSTEM TABLE is built on the fly."));

    return create_table_query;
}

void DatabaseOnDisk::modifySettingsMetadata(const SettingsChanges & settings_changes, ContextPtr query_context)
{
    std::lock_guard lock(modify_settings_mutex);

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
        auto storage_settings = std::make_shared<ASTSetQuery>();
        storage_settings->is_standalone = false;
        storage_settings->changes = settings_changes;
        create->storage->set(create->storage->settings, storage_settings->clone());
    }

    create->attach = true;
    create->if_not_exists = false;

    WriteBufferFromOwnString statement_buf;
    formatAST(*create, statement_buf, false);
    writeChar('\n', statement_buf);
    String statement = statement_buf.str();

    String database_name_escaped = escapeForFileName(database_name);
    fs::path metadata_root_path = fs::canonical(query_context->getGlobalContext()->getPath());
    fs::path metadata_file_tmp_path = fs::path(metadata_root_path) / "metadata" / (database_name_escaped + ".sql.tmp");
    fs::path metadata_file_path = fs::path(metadata_root_path) / "metadata" / (database_name_escaped + ".sql");

    WriteBufferFromFile out(metadata_file_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
    writeString(statement, out);

    out.next();
    if (getContext()->getSettingsRef().fsync_metadata)
        out.sync();
    out.close();

    fs::rename(metadata_file_tmp_path, metadata_file_path);
}
}
