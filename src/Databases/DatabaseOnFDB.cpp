#include <cassert>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <vector>
#include <Access/MemoryAccessStorage.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseOnFDB.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DDLTask.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IParser.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDictionary.h>
#include <Parsers/ParserDictionaryAttributeDeclaration.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageMaterializedView.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/FoundationDB/ProtobufTypeHelpers.h>
#include <Common/escapeForFileName.h>
#include <Common/filesystemHelpers.h>
#include <Common/renameat2.h>
#include <Common/FoundationDB/protos/MetadataDatabase.pb.h>
#include "base/logger_useful.h"
#include <base/UUID.h>

namespace Proto = DB::FoundationDB::Proto;

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DATABASE;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int CANNOT_ASSIGN_ALTER;
    extern const int INCORRECT_FILE_NAME;
    extern const int DATABASE_NOT_EMPTY;
    extern const int NOT_IMPLEMENTED;
    extern const int FILE_ALREADY_EXISTS;
    extern const int INCORRECT_QUERY;
    extern const int ABORTED;
    extern const int FDB_META_EXCEPTION;
}

namespace
{
    void tryAttachTable(
        ContextMutablePtr context,
        const ASTCreateQuery & query,
        DatabaseOnFDB & database,
        const String & database_name,
        const String & metadata_path,
        bool force_restore)
    {
        try
        {
            auto [table_name, table] = createTableFromAST(query, database_name, database.getTableDataPath(query), context, force_restore);

            database.attachTable(context, table_name, table, database.getTableDataPath(query));
        }
        catch (Exception & e)
        {
            e.addMessage(
                "Cannot attach table " + backQuote(database_name) + "." + backQuote(query.getTable()) + " from metadata file "
                + metadata_path + " from query " + serializeAST(query));
            throw;
        }
    }
}

class OnFDBDatabaseTablesSnapshotIterator final : public DatabaseTablesSnapshotIterator
{
public:
    explicit OnFDBDatabaseTablesSnapshotIterator(DatabaseTablesSnapshotIterator && base) : DatabaseTablesSnapshotIterator(std::move(base))
    {
    }
    UUID uuid() const override { return table()->getStorageID().uuid; }
};

DatabaseOnFDB::DatabaseOnFDB(String name_, UUID uuid, const String & logger_name, ContextPtr context_)
    : DatabaseWithOwnTablesBase(name_, logger_name, context_)
    , data_path("store/")
    , db_uuid(uuid)
    , meta_store(context_->getMetadataStoreFoundationDB())
{
    assert(db_uuid != UUIDHelpers::Nil);
}
DatabaseOnFDB::DatabaseOnFDB(String name_, UUID uuid, ContextPtr context_)
    : DatabaseOnFDB(name_, uuid, "DatabaseOnFDB (" + name_ + ")", context_)
{
}

ASTPtr DatabaseOnFDB::getCreateDatabaseQuery() const
{
    auto create = std::make_unique<ASTCreateQuery>();
    create->setDatabase(database_name);
    create->uuid = db_uuid;

    auto storage = std::make_shared<ASTStorage>();
    create->set(create->storage, storage);

    auto engine = makeASTFunction("OnFDB");
    engine->no_empty_args = true;
    storage->set(storage->engine, engine);

    if (!comment.empty())
        create->set(create->comment, std::make_shared<ASTLiteral>(comment));

    create->attach = false;

    return create;
}

/// It returns create table statement (even if table is detached)
ASTPtr DatabaseOnFDB::getCreateTableQueryImpl(const String & table_name, ContextPtr, bool throw_on_error) const
{
    ASTPtr ast;
    StoragePtr storage = tryGetTable(table_name, getContext());
    bool has_table = storage != nullptr;
    bool is_system_storage = false;
    if (has_table)
        is_system_storage = storage->isSystemStorage();
    try
    {
        ast = getQueryFromTableMeta(getContext(), *meta_store->getTableMeta(TableKey{db_uuid, table_name}), throw_on_error);
        if (ast)
        {
            auto & ast_create_query = ast->as<ASTCreateQuery &>();
            ast_create_query.attach = false;
            ast_create_query.setDatabase(database_name);
        }
    }
    catch (const Exception & e)
    {
        if (!has_table && e.code() == ErrorCodes::FDB_META_EXCEPTION && throw_on_error)
            throw Exception{"Table " + backQuote(table_name) + " doesn't exist", ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY};
        else if (is_system_storage)
            ast = getCreateQueryFromStorage(table_name, storage, throw_on_error);
        else if (throw_on_error)
            throw;
    }
    return ast;
}

ASTPtr DatabaseOnFDB::getCreateQueryFromStorage(const String & table_name, const StoragePtr & storage, bool throw_on_error) const
{
    auto metadata_ptr = storage->getInMemoryMetadataPtr();
    if (metadata_ptr == nullptr)
    {
        if (throw_on_error)
            throw Exception(
                ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY,
                "Cannot get metadata of {}.{}",
                backQuote(getDatabaseName()),
                backQuote(table_name));
        else
            return nullptr;
    }

    /// setup create table query storage info.
    auto ast_engine = std::make_shared<ASTFunction>();
    ast_engine->name = storage->getName();
    auto ast_storage = std::make_shared<ASTStorage>();
    ast_storage->set(ast_storage->engine, ast_engine);

    auto create_table_query
        = DB::getCreateQueryFromStorage(storage, ast_storage, false, getContext()->getSettingsRef().max_parser_depth, throw_on_error);

    create_table_query->set(
        create_table_query->as<ASTCreateQuery>()->comment, std::make_shared<ASTLiteral>("SYSTEM TABLE is built on the fly."));

    return create_table_query;
}

void DatabaseOnFDB::checkMetadataFilenameAvailability(const String & to_table_name) const
{
    std::unique_lock lock(mutex);
    checkMetadataFilenameAvailabilityUnlocked(to_table_name, lock);
}

void DatabaseOnFDB::checkMetadataFilenameAvailabilityUnlocked(const String & to_table_name, std::unique_lock<std::mutex> &) const
{
    if (meta_store->isExistsTable(TableKey{db_uuid, to_table_name}))
    {
        if (meta_store->isDetached(TableKey{db_uuid, to_table_name}))
            throw Exception(
                ErrorCodes::TABLE_ALREADY_EXISTS,
                "Table {}.{} already exists (detached permanently)",
                backQuote(database_name),
                backQuote(to_table_name));
        else
            throw Exception(
                ErrorCodes::TABLE_ALREADY_EXISTS,
                "Table {}.{} already exists (detached)",
                backQuote(database_name),
                backQuote(to_table_name));
    }
}

time_t DatabaseOnFDB::getObjectMetadataModificationTime(const String & object_name) const
{
    if (meta_store->isExistsTable(TableKey{db_uuid, object_name}))
        return meta_store->getModificationTime(TableKey{db_uuid, object_name});
    else
        return static_cast<time_t>(0);
}

String DatabaseOnFDB::getTableDataPath(const ASTCreateQuery & query) const
{
    auto tmp = data_path + DatabaseCatalog::getPathForUUID(query.uuid);
    assert(tmp != data_path && !tmp.empty());
    return tmp;
}

void DatabaseOnFDB::createTable(ContextPtr local_context, const String & table_name, const StoragePtr & table, const ASTPtr & query)
{
    ASTPtr query_clone = query->clone();
    auto * create_clone = query_clone->as<ASTCreateQuery>();
    if (!create_clone)
    {
        WriteBufferFromOwnString query_buf;
        formatAST(*query, query_buf, true);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query '{}' is not CREATE query", query_buf.str());
    }

    if (!create_clone->is_dictionary)
        create_clone->attach = true;

    /// We remove everything that is not needed for ATTACH from the query.
    assert(!create_clone->temporary);
    create_clone->as_database.clear();
    create_clone->as_table.clear();
    create_clone->if_not_exists = false;
    create_clone->is_populate = false;
    create_clone->replace_view = false;
    create_clone->replace_table = false;
    create_clone->create_or_replace = false;

    /// For views it is necessary to save the SELECT query itself, for the rest - on the contrary
    if (!create_clone->isView())
        create_clone->select = nullptr;

    WriteBufferFromOwnString statement_buf;
    formatAST(*create_clone, statement_buf, false);
    writeChar('\n', statement_buf);
    const auto & create = create_clone->as<ASTCreateQuery &>();

    if (isTableExist(table_name, getContext()))
        throw Exception(
            ErrorCodes::TABLE_ALREADY_EXISTS, "Table {}.{} already exists", backQuote(getDatabaseName()), backQuote(table_name));


    if (create.attach_short_syntax)
    {
        /// Metadata already exists, table was detached
        removeDetachedPermanentlyFlag(table_name, database_name);
        attachTable(local_context, table_name, table, getTableDataPath(create));
        return;
    }
    if (!create.attach)
        checkMetadataFilenameAvailability(table_name);

    if (create.attach && meta_store->isExistsTable(TableKey{db_uuid, table_name}))
    {
        try
        {
            auto table_meta = meta_store->getTableMeta(TableKey{db_uuid, table_name});
            ASTPtr ast_detached = getQueryFromTableMeta(local_context, *table_meta);
            auto & create_detached = ast_detached->as<ASTCreateQuery &>();
            // either both should be Nil, either values should be equal
            if (create.uuid != create_detached.uuid)
                throw Exception(
                    ErrorCodes::TABLE_ALREADY_EXISTS,
                    "Table {}.{} already exist (detached permanently). To attach it back "
                    "you need to use short ATTACH syntax or a full statement with the same UUID",
                    backQuote(database_name),
                    backQuote(table_name));
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::FDB_META_EXCEPTION)
                throw Exception{"Table " + backQuote(table_name) + " doesn't exist", ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY};
        }
    }

    auto tb_meta = getTableMetaFromQuery(create);
    DetachedTables not_in_use;
    auto table_data_path = getTableDataPath(create);
    bool locked_uuid = false;
    try
    {
        std::unique_lock lock{mutex};
        if (create.getDatabase() != database_name)
            throw Exception(
                ErrorCodes::UNKNOWN_DATABASE,
                "Database was renamed to `{}`, cannot create table in `{}`",
                database_name,
                create.getDatabase());
        not_in_use = cleanupDetachedTables();
        assertDetachedTableNotInUse(create.uuid);
        /// We will get en exception if some table with the same UUID exists (even if it's detached table or table from another database)
        DatabaseCatalog::instance().addUUIDMapping(create.uuid);
        locked_uuid = true;

        attachTableUnlocked(table_name, table, lock);
        meta_store->addTableMeta(*tb_meta, TableKey{db_uuid, table_name});
    }
    catch (...)
    {
        meta_store->removeTableMeta(TableKey{db_uuid, table_name});
        if (locked_uuid)
            DatabaseCatalog::instance().removeUUIDMappingFinally(create.uuid);
        throw;
    }
}

void DatabaseOnFDB::dropTable(ContextPtr local_context, const String & table_name, bool no_delay)
{
    auto storage = tryGetTable(table_name, local_context);
    /// Remove the inner table (if any) to avoid deadlock
    /// (due to attempt to execute DROP from the worker thread)
    if (storage)
        storage->dropInnerTableIfAny(no_delay, local_context);
    StoragePtr table;
    std::string dropped_meta_path;
    {
        std::unique_lock lock(mutex);
        table = getTableUnlocked(table_name, lock);
        dropped_meta_path = meta_store->renameTableToDropped(TableKey{db_uuid, table_name}, storage->getStorageID().uuid);
        detachTableUnlocked(table_name, lock); /// Should never throw
    }
    DatabaseCatalog::instance().enqueueDroppedTableCleanup(table->getStorageID(), table, dropped_meta_path, no_delay);
}

void DatabaseOnFDB::renameDatabase(ContextPtr query_context, const String & new_name)
{
    /// CREATE, ATTACH, DROP, DETACH and RENAME DATABASE must hold DDLGuard

    if (query_context->getSettingsRef().check_table_dependencies)
    {
        std::lock_guard lock(mutex);
        for (auto & table : tables)
            DatabaseCatalog::instance().checkTableCanBeRemovedOrRenamed({database_name, table.first});
    }

    meta_store->renameDatabase(getDatabaseName(), new_name);

    {
        std::lock_guard lock(mutex);
        {
            Strings table_names;
            table_names.reserve(tables.size());
            for (auto & table : tables)
                table_names.push_back(table.first);
            DatabaseCatalog::instance().updateDatabaseName(database_name, new_name, table_names);
        }
        database_name = new_name;

        for (auto & table : tables)
        {
            auto table_id = table.second->getStorageID();
            table_id.database_name = database_name;
            table.second->renameInMemory(table_id);
        }
    }
}


void DatabaseOnFDB::renameTable(
    ContextPtr local_context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name,
    bool exchange,
    bool dictionary)
{
    if (!typeid_cast<DatabaseOnFDB *>(&to_database))
        throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);
    if (exchange && !supportsRenameat2())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "RENAME EXCHANGE is not supported");

    auto & other_db = dynamic_cast<DatabaseOnFDB &>(to_database);
    bool inside_database = this == &other_db;

    if (inside_database && table_name == to_table_name)
        return;

    ///lock
    std::unique_lock<std::mutex> db_lock;
    std::unique_lock<std::mutex> other_db_lock;
    if (inside_database)
        db_lock = std::unique_lock{mutex};
    else if (this < &other_db)
    {
        db_lock = std::unique_lock{mutex};
        other_db_lock = std::unique_lock{other_db.mutex};
    }
    else
    {
        other_db_lock = std::unique_lock{other_db.mutex};
        db_lock = std::unique_lock{mutex};
    }

    if (!exchange)
        other_db.checkMetadataFilenameAvailabilityUnlocked(to_table_name, inside_database ? db_lock : other_db_lock);

    StoragePtr table = getTableUnlocked(table_name, db_lock);

    if (dictionary && !table->isDictionary())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Use RENAME/EXCHANGE TABLE (instead of RENAME/EXCHANGE DICTIONARY) for tables");
    /// anonymous func
    auto detach = [](DatabaseOnFDB & db, const String & table_name_) { db.tables.erase(table_name_); };

    auto attach = [](DatabaseOnFDB & db, const String & table_name_, const StoragePtr & table_) { db.tables.emplace(table_name_, table_); };

    auto assert_can_move_mat_view = [inside_database](const StoragePtr & table_) {
        if (inside_database)
            return;
        if (const auto * mv = dynamic_cast<const StorageMaterializedView *>(table_.get()))
            if (mv->hasInnerTable())
                throw Exception("Cannot move MaterializedView with inner table to other database", ErrorCodes::NOT_IMPLEMENTED);
    };
    table->checkTableCanBeRenamed();
    assert_can_move_mat_view(table);
    StoragePtr other_table;
    /// before check
    if (exchange)
    {
        other_table = other_db.getTableUnlocked(to_table_name, other_db_lock);
        if (dictionary && !other_table->isDictionary())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Use RENAME/EXCHANGE TABLE (instead of RENAME/EXCHANGE DICTIONARY) for tables");
        other_table->checkTableCanBeRenamed();
        assert_can_move_mat_view(other_table);
    }

    /// Metadata
    if (exchange)
    {
        try
        {
            auto table_meta = meta_store->getTableMeta(TableKey{db_uuid, table_name});
            auto other_table_meta = meta_store->getTableMeta(TableKey{other_db.db_uuid, to_table_name});
            auto ast = getQueryFromTableMeta(local_context, *table_meta);
            auto other_ast = getQueryFromTableMeta(local_context, *other_table_meta);
            auto & ast_create_query = ast->as<ASTCreateQuery &>();
            ast_create_query.setDatabase(other_db.database_name);
            ast_create_query.setTable(to_table_name);
            auto & other_ast_create_query = other_ast->as<ASTCreateQuery &>();
            other_ast_create_query.setDatabase(database_name);
            other_ast_create_query.setTable(table_name);
            auto new_table_meta = getTableMetaFromQuery(ast_create_query);
            auto new_other_table_meta = getTableMetaFromQuery(other_ast_create_query);
            meta_store->exchangeTableMeta(
                TableKey{db_uuid, table_name}, TableKey{other_db.db_uuid, to_table_name}, *new_table_meta, *new_other_table_meta);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::FDB_META_EXCEPTION)
                throw Exception{"Table " + backQuote(table_name) + " doesn't exist", ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY};
        }
    }
    else
    {
        try
        {
            auto table_meta = meta_store->getTableMeta(TableKey{db_uuid, table_name});
            auto ast = getQueryFromTableMeta(local_context, *table_meta);
            auto & ast_create_query = ast->as<ASTCreateQuery &>();
            ast_create_query.setDatabase(other_db.database_name);
            ast_create_query.setTable(to_table_name);
            auto new_table_meta = getTableMetaFromQuery(ast_create_query);
            meta_store->renameTable(TableKey{db_uuid, table_name}, TableKey{other_db.db_uuid, to_table_name}, *new_table_meta);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::FDB_META_EXCEPTION)
                throw Exception{"Table " + backQuote(table_name) + " doesn't exist", ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY};
        }
    }

    /// After metadata was successfully moved, the following methods should not throw (if them do, it's a logical error)

    /// In memory
    detach(*this, table_name);
    if (exchange)
        detach(other_db, to_table_name);
    auto old_table_id = table->getStorageID();

    table->renameInMemory({other_db.database_name, to_table_name, old_table_id.uuid});
    if (exchange)
        other_table->renameInMemory({database_name, table_name, other_table->getStorageID().uuid});

    if (!inside_database)
    {
        DatabaseCatalog::instance().updateUUIDMapping(old_table_id.uuid, other_db.shared_from_this(), table);
        if (exchange)
            DatabaseCatalog::instance().updateUUIDMapping(other_table->getStorageID().uuid, shared_from_this(), other_table);
    }

    attach(other_db, to_table_name, table);
    if (exchange)
        attach(*this, table_name, other_table);
}

void DatabaseOnFDB::alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    std::string table_name = table_id.table_name;
    try
    {
        ASTPtr ast = getCreateTableQuery(table_name, local_context);
        applyMetadataChangesToCreateQuery(ast, metadata);
        auto new_table_meta = getTableMetaFromQuery(ast->as<ASTCreateQuery &>());
        meta_store->updateTableMeta(*new_table_meta, TableKey{db_uuid, table_name});
        TableNamesSet new_dependencies
            = getDependenciesSetFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), ast);
        DatabaseCatalog::instance().updateLoadingDependencies(table_id, std::move(new_dependencies));
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::FDB_META_EXCEPTION)
            throw Exception{"Table " + backQuote(table_name) + " doesn't exist", ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY};
    }
}

void DatabaseOnFDB::attachTable(
    ContextPtr /* context_ */, const String & name, const StoragePtr & table, const String & relative_table_path)
{
    if (relative_table_path == data_path || relative_table_path.empty())
    {
        LOG_INFO(log, "Try to attach table on disk path {}.", backQuote(relative_table_path));
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Migrate metadate from local to FoundationDB error");
    }
    DetachedTables not_in_use;
    std::unique_lock lock(mutex);
    not_in_use = cleanupDetachedTables();
    auto table_id = table->getStorageID();
    assertDetachedTableNotInUse(table_id.uuid);
    attachTableUnlocked(name, table, lock);
}

StoragePtr DatabaseOnFDB::detachTable(ContextPtr /* context */, const String & name)
{
    DetachedTables not_in_use;
    std::unique_lock lock(mutex);
    auto table = detachTableUnlocked(name, lock);
    detached_tables.emplace(table->getStorageID().uuid, table);
    not_in_use = cleanupDetachedTables(); //-V1001
    return table;
}

DatabaseOnFDB::DetachedTables DatabaseOnFDB::cleanupDetachedTables()
{
    DetachedTables not_in_use;
    auto it = detached_tables.begin();
    while (it != detached_tables.end())
    {
        if (it->second.unique())
        {
            not_in_use.emplace(it->first, it->second);
            it = detached_tables.erase(it);
        }
        else
            ++it;
    }
    /// It should be destroyed in caller with released database mutex
    return not_in_use;
}

void DatabaseOnFDB::startupTables(ThreadPool & thread_pool, bool /*force_restore*/, bool /*force_attach*/)
{
    LOG_INFO(log, "Starting up tables.");

    const size_t total_tables = tables.size();
    if (!total_tables)
        return;

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed{0};

    auto startup_one_table = [&](const StoragePtr & table) {
        table->startup();
        logAboutProgress(log, ++tables_processed, total_tables, watch);
    };


    try
    {
        for (const auto & table : tables)
            thread_pool.scheduleOrThrowOnError([&]() { startup_one_table(table.second); });
    }
    catch (...)
    {
        /// We have to wait for jobs to finish here, because job function has reference to variables on the stack of current thread.
        thread_pool.wait();
        throw;
    }
    thread_pool.wait();
}

void DatabaseOnFDB::loadStoredObjects(ContextMutablePtr local_context, bool force_restore, bool force_attach, bool skip_startup_tables)
{
    /** Tables load faster if they are loaded in sorted (by name) order.
      * Otherwise (for the ext4 filesystem), `DirectoryIterator` iterates through them in some order,
      *  which does not correspond to order tables creation and does not correspond to order of their location on disk.
      */

    ParsedTablesMetadata metadata;
    loadTablesMetadata(local_context, metadata);

    size_t total_tables = metadata.parsed_tables.size() - metadata.total_dictionaries;

    AtomicStopwatch watch;
    std::atomic<size_t> dictionaries_processed{0};
    std::atomic<size_t> tables_processed{0};

    ThreadPool pool;

    /// We must attach dictionaries before attaching tables
    /// because while we're attaching tables we may need to have some dictionaries attached
    /// (for example, dictionaries can be used in the default expressions for some tables).
    /// On the other hand we can attach any dictionary (even sourced from ClickHouse table)
    /// without having any tables attached. It is so because attaching of a dictionary means
    /// loading of its config only, it doesn't involve loading the dictionary itself.

    /// Attach dictionaries.
    for (const auto & name_with_path_and_query : metadata.parsed_tables)
    {
        const auto & name = name_with_path_and_query.first;
        const auto & path = name_with_path_and_query.second.path;
        const auto & ast = name_with_path_and_query.second.ast;
        const auto & create_query = ast->as<const ASTCreateQuery &>();

        if (create_query.is_dictionary)
        {
            pool.scheduleOrThrowOnError([&]() {
                loadTableFromMetadata(local_context, path, name, ast, force_restore);

                /// Messages, so that it's not boring to wait for the server to load for a long time.
                logAboutProgress(log, ++dictionaries_processed, metadata.total_dictionaries, watch);
            });
        }
    }

    pool.wait();

    /// Attach tables.
    for (const auto & name_with_path_and_query : metadata.parsed_tables)
    {
        const auto & name = name_with_path_and_query.first;
        const auto & path = name_with_path_and_query.second.path;
        const auto & ast = name_with_path_and_query.second.ast;
        const auto & create_query = ast->as<const ASTCreateQuery &>();

        if (!create_query.is_dictionary)
        {
            pool.scheduleOrThrowOnError([&]() {
                loadTableFromMetadata(local_context, path, name, ast, force_restore);

                /// Messages, so that it's not boring to wait for the server to load for a long time.
                logAboutProgress(log, ++tables_processed, total_tables, watch);
            });
        }
    }

    pool.wait();

    if (!skip_startup_tables)
    {
        /// After all tables was basically initialized, startup them.
        startupTables(pool, force_restore, force_attach);
    }
}

void DatabaseOnFDB::loadTableFromMetadata(
    ContextMutablePtr local_context, const String & file_path, const QualifiedTableName & name, const ASTPtr & ast, bool force_restore)
{
    assert(name.database == database_name);
    const auto & create_query = ast->as<const ASTCreateQuery &>();

    tryAttachTable(local_context, create_query, *this, name.database, file_path, force_restore);
}

void DatabaseOnFDB::loadTablesMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata)
{
    size_t prev_tables_count = metadata.parsed_tables.size();
    size_t prev_total_dictionaries = metadata.total_dictionaries;

    auto process_metadata = [&metadata, this](const String & file_name) {
        fs::path path(getMetadataPath());
        fs::path file_path(file_name);
        fs::path full_path = path / file_path;

        try
        {
            auto ast = DatabaseOnDisk::parseQueryFromMetadata(
                log, getContext(), full_path.string(), /*throw_on_error*/ true, /*remove_empty*/ false);
            if (ast)
            {
                auto * create_query = ast->as<ASTCreateQuery>();
                create_query->setDatabase(database_name);
                /// Add table meta to FDB.
                auto table_meta = getTableMetaFromQuery(*create_query);
                meta_store->addTableMeta(*table_meta, TableKey{db_uuid, unescapeForFileName(file_name.substr(0, file_name.size() - 4))});
                if (fs::exists(full_path.string() + ".detached"))
                {
                    /// FIXME: even if we don't load the table we can still mark the uuid of it as taken.
                    /// if (create_query->uuid != UUIDHelpers::Nil)
                    ///     DatabaseCatalog::instance().addUUIDMapping(create_query->uuid);

                    const std::string table_name = unescapeForFileName(file_name.substr(0, file_name.size() - 4));
                    LOG_DEBUG(log, "Skipping permanently detached table {}.", backQuote(table_name));
                    try
                    {
                        meta_store->updateDetachTableStatus(TableKey{db_uuid, table_name}, true);
                    }
                    catch (Exception & e)
                    {
                        e.addMessage(
                            "Loading detached table {}.{} from loacl. Setting permanently detached flag on fdb. ",
                            backQuote(getDatabaseName()),
                            backQuote(table_name));
                        throw;
                    }

                    return;
                }

                QualifiedTableName qualified_name{database_name, create_query->getTable()};
                TableNamesSet loading_dependencies = getDependenciesSetFromCreateQuery(getContext(), qualified_name, ast);

                std::lock_guard lock{metadata.mutex};
                metadata.parsed_tables[qualified_name] = ParsedTableMetadata{full_path.string(), ast};
                if (loading_dependencies.empty())
                {
                    metadata.independent_database_objects.emplace_back(std::move(qualified_name));
                }
                else
                {
                    for (const auto & dependency : loading_dependencies)
                        metadata.dependencies_info[dependency].dependent_database_objects.insert(qualified_name);
                    assert(metadata.dependencies_info[qualified_name].dependencies.empty());
                    metadata.dependencies_info[qualified_name].dependencies = std::move(loading_dependencies);
                }
                metadata.total_dictionaries += create_query->is_dictionary;
            }
        }
        catch (Exception & e)
        {
            e.addMessage("Cannot parse definition from metadata file " + full_path.string());
            throw;
        }
    };

    auto process_metadata_from_fdb = [&metadata, this](const MetadataTable & table_meta) {
        try
        {
            auto ast = getQueryFromTableMeta(getContext(), table_meta);
            if (ast)
            {
                auto * create_query = ast->as<ASTCreateQuery>();
                QualifiedTableName qualified_name{database_name, create_query->getTable()};
                TableNamesSet loading_dependencies = getDependenciesSetFromCreateQuery(getContext(), qualified_name, ast);

                std::lock_guard lock{metadata.mutex};
                metadata.parsed_tables[qualified_name] = ParsedTableMetadata{toString(create_query->uuid), ast};
                if (loading_dependencies.empty())
                {
                    metadata.independent_database_objects.emplace_back(std::move(qualified_name));
                }
                else
                {
                    for (const auto & dependency : loading_dependencies)
                        metadata.dependencies_info[dependency].dependent_database_objects.insert(qualified_name);
                    assert(metadata.dependencies_info[qualified_name].dependencies.empty());
                    metadata.dependencies_info[qualified_name].dependencies = std::move(loading_dependencies);
                }
                metadata.total_dictionaries += create_query->is_dictionary;
            }
        }
        catch (Exception & e)
        {
            e.addMessage("Cannot parse definition from metadata  " + table_meta.sql());
            throw;
        }
    };
    if (!fs::exists(getMetadataPath()) || !meta_store->isFirstBoot())
        iterateMetadataFromFDB(process_metadata_from_fdb);
    else
        iterateMetadataFiles(local_context, process_metadata);

    size_t objects_in_database = metadata.parsed_tables.size() - prev_tables_count;
    size_t dictionaries_in_database = metadata.total_dictionaries - prev_total_dictionaries;
    size_t tables_in_database = objects_in_database - dictionaries_in_database;

    LOG_INFO(
        log,
        "Metadata processed, database {} has {} tables and {} dictionaries in total.",
        database_name,
        tables_in_database,
        dictionaries_in_database);
}

void DatabaseOnFDB::drop(ContextPtr /*context*/)
{
    assert(tables.empty());
    try
    {
        meta_store->removeDatabaseMeta(getDatabaseName());
    }
    catch (Exception & e)
    {
        e.addMessage("If you want to drop database anyway, try to attach database back and drop it again", database_name);
        throw;
    }
}

DatabaseTablesIteratorPtr
DatabaseOnFDB::getTablesIterator(ContextPtr local_context, const IDatabase::FilterByNameFunction & filter_by_table_name) const
{
    auto base_iter = DatabaseWithOwnTablesBase::getTablesIterator(local_context, filter_by_table_name);
    return std::make_unique<OnFDBDatabaseTablesSnapshotIterator>(std::move(typeid_cast<DatabaseTablesSnapshotIterator &>(*base_iter)));
}

void DatabaseOnFDB::iterateMetadataFiles(ContextPtr local_context, const IteratingFunction & process_metadata_file) const
{
    auto process_tmp_drop_metadata_file = [&](const String & file_name) {
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
            throw Exception(
                ErrorCodes::INCORRECT_FILE_NAME, "Incorrect file extension: {} in metadata directory {}", file_name, getMetadataPath());
    }

    /// Read and parse metadata in parallel
    ThreadPool pool;
    for (const auto & file : metadata_files)
    {
        pool.scheduleOrThrowOnError([&]() {
            if (file.second)
                process_metadata_file(file.first);
            else
                process_tmp_drop_metadata_file(file.first);
        });
    }
    pool.wait();
}

void DatabaseOnFDB::iterateMetadataFromFDB(const IteratingFDBFunction & process_metadata_from_fdb) const
{
    auto table_metas = meta_store->listAllTableMeta(db_uuid);
    for (const auto & table_meta : table_metas)
    {
        process_metadata_from_fdb(*table_meta);
    }
}

void DatabaseOnFDB::detachTablePermanently(ContextPtr query_context, const String & table_name)
{
    auto table = detachTable(query_context, table_name);

    try
    {
        meta_store->updateDetachTableStatus(TableKey{db_uuid, table_name}, true);
    }
    catch (Exception & e)
    {
        e.addMessage(
            "while trying to set permanently detached flag. Table {}.{} may be reattached during server restart.",
            backQuote(getDatabaseName()),
            backQuote(table_name));
        throw;
    }
}

void DatabaseOnFDB::assertCanBeDetached(bool cleanup)
{
    if (cleanup)
    {
        DetachedTables not_in_use;
        {
            std::lock_guard lock(mutex);
            not_in_use = cleanupDetachedTables();
        }
    }
    std::lock_guard lock(mutex);
    if (!detached_tables.empty())
        throw Exception(
            "Database " + backQuoteIfNeed(database_name)
                + " cannot be detached, "
                  "because some tables are still in use. Retry later.",
            ErrorCodes::DATABASE_NOT_EMPTY);
}
UUID DatabaseOnFDB::tryGetTableUUID(const String & table_name) const
{
    if (auto table = tryGetTable(table_name, getContext()))
        return table->getStorageID().uuid;
    return UUIDHelpers::Nil;
}

void DatabaseOnFDB::waitDetachedTableNotInUse(const UUID & uuid)
{
    /// Table is in use while its shared_ptr counter is greater than 1.
    /// We cannot trigger condvar on shared_ptr destruction, so it's busy wait.
    while (true)
    {
        DetachedTables not_in_use;
        {
            std::lock_guard lock{mutex};
            not_in_use = cleanupDetachedTables();
            if (detached_tables.count(uuid) == 0)
                return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}
void DatabaseOnFDB::checkDetachedTableNotInUse(const UUID & uuid)
{
    DetachedTables not_in_use;
    std::lock_guard lock{mutex};
    not_in_use = cleanupDetachedTables();
    assertDetachedTableNotInUse(uuid);
}
void DatabaseOnFDB::setDetachedTableNotInUseForce(const UUID & uuid)
{
    std::unique_lock lock{mutex};
    detached_tables.erase(uuid);
}
void DatabaseOnFDB::assertDetachedTableNotInUse(const UUID & uuid)
{
    /// Without this check the following race is possible since table RWLocks are not used:
    /// 1. INSERT INTO table ...;
    /// 2. DETACH TABLE table; (INSERT still in progress, it holds StoragePtr)
    /// 3. ATTACH TABLE table; (new instance of Storage with the same UUID is created, instances share data on disk)
    /// 4. INSERT INTO table ...; (both Storage instances writes data without any synchronization)
    /// To avoid it, we remember UUIDs of detached tables and does not allow ATTACH table with such UUID until detached instance still in use.
    if (detached_tables.count(uuid))
        throw Exception(
            ErrorCodes::TABLE_ALREADY_EXISTS,
            "Cannot attach table with UUID {}, "
            "because it was detached but still used by some query. Retry later.",
            toString(uuid));
}
void DatabaseOnFDB::removeDetachedPermanentlyFlag(const String & table_name, const String & db_name) const
{
    try
    {
        if (meta_store->isDetached(TableKey{db_uuid, table_name}))
            meta_store->updateDetachTableStatus(TableKey{db_uuid, table_name}, false);
    }
    catch (Exception & e)
    {
        e.addMessage(
            "while trying to remove permanently detached flag. Table {}.{} may still be marked as permanently detached, and will not be "
            "reattached during server restart.",
            backQuote(db_name),
            backQuote(table_name));
        throw;
    }
}


std::shared_ptr<Proto::MetadataTable> DatabaseOnFDB::getTableMetaFromQuery(const ASTCreateQuery & create)
{
    auto tb_meta = std::make_shared<MetadataTable>();
    tb_meta->set_sql(serializeAST(create));
    return tb_meta;
}

ASTPtr DatabaseOnFDB::getQueryFromTableMeta(ContextPtr local_context, const MetadataTable & table_meta, bool throw_on_error)
{
    auto query = table_meta.sql();
    auto settings = local_context->getSettingsRef();
    ParserCreateQuery parser;
    const char * pos = query.data();
    std::string error_message;
    DB::ASTPtr ast = tryParseQuery(
        parser,
        pos,
        pos + query.size(),
        error_message,
        /* hilite = */ false,
        "in fdb ",
        /* allow_multi_statements = */ false,
        0,
        settings.max_parser_depth);

    if (!ast && throw_on_error)
        throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);
    else if (!ast)
        return nullptr;
    return ast;
}

std::unique_ptr<FoundationDB::Proto::MetadataDatabase> DatabaseOnFDB::getDBMetaFromQuery(const ASTCreateQuery & create)
{
    if (!create.attach)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SQL in database meta on fdb is not an ATTACH query");

    auto meta = std::make_unique<DatabaseMeta>();
    meta->set_name(create.getDatabase());
    meta->set_sql(serializeAST(create));
    return meta;
}

ASTPtr DatabaseOnFDB::getAttachQueryFromDBMeta(ContextPtr local_context, const FoundationDB::Proto::MetadataDatabase & meta)
{
    const auto & query = meta.sql();

    ParserCreateQuery parser;
    const char * pos = query.data();
    std::string error_message;
    DB::ASTPtr ast = tryParseQuery(
        parser,
        pos,
        pos + query.size(),
        error_message,
        /* hilite = */ false,
        "in fdb ",
        /* allow_multi_statements = */ false,
        0,
        local_context->getSettingsRef().max_parser_depth);

    if (!ast)
        throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);

    auto & create = ast->as<ASTCreateQuery &>();
    create.setDatabase(meta.name());
    if (!create.attach)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SQL in database meta on fdb is not an ATTACH query");

    return ast;
}
}
