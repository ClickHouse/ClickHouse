#include <Databases/DatabasesCommon.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageFactory.h>
#include <Common/typeid_cast.h>
#include <Common/escapeForFileName.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/RestorerFromBackup.h>
#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseMemory.h>
#include <Storages/StorageMaterializedView.h>
#include <Interpreters/DDLTask.h>
#include <Common/atomicRename.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DATABASE;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int INCONSISTENT_METADATA_FOR_BACKUP;
    extern const int INCORRECT_QUERY;
}

void applyMetadataChangesToCreateQuery(const ASTPtr & query, const StorageInMemoryMetadata & metadata)
{
    auto & ast_create_query = query->as<ASTCreateQuery &>();

    bool has_structure = ast_create_query.columns_list && ast_create_query.columns_list->columns;

    if (ast_create_query.as_table_function && !has_structure)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot alter table {} because it was created AS table function"
                                                     " and doesn't have structure in metadata", backQuote(ast_create_query.getTable()));

    if (!has_structure && !ast_create_query.is_dictionary)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot alter table {} metadata doesn't have structure",
                        backQuote(ast_create_query.getTable()));

    if (!ast_create_query.is_dictionary)
    {
        ASTPtr new_columns = InterpreterCreateQuery::formatColumns(metadata.columns);
        ASTPtr new_indices = InterpreterCreateQuery::formatIndices(metadata.secondary_indices);
        ASTPtr new_constraints = InterpreterCreateQuery::formatConstraints(metadata.constraints);
        ASTPtr new_projections = InterpreterCreateQuery::formatProjections(metadata.projections);

        ast_create_query.columns_list->replace(ast_create_query.columns_list->columns, new_columns);
        ast_create_query.columns_list->setOrReplace(ast_create_query.columns_list->indices, new_indices);
        ast_create_query.columns_list->setOrReplace(ast_create_query.columns_list->constraints, new_constraints);
        ast_create_query.columns_list->setOrReplace(ast_create_query.columns_list->projections, new_projections);
    }

    if (metadata.select.select_query)
    {
        query->replace(ast_create_query.select, metadata.select.select_query);
    }

    /// MaterializedView, Dictionary are types of CREATE query without storage.
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
            else if (storage_ast.sample_by != nullptr) /// SAMPLE BY was removed
                storage_ast.sample_by = nullptr;

            if (metadata.table_ttl.definition_ast)
                storage_ast.set(storage_ast.ttl_table, metadata.table_ttl.definition_ast);
            else if (storage_ast.ttl_table != nullptr) /// TTL was removed
                storage_ast.ttl_table = nullptr;

            if (metadata.settings_changes)
                storage_ast.set(storage_ast.settings, metadata.settings_changes);
        }
    }

    if (metadata.comment.empty())
        ast_create_query.reset(ast_create_query.comment);
    else
        ast_create_query.set(ast_create_query.comment, std::make_shared<ASTLiteral>(metadata.comment));
}


ASTPtr getCreateQueryFromStorage(const StoragePtr & storage, const ASTPtr & ast_storage, bool only_ordinary, uint32_t max_parser_depth, bool throw_on_error)
{
    auto table_id = storage->getStorageID();
    auto metadata_ptr = storage->getInMemoryMetadataPtr();
    if (metadata_ptr == nullptr)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY, "Cannot get metadata of {}.{}",
                            backQuote(table_id.database_name), backQuote(table_id.table_name));
        else
            return nullptr;
    }

    auto create_table_query = std::make_shared<ASTCreateQuery>();
    create_table_query->attach = false;
    create_table_query->setTable(table_id.table_name);
    create_table_query->setDatabase(table_id.database_name);
    create_table_query->set(create_table_query->storage, ast_storage);

    /// setup create table query columns info.
    {
        auto ast_columns_list = std::make_shared<ASTColumns>();
        auto ast_expression_list = std::make_shared<ASTExpressionList>();
        NamesAndTypesList columns;
        if (only_ordinary)
            columns = metadata_ptr->columns.getOrdinary();
        else
            columns = metadata_ptr->columns.getAll();
        for (const auto & column_name_and_type: columns)
        {
            const auto & ast_column_declaration = std::make_shared<ASTColumnDeclaration>();
            ast_column_declaration->name = column_name_and_type.name;
            /// parser typename
            {
                ASTPtr ast_type;
                auto type_name = column_name_and_type.type->getName();
                const auto * string_end = type_name.c_str() + type_name.length();
                Expected expected;
                expected.max_parsed_pos = string_end;
                Tokens tokens(type_name.c_str(), string_end);
                IParser::Pos pos(tokens, max_parser_depth);
                ParserDataType parser;
                if (!parser.parse(pos, ast_type, expected))
                {
                    if (throw_on_error)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot parser metadata of {}.{}",
                                        backQuote(table_id.database_name), backQuote(table_id.table_name));
                    else
                        return nullptr;
                }
                ast_column_declaration->type = ast_type;
            }
            ast_expression_list->children.emplace_back(ast_column_declaration);
        }

        ast_columns_list->set(ast_columns_list->columns, ast_expression_list);
        create_table_query->set(create_table_query->columns_list, ast_columns_list);
    }
    return create_table_query;
}


void cleanupObjectDefinitionFromTemporaryFlags(ASTCreateQuery & query)
{
    query.as_database.clear();
    query.as_table.clear();
    query.if_not_exists = false;
    query.is_populate = false;
    query.is_create_empty = false;
    query.replace_view = false;
    query.replace_table = false;
    query.create_or_replace = false;

    /// For views it is necessary to save the SELECT query itself, for the rest - on the contrary
    if (!query.isView())
        query.select = nullptr;

    query.format = nullptr;
    query.out_file = nullptr;
}


DatabaseWithOwnTablesBase::DatabaseWithOwnTablesBase(const String & name_, const String & logger, ContextPtr context_)
        : IDatabase(name_), WithContext(context_->getGlobalContext()), log(&Poco::Logger::get(logger))
{
}

bool DatabaseWithOwnTablesBase::isTableExist(const String & table_name, ContextPtr) const
{
    std::lock_guard lock(mutex);
    return tables.find(table_name) != tables.end();
}

StoragePtr DatabaseWithOwnTablesBase::tryGetTable(const String & table_name, ContextPtr) const
{
    std::lock_guard lock(mutex);
    auto it = tables.find(table_name);
    if (it != tables.end())
        return it->second;
    return {};
}

DatabaseTablesIteratorPtr DatabaseWithOwnTablesBase::getTablesIterator(ContextPtr, const FilterByNameFunction & filter_by_table_name) const
{
    std::lock_guard lock(mutex);
    if (!filter_by_table_name)
        return std::make_unique<DatabaseTablesSnapshotIterator>(tables, database_name);

    Tables filtered_tables;
    for (const auto & [table_name, storage] : tables)
        if (filter_by_table_name(table_name))
            filtered_tables.emplace(table_name, storage);

    return std::make_unique<DatabaseTablesSnapshotIterator>(std::move(filtered_tables), database_name);
}

bool DatabaseWithOwnTablesBase::empty() const
{
    std::lock_guard lock(mutex);
    return tables.empty();
}

StoragePtr DatabaseWithOwnTablesBase::detachTable(ContextPtr /* context_ */, const String & table_name)
{
    std::lock_guard lock(mutex);
    return detachTableUnlocked(table_name);
}

StoragePtr DatabaseWithOwnTablesBase::detachTableUnlocked(const String & table_name)
{
    StoragePtr res;

    auto it = tables.find(table_name);
    if (it == tables.end())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist",
                        backQuote(database_name), backQuote(table_name));
    res = it->second;
    tables.erase(it);
    res->is_detached = true;

    auto table_id = res->getStorageID();
    if (table_id.hasUUID())
    {
        assert(database_name == DatabaseCatalog::TEMPORARY_DATABASE || getUUID() != UUIDHelpers::Nil);
        DatabaseCatalog::instance().removeUUIDMapping(table_id.uuid);
    }

    return res;
}

void DatabaseWithOwnTablesBase::attachTable(ContextPtr /* context_ */, const String & table_name, const StoragePtr & table, const String &)
{
    std::lock_guard lock(mutex);
    attachTableUnlocked(table_name, table);
}

void DatabaseWithOwnTablesBase::attachTableUnlocked(const String & table_name, const StoragePtr & table)
{
    auto table_id = table->getStorageID();
    if (table_id.database_name != database_name)
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database was renamed to `{}`, cannot create table in `{}`",
                        database_name, table_id.database_name);

    if (table_id.hasUUID())
    {
        assert(database_name == DatabaseCatalog::TEMPORARY_DATABASE || getUUID() != UUIDHelpers::Nil);
        DatabaseCatalog::instance().addUUIDMapping(table_id.uuid, shared_from_this(), table);
    }

    if (!tables.emplace(table_name, table).second)
    {
        if (table_id.hasUUID())
            DatabaseCatalog::instance().removeUUIDMapping(table_id.uuid);
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {} already exists.", table_id.getFullTableName());
    }

    /// It is important to reset is_detached here since in case of RENAME in
    /// non-Atomic database the is_detached is set to true before RENAME.
    table->is_detached = false;
}

void DatabaseWithOwnTablesBase::shutdown()
{
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function tables can work with database, and mutex is not recursive.

    Tables tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = tables;
    }

    for (const auto & kv : tables_snapshot)
    {
        kv.second->flush();
    }

    for (const auto & kv : tables_snapshot)
    {
        auto table_id = kv.second->getStorageID();
        kv.second->flushAndShutdown();
        if (table_id.hasUUID())
        {
            assert(getDatabaseName() == DatabaseCatalog::TEMPORARY_DATABASE || getUUID() != UUIDHelpers::Nil);
            DatabaseCatalog::instance().removeUUIDMapping(table_id.uuid);
        }
    }

    std::lock_guard lock(mutex);
    tables.clear();
}

DatabaseWithOwnTablesBase::~DatabaseWithOwnTablesBase()
{
    try
    {
        DatabaseWithOwnTablesBase::shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

StoragePtr DatabaseWithOwnTablesBase::getTableUnlocked(const String & table_name) const
{
    auto it = tables.find(table_name);
    if (it != tables.end())
        return it->second;
    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist",
                    backQuote(database_name), backQuote(table_name));
}

std::vector<std::pair<ASTPtr, StoragePtr>> DatabaseWithOwnTablesBase::getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const
{
    std::vector<std::pair<ASTPtr, StoragePtr>> res;

    for (auto it = getTablesIterator(local_context, filter); it->isValid(); it->next())
    {
        auto storage = it->table();
        if (!storage)
            continue; /// Probably the table has been just dropped.

        auto create_table_query = tryGetCreateTableQuery(it->name(), local_context);
        if (!create_table_query)
            throw Exception(ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP,
                            "Couldn't get a create query for table {}.{}",
                            backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(it->name()));

        const auto & create = create_table_query->as<const ASTCreateQuery &>();
        if (create.getTable() != it->name())
            throw Exception(ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP,
                            "Got a create query with unexpected name {} for table {}.{}",
                            backQuoteIfNeed(create.getTable()),
                            backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(it->name()));

        storage->adjustCreateQueryForBackup(create_table_query);
        res.emplace_back(create_table_query, storage);
    }

    return res;
}

void DatabaseWithOwnTablesBase::createTableRestoredFromBackup(const ASTPtr & create_table_query, ContextMutablePtr local_context, std::shared_ptr<IRestoreCoordination>, UInt64)
{
    /// Creates a table by executing a "CREATE TABLE" query.
    InterpreterCreateQuery interpreter{create_table_query, local_context};
    interpreter.setInternal(true);
    interpreter.execute();
}

template <class DatabaseClass>
void DatabaseWithOwnTablesBase::renameTableImpl(ContextPtr local_context, const String & table_name, IDatabase & to_database,
                                 const String & to_table_name, bool exchange, bool dictionary)
    TSA_NO_THREAD_SAFETY_ANALYSIS   /// TSA does not support conditional locking
{
    auto & current_db = dynamic_cast<DatabaseClass &>(*this);
    auto & other_db = dynamic_cast<DatabaseClass &>(to_database);
    bool inside_database = &current_db == &other_db;

    String old_metadata_path;
    String new_metadata_path;

    if constexpr (std::is_same_v<DatabaseClass, DatabaseAtomic>)
    {
        old_metadata_path = current_db.getObjectMetadataPath(table_name);
        new_metadata_path = to_database.getObjectMetadataPath(to_table_name);
    }

    auto detach = [](DatabaseClass & db, const String & table_name_, bool has_symlink) TSA_REQUIRES(db.mutex)
    {
        auto it = db.table_name_to_path.find(table_name_);
        String table_data_path_saved;
        /// Path can be not set for DDL dictionaries, but it does not matter for StorageDictionary.
        if (it != db.table_name_to_path.end())
            table_data_path_saved = it->second;
        assert(!table_data_path_saved.empty());
        db.tables.erase(table_name_);
        db.table_name_to_path.erase(table_name_);
        if (has_symlink)
            db.tryRemoveSymlink(table_name_);
        return table_data_path_saved;
    };

    auto attach = [](DatabaseClass & db, const String & table_name_, const String & table_data_path_, const StoragePtr & table_) TSA_REQUIRES(db.mutex)
    {
        db.tables.emplace(table_name_, table_);
        if (table_data_path_.empty())
            return;
        db.table_name_to_path.emplace(table_name_, table_data_path_);
        if (table_->storesDataOnDisk())
            db.tryCreateSymlink(table_name_, table_data_path_);
    };

    auto assert_can_move_mat_view = [inside_database](const StoragePtr & table_)
    {
        if (inside_database)
            return;
        if (const auto * mv = dynamic_cast<const StorageMaterializedView *>(table_.get()))
            if (mv->hasInnerTable())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot move MaterializedView with inner table to other database");
    };

    String table_data_path;
    String other_table_data_path;

    if (inside_database && table_name == to_table_name)
        return;

    std::unique_lock<std::mutex> db_lock;
    std::unique_lock<std::mutex> other_db_lock;
    if (inside_database)
    {
        db_lock = std::unique_lock{mutex};
    }
    else if (&current_db < &other_db)
    {
        db_lock = std::unique_lock{current_db.mutex};
        other_db_lock = std::unique_lock{other_db.mutex};
    }
    else
    {
        other_db_lock = std::unique_lock{other_db.mutex};
        db_lock = std::unique_lock{current_db.mutex};
    }

    if constexpr (std::is_same_v<DatabaseClass, DatabaseAtomic>)
    {
        if (!exchange)
            other_db.checkMetadataFilenameAvailabilityUnlocked(to_table_name);
    }

    StoragePtr table = getTableUnlocked(table_name);

    if (dictionary && !table->isDictionary())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Use RENAME/EXCHANGE TABLE (instead of RENAME/EXCHANGE DICTIONARY) for tables");

    StorageID old_table_id = table->getStorageID();
    StorageID new_table_id = {other_db.database_name, to_table_name, old_table_id.uuid};
    table->checkTableCanBeRenamed({new_table_id});
    assert_can_move_mat_view(table);
    StoragePtr other_table;
    StorageID other_table_new_id = StorageID::createEmpty();
    if (exchange)
    {
        other_table = other_db.getTableUnlocked(to_table_name);
        if (dictionary && !other_table->isDictionary())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Use RENAME/EXCHANGE TABLE (instead of RENAME/EXCHANGE DICTIONARY) for tables");
        other_table_new_id = {database_name, table_name, other_table->getStorageID().uuid};
        other_table->checkTableCanBeRenamed(other_table_new_id);
        assert_can_move_mat_view(other_table);
    }

    if constexpr (std::is_same_v<DatabaseClass, DatabaseAtomic>)
    {
        /// Table renaming actually begins here
        auto txn = local_context->getZooKeeperMetadataTransaction();
        if (txn && !local_context->isInternalSubquery())
            txn->commit();     /// Commit point (a sort of) for Replicated database

        /// NOTE: replica will be lost if server crashes before the following rename
        /// TODO better detection and recovery

        if (exchange)
            renameExchange(old_metadata_path, new_metadata_path);
        else
            renameNoReplace(old_metadata_path, new_metadata_path);
    }

    /// After metadata was successfully moved, the following methods should not throw (if they do, it's a logical error)
    table_data_path = detach(current_db, table_name, table->storesDataOnDisk());
    if (exchange)
        other_table_data_path = detach(other_db, to_table_name, other_table->storesDataOnDisk());

    table->renameInMemory(new_table_id);
    if (exchange)
        other_table->renameInMemory(other_table_new_id);

    if (!inside_database)
    {
        DatabaseCatalog::instance().updateUUIDMapping(old_table_id.uuid, other_db.shared_from_this(), table);
        if (exchange)
            DatabaseCatalog::instance().updateUUIDMapping(other_table->getStorageID().uuid, current_db.shared_from_this(), other_table);
    }

    attach(other_db, to_table_name, table_data_path, table);
    if (exchange)
        attach(current_db, table_name, other_table_data_path, other_table);
}

template void DatabaseWithOwnTablesBase::renameTableImpl<DatabaseAtomic>(ContextPtr, const String &, IDatabase &, const String &, bool, bool);
template void DatabaseWithOwnTablesBase::renameTableImpl<DatabaseMemory>(ContextPtr, const String &, IDatabase &, const String &, bool, bool);

template <bool lazy>
TableDataMapping<lazy>::TableDataMapping(const String & current_path_, const String & path_to_table_symlinks_, const String & logger)
    : log_mapping(&Poco::Logger::get(logger))
    , current_path(current_path_)
    , path_to_table_symlinks(fs::path(current_path_) / path_to_table_symlinks_ / "")
{
    if constexpr (!lazy)
        fs::create_directories(path_to_table_symlinks);
}

template <bool lazy>
void TableDataMapping<lazy>::tryCreateSymlink(const String & table_name, const String & actual_data_path, bool if_data_path_exist)
{
    try
    {
        if constexpr (lazy)
        {
            if (!fs::exists(path_to_table_symlinks))
                fs::create_directories(path_to_table_symlinks);
        }

        String link = path_to_table_symlinks + escapeForFileName(table_name);
        fs::path data = fs::canonical(current_path) / actual_data_path;
        if (!if_data_path_exist || fs::exists(data))
            fs::create_directory_symlink(data, link);
    }
    catch (...)
    {
        LOG_WARNING(log_mapping, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }
}

template <bool lazy>
void TableDataMapping<lazy>::tryRemoveSymlink(const String & table_name)
{
    try
    {
        String path = path_to_table_symlinks + escapeForFileName(table_name);
        fs::remove(path);
    }
    catch (...)
    {
        LOG_WARNING(log_mapping, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }
}

template <bool lazy>
String TableDataMapping<lazy>::getTableDataPathUnlocked(const String & table_name, const String & database_name) const
{
    auto it = table_name_to_path.find(table_name);
    if (it == table_name_to_path.end())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} not found in database {}", table_name, database_name);
    assert(it->second != data_path && !it->second.empty());
    return it->second;
}

template class TableDataMapping<true>;
template class TableDataMapping<false>;

}
