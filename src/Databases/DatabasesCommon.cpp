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
}

void applyMetadataChangesToCreateQuery(const ASTPtr & query, const StorageInMemoryMetadata & metadata)
{
    auto & ast_create_query = query->as<ASTCreateQuery &>();

    bool has_structure = ast_create_query.columns_list && ast_create_query.columns_list->columns;

    if (ast_create_query.as_table_function && !has_structure)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot alter table {} because it was created AS table function"
                                                     " and doesn't have structure in metadata", backQuote(ast_create_query.getTable()));

    if (!has_structure && !ast_create_query.is_dictionary)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot alter table {} metadata doesn't have structure", backQuote(ast_create_query.getTable()));

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
            throw Exception(ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY, "Cannot get metadata of {}.{}", backQuote(table_id.database_name), backQuote(table_id.table_name));
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
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot parser metadata of {}.{}", backQuote(table_id.database_name), backQuote(table_id.table_name));
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
        auto create_table_query = tryGetCreateTableQuery(it->name(), local_context);
        if (!create_table_query)
            throw Exception(ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP, "Couldn't get a create query for table {}.{}", backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(it->name()));

        const auto & create = create_table_query->as<const ASTCreateQuery &>();
        if (create.getTable() != it->name())
            throw Exception(ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP, "Got a create query with unexpected name {} for table {}.{}", backQuoteIfNeed(create.getTable()), backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(it->name()));

        auto storage = it->table();
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

}
