#include <base/scope_guard.h>
#include <Common/logger_useful.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Storages/IStorage.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
}

DatabaseMemory::DatabaseMemory(const String & name_, ContextPtr context_)
    : DatabaseWithOwnTablesBase(name_, "DatabaseMemory(" + name_ + ")", context_)
    , data_path("data/" + escapeForFileName(database_name) + "/")
{}

void DatabaseMemory::createTable(
    ContextPtr /*context*/,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    std::unique_lock lock{mutex};
    attachTableUnlocked(table_name, table, lock);
    create_queries.emplace(table_name, query);
}

void DatabaseMemory::dropTable(
    ContextPtr /*context*/,
    const String & table_name,
    bool /*no_delay*/)
{
    std::unique_lock lock{mutex};
    auto table = detachTableUnlocked(table_name, lock);
    try
    {
        /// Remove table without lock since:
        /// - it does not require it
        /// - it may cause lock-order-inversion if underlying storage need to
        ///   resolve tables (like StorageLiveView)
        SCOPE_EXIT(lock.lock());
        lock.unlock();
        table->drop();

        if (table->storesDataOnDisk())
        {
            assert(database_name != DatabaseCatalog::TEMPORARY_DATABASE);
            fs::path table_data_dir{getTableDataPath(table_name)};
            if (fs::exists(table_data_dir))
                fs::remove_all(table_data_dir);
        }
    }
    catch (...)
    {
        assert(database_name != DatabaseCatalog::TEMPORARY_DATABASE);
        attachTableUnlocked(table_name, table, lock);
        throw;
    }
    table->is_dropped = true;
    create_queries.erase(table_name);
    UUID table_uuid = table->getStorageID().uuid;
    if (table_uuid != UUIDHelpers::Nil)
        DatabaseCatalog::instance().removeUUIDMappingFinally(table_uuid);
}

ASTPtr DatabaseMemory::getCreateDatabaseQuery() const
{
    auto create_query = std::make_shared<ASTCreateQuery>();
    create_query->setDatabase(getDatabaseName());
    create_query->set(create_query->storage, std::make_shared<ASTStorage>());
    auto engine = makeASTFunction(getEngineName());
    engine->no_empty_args = true;
    create_query->storage->set(create_query->storage->engine, engine);

    if (const auto comment_value = getDatabaseComment(); !comment_value.empty())
        create_query->set(create_query->comment, std::make_shared<ASTLiteral>(comment_value));

    return create_query;
}

ASTPtr DatabaseMemory::getCreateTableQueryImpl(const String & table_name, ContextPtr, bool throw_on_error) const
{
    std::lock_guard lock{mutex};
    auto it = create_queries.find(table_name);
    if (it == create_queries.end() || !it->second)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "There is no metadata of table {} in database {}", table_name, database_name);
        else
            return {};
    }
    return it->second->clone();
}

UUID DatabaseMemory::tryGetTableUUID(const String & table_name) const
{
    if (auto table = tryGetTable(table_name, getContext()))
        return table->getStorageID().uuid;
    return UUIDHelpers::Nil;
}

void DatabaseMemory::drop(ContextPtr local_context)
{
    /// Remove data on explicit DROP DATABASE
    std::filesystem::remove_all(local_context->getPath() + data_path);
}

void DatabaseMemory::alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    std::lock_guard lock{mutex};
    auto it = create_queries.find(table_id.table_name);
    if (it == create_queries.end() || !it->second)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Cannot alter: There is no metadata of table {}", table_id.getNameForLogs());

    applyMetadataChangesToCreateQuery(it->second, metadata);
    TableNamesSet new_dependencies = getDependenciesSetFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), it->second);
    DatabaseCatalog::instance().updateLoadingDependencies(table_id, std::move(new_dependencies));
}

}
