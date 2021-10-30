#include "config_core.h"

#if USE_MYSQL

#    include <Databases/MySQL/DatabaseMaterializedMySQL.h>

#    include <Interpreters/Context.h>
#    include <Databases/DatabaseOrdinary.h>
#    include <Databases/DatabaseAtomic.h>
#    include <Databases/MySQL/DatabaseMaterializedTablesIterator.h>
#    include <Databases/MySQL/MaterializedMySQLSyncThread.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Storages/StorageMaterializedMySQL.h>
#    include <Poco/Logger.h>
#    include <Common/setThreadName.h>
#    include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

template <>
DatabaseMaterializedMySQL<DatabaseOrdinary>::DatabaseMaterializedMySQL(
    ContextPtr context_,
    const String & database_name_,
    const String & metadata_path_,
    UUID /*uuid*/,
    const String & mysql_database_name_,
    mysqlxx::Pool && pool_,
    MySQLClient && client_,
    std::unique_ptr<MaterializedMySQLSettings> settings_)
    : DatabaseOrdinary(
        database_name_,
        metadata_path_,
        "data/" + escapeForFileName(database_name_) + "/",
        "DatabaseMaterializedMySQL<Ordinary> (" + database_name_ + ")",
        context_)
    , settings(std::move(settings_))
    , materialize_thread(context_, database_name_, mysql_database_name_, std::move(pool_), std::move(client_), settings.get())
{
}

template <>
DatabaseMaterializedMySQL<DatabaseAtomic>::DatabaseMaterializedMySQL(
    ContextPtr context_,
    const String & database_name_,
    const String & metadata_path_,
    UUID uuid,
    const String & mysql_database_name_,
    mysqlxx::Pool && pool_,
    MySQLClient && client_,
    std::unique_ptr<MaterializedMySQLSettings> settings_)
    : DatabaseAtomic(database_name_, metadata_path_, uuid, "DatabaseMaterializedMySQL<Atomic> (" + database_name_ + ")", context_)
    , settings(std::move(settings_))
    , materialize_thread(context_, database_name_, mysql_database_name_, std::move(pool_), std::move(client_), settings.get())
{
}

template<typename Base>
void DatabaseMaterializedMySQL<Base>::rethrowExceptionIfNeed() const
{
    std::unique_lock<std::mutex> lock(Base::mutex);

    if (!settings->allows_query_when_mysql_lost && exception)
    {
        try
        {
            std::rethrow_exception(exception);
        }
        catch (Exception & ex)
        {
            /// This method can be called from multiple threads
            /// and Exception can be modified concurrently by calling addMessage(...),
            /// so we rethrow a copy.
            throw Exception(ex);
        }
    }
}

template<typename Base>
void DatabaseMaterializedMySQL<Base>::setException(const std::exception_ptr & exception_)
{
    std::unique_lock<std::mutex> lock(Base::mutex);
    exception = exception_;
}

template <typename Base>
void DatabaseMaterializedMySQL<Base>::startupTables(ThreadPool & thread_pool, bool force_restore, bool force_attach)
{
    Base::startupTables(thread_pool, force_restore, force_attach);

    if (!force_attach)
        materialize_thread.assertMySQLAvailable();

    materialize_thread.startSynchronization();
    started_up = true;
}

template<typename Base>
void DatabaseMaterializedMySQL<Base>::createTable(ContextPtr context_, const String & name, const StoragePtr & table, const ASTPtr & query)
{
    assertCalledFromSyncThreadOrDrop("create table");
    Base::createTable(context_, name, table, query);
}

template<typename Base>
void DatabaseMaterializedMySQL<Base>::dropTable(ContextPtr context_, const String & name, bool no_delay)
{
    assertCalledFromSyncThreadOrDrop("drop table");
    Base::dropTable(context_, name, no_delay);
}

template<typename Base>
void DatabaseMaterializedMySQL<Base>::attachTable(const String & name, const StoragePtr & table, const String & relative_table_path)
{
    assertCalledFromSyncThreadOrDrop("attach table");
    Base::attachTable(name, table, relative_table_path);
}

template<typename Base>
StoragePtr DatabaseMaterializedMySQL<Base>::detachTable(const String & name)
{
    assertCalledFromSyncThreadOrDrop("detach table");
    return Base::detachTable(name);
}

template<typename Base>
void DatabaseMaterializedMySQL<Base>::renameTable(ContextPtr context_, const String & name, IDatabase & to_database, const String & to_name, bool exchange, bool dictionary)
{
    assertCalledFromSyncThreadOrDrop("rename table");

    if (exchange)
        throw Exception("MaterializedMySQL database not support exchange table.", ErrorCodes::NOT_IMPLEMENTED);

    if (dictionary)
        throw Exception("MaterializedMySQL database not support rename dictionary.", ErrorCodes::NOT_IMPLEMENTED);

    if (to_database.getDatabaseName() != Base::getDatabaseName())
        throw Exception("Cannot rename with other database for MaterializedMySQL database.", ErrorCodes::NOT_IMPLEMENTED);

    Base::renameTable(context_, name, *this, to_name, exchange, dictionary);
}

template<typename Base>
void DatabaseMaterializedMySQL<Base>::alterTable(ContextPtr context_, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    assertCalledFromSyncThreadOrDrop("alter table");
    Base::alterTable(context_, table_id, metadata);
}

template<typename Base>
void DatabaseMaterializedMySQL<Base>::drop(ContextPtr context_)
{
    /// Remove metadata info
    fs::path metadata(Base::getMetadataPath() + "/.metadata");

    if (fs::exists(metadata))
        fs::remove(metadata);

    Base::drop(context_);
}

template<typename Base>
StoragePtr DatabaseMaterializedMySQL<Base>::tryGetTable(const String & name, ContextPtr context_) const
{
    if (!MaterializedMySQLSyncThread::isMySQLSyncThread())
    {
        StoragePtr nested_storage = Base::tryGetTable(name, context_);

        if (!nested_storage)
            return {};

        return std::make_shared<StorageMaterializedMySQL>(std::move(nested_storage), this);
    }

    return Base::tryGetTable(name, context_);
}

template <typename Base>
DatabaseTablesIteratorPtr
DatabaseMaterializedMySQL<Base>::getTablesIterator(ContextPtr context_, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name) const
{
    if (!MaterializedMySQLSyncThread::isMySQLSyncThread())
    {
        DatabaseTablesIteratorPtr iterator = Base::getTablesIterator(context_, filter_by_table_name);
        return std::make_unique<DatabaseMaterializedTablesIterator>(std::move(iterator), this);
    }

    return Base::getTablesIterator(context_, filter_by_table_name);
}

template<typename Base>
void DatabaseMaterializedMySQL<Base>::assertCalledFromSyncThreadOrDrop(const char * method) const
{
    if (!MaterializedMySQLSyncThread::isMySQLSyncThread() && started_up)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MaterializedMySQL database not support {}", method);
}

template<typename Base>
void DatabaseMaterializedMySQL<Base>::shutdownSynchronizationThread()
{
    materialize_thread.stopSynchronization();
    started_up = false;
}

template<typename Database, template<class> class Helper, typename... Args>
auto castToMaterializedMySQLAndCallHelper(Database * database, Args && ... args)
{
    using Ordinary = DatabaseMaterializedMySQL<DatabaseOrdinary>;
    using Atomic = DatabaseMaterializedMySQL<DatabaseAtomic>;
    using ToOrdinary = typename std::conditional_t<std::is_const_v<Database>, const Ordinary *, Ordinary *>;
    using ToAtomic = typename std::conditional_t<std::is_const_v<Database>, const Atomic *, Atomic *>;
    if (auto * database_materialize = typeid_cast<ToOrdinary>(database))
        return (database_materialize->*Helper<Ordinary>::v)(std::forward<Args>(args)...);
    if (auto * database_materialize = typeid_cast<ToAtomic>(database))
        return (database_materialize->*Helper<Atomic>::v)(std::forward<Args>(args)...);

    throw Exception("LOGICAL_ERROR: cannot cast to DatabaseMaterializedMySQL, it is a bug.", ErrorCodes::LOGICAL_ERROR);
}

template<typename T> struct HelperSetException { static constexpr auto v = &T::setException; };
void setSynchronizationThreadException(const DatabasePtr & materialized_mysql_db, const std::exception_ptr & exception)
{
    castToMaterializedMySQLAndCallHelper<IDatabase, HelperSetException>(materialized_mysql_db.get(), exception);
}

template<typename T> struct HelperStopSync { static constexpr auto v = &T::shutdownSynchronizationThread; };
void stopDatabaseSynchronization(const DatabasePtr & materialized_mysql_db)
{
    castToMaterializedMySQLAndCallHelper<IDatabase, HelperStopSync>(materialized_mysql_db.get());
}

template<typename T> struct HelperRethrow { static constexpr auto v = &T::rethrowExceptionIfNeed; };
void rethrowSyncExceptionIfNeed(const IDatabase * materialized_mysql_db)
{
    castToMaterializedMySQLAndCallHelper<const IDatabase, HelperRethrow>(materialized_mysql_db);
}

template class DatabaseMaterializedMySQL<DatabaseOrdinary>;
template class DatabaseMaterializedMySQL<DatabaseAtomic>;

}

#endif
