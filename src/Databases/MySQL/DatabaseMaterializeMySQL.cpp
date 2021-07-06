#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#    include <Databases/MySQL/DatabaseMaterializeMySQL.h>

#    include <Interpreters/Context.h>
#    include <Databases/DatabaseOrdinary.h>
#    include <Databases/DatabaseAtomic.h>
#    include <Databases/MySQL/DatabaseMaterializeTablesIterator.h>
#    include <Databases/MySQL/MaterializeMySQLSyncThread.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Storages/StorageMaterializeMySQL.h>
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
DatabaseMaterializeMySQL<DatabaseOrdinary>::DatabaseMaterializeMySQL(
    ContextPtr context_,
    const String & database_name_,
    const String & metadata_path_,
    UUID /*uuid*/,
    const String & mysql_database_name_,
    mysqlxx::Pool && pool_,
    MySQLClient && client_,
    std::unique_ptr<MaterializeMySQLSettings> settings_)
    : DatabaseOrdinary(
        database_name_,
        metadata_path_,
        "data/" + escapeForFileName(database_name_) + "/",
        "DatabaseMaterializeMySQL<Ordinary> (" + database_name_ + ")",
        context_)
    , settings(std::move(settings_))
    , materialize_thread(context_, database_name_, mysql_database_name_, std::move(pool_), std::move(client_), settings.get())
{
}

template <>
DatabaseMaterializeMySQL<DatabaseAtomic>::DatabaseMaterializeMySQL(
    ContextPtr context_,
    const String & database_name_,
    const String & metadata_path_,
    UUID uuid,
    const String & mysql_database_name_,
    mysqlxx::Pool && pool_,
    MySQLClient && client_,
    std::unique_ptr<MaterializeMySQLSettings> settings_)
    : DatabaseAtomic(database_name_, metadata_path_, uuid, "DatabaseMaterializeMySQL<Atomic> (" + database_name_ + ")", context_)
    , settings(std::move(settings_))
    , materialize_thread(context_, database_name_, mysql_database_name_, std::move(pool_), std::move(client_), settings.get())
{
}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::rethrowExceptionIfNeed() const
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
void DatabaseMaterializeMySQL<Base>::setException(const std::exception_ptr & exception_)
{
    std::unique_lock<std::mutex> lock(Base::mutex);
    exception = exception_;
}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::loadStoredObjects(ContextMutablePtr context_, bool has_force_restore_data_flag, bool force_attach)
{
    Base::loadStoredObjects(context_, has_force_restore_data_flag, force_attach);
    if (!force_attach)
        materialize_thread.assertMySQLAvailable();

    materialize_thread.startSynchronization();
    started_up = true;
}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::createTable(ContextPtr context_, const String & name, const StoragePtr & table, const ASTPtr & query)
{
    assertCalledFromSyncThreadOrDrop("create table");
    Base::createTable(context_, name, table, query);
}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::dropTable(ContextPtr context_, const String & name, bool no_delay)
{
    assertCalledFromSyncThreadOrDrop("drop table");
    Base::dropTable(context_, name, no_delay);
}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::attachTable(const String & name, const StoragePtr & table, const String & relative_table_path)
{
    assertCalledFromSyncThreadOrDrop("attach table");
    Base::attachTable(name, table, relative_table_path);
}

template<typename Base>
StoragePtr DatabaseMaterializeMySQL<Base>::detachTable(const String & name)
{
    assertCalledFromSyncThreadOrDrop("detach table");
    return Base::detachTable(name);
}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::renameTable(ContextPtr context_, const String & name, IDatabase & to_database, const String & to_name, bool exchange, bool dictionary)
{
    assertCalledFromSyncThreadOrDrop("rename table");

    if (exchange)
        throw Exception("MaterializeMySQL database not support exchange table.", ErrorCodes::NOT_IMPLEMENTED);

    if (dictionary)
        throw Exception("MaterializeMySQL database not support rename dictionary.", ErrorCodes::NOT_IMPLEMENTED);

    if (to_database.getDatabaseName() != Base::getDatabaseName())
        throw Exception("Cannot rename with other database for MaterializeMySQL database.", ErrorCodes::NOT_IMPLEMENTED);

    Base::renameTable(context_, name, *this, to_name, exchange, dictionary);
}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::alterTable(ContextPtr context_, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    assertCalledFromSyncThreadOrDrop("alter table");
    Base::alterTable(context_, table_id, metadata);
}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::drop(ContextPtr context_)
{
    /// Remove metadata info
    fs::path metadata(Base::getMetadataPath() + "/.metadata");

    if (fs::exists(metadata))
        fs::remove(metadata);

    Base::drop(context_);
}

template<typename Base>
StoragePtr DatabaseMaterializeMySQL<Base>::tryGetTable(const String & name, ContextPtr context_) const
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
    {
        StoragePtr nested_storage = Base::tryGetTable(name, context_);

        if (!nested_storage)
            return {};

        return std::make_shared<StorageMaterializeMySQL>(std::move(nested_storage), this);
    }

    return Base::tryGetTable(name, context_);
}

template <typename Base>
DatabaseTablesIteratorPtr
DatabaseMaterializeMySQL<Base>::getTablesIterator(ContextPtr context_, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
    {
        DatabaseTablesIteratorPtr iterator = Base::getTablesIterator(context_, filter_by_table_name);
        return std::make_unique<DatabaseMaterializeTablesIterator>(std::move(iterator), this);
    }

    return Base::getTablesIterator(context_, filter_by_table_name);
}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::assertCalledFromSyncThreadOrDrop(const char * method) const
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread() && started_up)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MaterializeMySQL database not support {}", method);
}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::shutdownSynchronizationThread()
{
    materialize_thread.stopSynchronization();
    started_up = false;
}

template<typename Database, template<class> class Helper, typename... Args>
auto castToMaterializeMySQLAndCallHelper(Database * database, Args && ... args)
{
    using Ordinary = DatabaseMaterializeMySQL<DatabaseOrdinary>;
    using Atomic = DatabaseMaterializeMySQL<DatabaseAtomic>;
    using ToOrdinary = typename std::conditional_t<std::is_const_v<Database>, const Ordinary *, Ordinary *>;
    using ToAtomic = typename std::conditional_t<std::is_const_v<Database>, const Atomic *, Atomic *>;
    if (auto * database_materialize = typeid_cast<ToOrdinary>(database))
        return (database_materialize->*Helper<Ordinary>::v)(std::forward<Args>(args)...);
    if (auto * database_materialize = typeid_cast<ToAtomic>(database))
        return (database_materialize->*Helper<Atomic>::v)(std::forward<Args>(args)...);

    throw Exception("LOGICAL_ERROR: cannot cast to DatabaseMaterializeMySQL, it is a bug.", ErrorCodes::LOGICAL_ERROR);
}

template<typename T> struct HelperSetException { static constexpr auto v = &T::setException; };
void setSynchronizationThreadException(const DatabasePtr & materialize_mysql_db, const std::exception_ptr & exception)
{
    castToMaterializeMySQLAndCallHelper<IDatabase, HelperSetException>(materialize_mysql_db.get(), exception);
}

template<typename T> struct HelperStopSync { static constexpr auto v = &T::shutdownSynchronizationThread; };
void stopDatabaseSynchronization(const DatabasePtr & materialize_mysql_db)
{
    castToMaterializeMySQLAndCallHelper<IDatabase, HelperStopSync>(materialize_mysql_db.get());
}

template<typename T> struct HelperRethrow { static constexpr auto v = &T::rethrowExceptionIfNeed; };
void rethrowSyncExceptionIfNeed(const IDatabase * materialize_mysql_db)
{
    castToMaterializeMySQLAndCallHelper<const IDatabase, HelperRethrow>(materialize_mysql_db);
}

template class DatabaseMaterializeMySQL<DatabaseOrdinary>;
template class DatabaseMaterializeMySQL<DatabaseAtomic>;

}

#endif
