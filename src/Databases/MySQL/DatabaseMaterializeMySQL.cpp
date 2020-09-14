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
#    include <Poco/File.h>
#    include <Poco/Logger.h>
#    include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

template<>
DatabaseMaterializeMySQL<DatabaseOrdinary>::DatabaseMaterializeMySQL(
    const Context & context, const String & database_name_, const String & metadata_path_, UUID /*uuid*/, const IAST * /*database_engine_define_*/
    , const String & mysql_database_name_, mysqlxx::Pool && pool_, MySQLClient && client_, std::unique_ptr<MaterializeMySQLSettings> settings_)
    : DatabaseOrdinary(database_name_, metadata_path_, context)
    //, global_context(context.getGlobalContext())
    //, engine_define(database_engine_define_->clone())
    //, nested_database(uuid == UUIDHelpers::Nil
    //                  ? std::make_shared<DatabaseOrdinary>(database_name_, metadata_path_, context)
    //                  : std::make_shared<DatabaseAtomic>(database_name_, metadata_path_, uuid, context))
    , settings(std::move(settings_))
    //, log(&Poco::Logger::get("DatabaseMaterializeMySQL"))
    , materialize_thread(context, database_name_, mysql_database_name_, std::move(pool_), std::move(client_), settings.get())
{
}

template<>
DatabaseMaterializeMySQL<DatabaseAtomic>::DatabaseMaterializeMySQL(
    const Context & context, const String & database_name_, const String & metadata_path_, UUID uuid, const IAST * /*database_engine_define_*/
    , const String & mysql_database_name_, mysqlxx::Pool && pool_, MySQLClient && client_, std::unique_ptr<MaterializeMySQLSettings> settings_)
    : DatabaseAtomic(database_name_, metadata_path_, uuid, context)
    //, global_context(context.getGlobalContext())
    //, engine_define(database_engine_define_->clone())
    //, nested_database(uuid == UUIDHelpers::Nil
    //                  ? std::make_shared<DatabaseOrdinary>(database_name_, metadata_path_, context)
    //                  : std::make_shared<DatabaseAtomic>(database_name_, metadata_path_, uuid, context))
    , settings(std::move(settings_))
    //, log(&Poco::Logger::get("DatabaseMaterializeMySQL"))
    , materialize_thread(context, database_name_, mysql_database_name_, std::move(pool_), std::move(client_), settings.get())
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

//ASTPtr DatabaseMaterializeMySQL::getCreateDatabaseQuery() const
//{
//    const auto & create_query = std::make_shared<ASTCreateQuery>();
//    create_query->database = database_name;
//    create_query->uuid = nested_database->getUUID();
//    create_query->set(create_query->storage, engine_define);
//    return create_query;
//}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::loadStoredObjects(Context & context, bool has_force_restore_data_flag, bool force_attach)
{
    Base::loadStoredObjects(context, has_force_restore_data_flag, force_attach);
    try
    {
        //std::unique_lock<std::mutex> lock(Base::mutex);
        //Base::loadStoredObjects(context, has_force_restore_data_flag, force_attach);
        materialize_thread.startSynchronization();
        started_up = true;
    }
    catch (...)
    {
        tryLogCurrentException(Base::log, "Cannot load MySQL nested database stored objects.");

        if (!force_attach)
            throw;
    }
}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::shutdown()
{
    //materialize_thread.stopSynchronization();
    //started_up = false;

    Base::shutdown();

//    //FIXME
//    //auto iterator = nested_database->getTablesIterator(global_context, {});
//    auto iterator = Base::getTablesIterator(Base::global_context, {});
//
//    /// We only shutdown the table, The tables is cleaned up when destructed database
//    for (; iterator->isValid(); iterator->next())
//        iterator->table()->shutdown();
}

//bool DatabaseMaterializeMySQL::empty() const
//{
//    return nested_database->empty();
//}
//
//String DatabaseMaterializeMySQL::getDataPath() const
//{
//    return nested_database->getDataPath();
//}
//
//String DatabaseMaterializeMySQL::getMetadataPath() const
//{
//    return nested_database->getMetadataPath();
//}
//
//String DatabaseMaterializeMySQL::getTableDataPath(const String & table_name) const
//{
//    return nested_database->getTableDataPath(table_name);
//}
//
//String DatabaseMaterializeMySQL::getTableDataPath(const ASTCreateQuery & query) const
//{
//    return nested_database->getTableDataPath(query);
//}
//
//String DatabaseMaterializeMySQL::getObjectMetadataPath(const String & table_name) const
//{
//    return nested_database->getObjectMetadataPath(table_name);
//}
//
//UUID DatabaseMaterializeMySQL::tryGetTableUUID(const String & table_name) const
//{
//    return nested_database->tryGetTableUUID(table_name);
//}
//
//time_t DatabaseMaterializeMySQL::getObjectMetadataModificationTime(const String & name) const
//{
//    return nested_database->getObjectMetadataModificationTime(name);
//}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::createTable(const Context & context, const String & name, const StoragePtr & table, const ASTPtr & query)
{
    assertCalledFromSyncThreadOrDrop("create table");

    //nested_database->createTable(context, name, std::make_shared<StorageMaterializeMySQL>(std::move(table), this), query);
    Base::createTable(context, name, table, query);
}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::dropTable(const Context & context, const String & name, bool no_delay)
{
    ///FIXME cannot be called from DROP DATABASE, so we need hack with shouldBeEmptyOnDetach = false
    assertCalledFromSyncThreadOrDrop("drop table");

    Base::dropTable(context, name, no_delay);
}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::attachTable(const String & name, const StoragePtr & table, const String & relative_table_path)
{
    assertCalledFromSyncThreadOrDrop("attach table");

    //nested_database->attachTable(name, std::make_shared<StorageMaterializeMySQL>(std::move(table), this), relative_table_path);
    Base::attachTable(name, table, relative_table_path);
}

template<typename Base>
StoragePtr DatabaseMaterializeMySQL<Base>::detachTable(const String & name)
{
    assertCalledFromSyncThreadOrDrop("detach table");
    //return std::dynamic_pointer_cast<StorageMaterializeMySQL>(nested_database->detachTable(name));
    return Base::detachTable(name);
}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::renameTable(const Context & context, const String & name, IDatabase & to_database, const String & to_name, bool exchange, bool dictionary)
{
    assertCalledFromSyncThreadOrDrop("rename table");

    if (exchange)
        throw Exception("MaterializeMySQL database not support exchange table.", ErrorCodes::NOT_IMPLEMENTED);

    if (dictionary)
        throw Exception("MaterializeMySQL database not support rename dictionary.", ErrorCodes::NOT_IMPLEMENTED);

    if (to_database.getDatabaseName() != Base::getDatabaseName())
        throw Exception("Cannot rename with other database for MaterializeMySQL database.", ErrorCodes::NOT_IMPLEMENTED);

    Base::renameTable(context, name, *this, to_name, exchange, dictionary);
}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::alterTable(const Context & context, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    assertCalledFromSyncThreadOrDrop("alter table");
    Base::alterTable(context, table_id, metadata);
}

//template<typename Base>
//bool DatabaseMaterializeMySQL<Base>::shouldBeEmptyOnDetach() const
//{
//    return false;   /// FIXME
//}

//template<typename Base>
//void DatabaseMaterializeMySQL<Base>::drop(const Context & context)
//{
//    /// FIXME
//    if (Base::shouldBeEmptyOnDetach())
//    {
//        for (auto iterator = Base::getTablesIterator(context, {}); iterator->isValid(); iterator->next())
//        {
//            TableExclusiveLockHolder table_lock = iterator->table()->lockExclusively(
//                context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
//
//            Base::dropTable(context, iterator->name(), true);
//        }
//
//        /// Remove metadata info
//        Poco::File metadata(Base::getMetadataPath() + "/.metadata");
//
//        if (metadata.exists())
//            metadata.remove(false);
//    }
//
//    Base::drop(context);
//}

template<typename Base>
void DatabaseMaterializeMySQL<Base>::drop(const Context & context)
{
    /// Remove metadata info
    Poco::File metadata(Base::getMetadataPath() + "/.metadata");

    if (metadata.exists())
        metadata.remove(false);

    Base::drop(context);
}

//bool DatabaseMaterializeMySQL::isTableExist(const String & name, const Context & context) const
//{
//    return nested_database->isTableExist(name, context);
//}

template<typename Base>
StoragePtr DatabaseMaterializeMySQL<Base>::tryGetTable(const String & name, const Context & context) const
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
    {
        using Storage = StorageMaterializeMySQL<DatabaseMaterializeMySQL<Base>>;
        StoragePtr nested_storage = Base::tryGetTable(name, context);

        if (!nested_storage)
            return {};

        return std::make_shared<Storage>(std::move(nested_storage), this);
    }

    return Base::tryGetTable(name, context);
    //auto table = nested_database->tryGetTable(name, context);
    //if (table && MaterializeMySQLSyncThread::isMySQLSyncThread())
    //    return typeid_cast<const StorageMaterializeMySQL &>(*table).getNested();
    //return table;
}

template<typename Base>
DatabaseTablesIteratorPtr DatabaseMaterializeMySQL<Base>::getTablesIterator(const Context & context, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
    {
        using TablesIterator = DatabaseMaterializeTablesIterator<DatabaseMaterializeMySQL<Base>>;
        DatabaseTablesIteratorPtr iterator = Base::getTablesIterator(context, filter_by_table_name);
        return std::make_unique<TablesIterator>(std::move(iterator), this);
    }

    return Base::getTablesIterator(context, filter_by_table_name);
}

//void DatabaseMaterializeMySQL::assertCanBeDetached(bool cleanup)
//{
//    nested_database->assertCanBeDetached(cleanup);
//}

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

void setSynchronizationThreadException(const DatabasePtr & materialize_mysql_db, const std::exception_ptr & exception)
{
    if (auto * database_materialize = typeid_cast<DatabaseMaterializeMySQL<DatabaseOrdinary> *>(materialize_mysql_db.get()))
        return database_materialize->setException(exception);
    if (auto * database_materialize = typeid_cast<DatabaseMaterializeMySQL<DatabaseAtomic> *>(materialize_mysql_db.get()))
        return database_materialize->setException(exception);

    throw Exception("LOGICAL_ERROR: cannot cast to DatabaseMaterializeMySQL, it is a bug.", ErrorCodes::LOGICAL_ERROR);
}

void stopDatabaseSynchronization(const DatabasePtr & materialize_mysql_db)
{
    if (auto * database_materialize = typeid_cast<DatabaseMaterializeMySQL<DatabaseOrdinary> *>(materialize_mysql_db.get()))
        return database_materialize->shutdownSynchronizationThread();
    if (auto * database_materialize = typeid_cast<DatabaseMaterializeMySQL<DatabaseAtomic> *>(materialize_mysql_db.get()))
        return database_materialize->shutdownSynchronizationThread();

    throw Exception("LOGICAL_ERROR: cannot cast to DatabaseMaterializeMySQL, it is a bug.", ErrorCodes::LOGICAL_ERROR);
}

template class DatabaseMaterializeMySQL<DatabaseOrdinary>;
template class DatabaseMaterializeMySQL<DatabaseAtomic>;

}

#endif
