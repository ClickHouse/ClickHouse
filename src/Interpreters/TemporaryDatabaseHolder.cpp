#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/loadMetadata.h>
#include <Storages/IStorage.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseOnDisk.h>
#include <Disks/IDisk.h>
#include <Common/quoteString.h>
#include <Access/Common/AccessFlags.h>
#include <Storages/StorageMemory.h>
#include <Storages/LiveView/TemporaryLiveViewCleaner.h>
#include <Core/BackgroundSchedulePool.h>
#include <Parsers/formatAST.h>
#include <IO/ReadHelpers.h>
#include <Poco/DirectoryIterator.h>
#include <Common/atomicRename.h>
#include <Common/CurrentMetrics.h>
#include <Common/logger_useful.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/filesystemHelpers.h>
#include <Common/noexcept_scope.h>
#include <Common/checkStackSize.h>
#include <Interpreters/TemporaryDatabaseHolder.h>
#include <Parsers/ASTDropQuery.h>

namespace DB
{


void executeToTableImpl(ContextPtr context_, ASTDropQuery & query, DatabasePtr & db, UUID & uuid_to_wait)
{
    /// NOTE: it does not contain UUID, we will resolve it with locked DDLGuard
    auto table_id = StorageID(query);

    auto ddl_guard = (!query.no_ddl_lock ? DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name) : nullptr);

    /// If table was already dropped by anyone, an exception will be thrown
    auto [database, table] = query.if_exists ? DatabaseCatalog::instance().tryGetDatabaseAndTable(table_id, context_)
                                             : DatabaseCatalog::instance().getDatabaseAndTable(table_id, context_);

    if (database && table)
    {
        /// Now get UUID, so we can wait for table data to be finally dropped
        table_id.uuid = database->tryGetTableUUID(table_id.table_name);

        /// Prevents recursive drop from drop database query. The original query must specify a table.
        bool is_drop_or_detach_database = true;

        AccessFlags drop_storage;

        if (table->isView())
            drop_storage = AccessType::DROP_VIEW;
        else if (table->isDictionary())
            drop_storage = AccessType::DROP_DICTIONARY;
        else
            drop_storage = AccessType::DROP_TABLE;

        if (query.kind == ASTDropQuery::Kind::Drop)
        {
            if (table->isDictionary())
            {
                /// If DROP DICTIONARY query is not used, check if Dictionary can be dropped with DROP TABLE query
                if (!query.is_dictionary)
                    table->checkTableCanBeDropped();
            }
            else
                table->checkTableCanBeDropped();

            table->flushAndShutdown();

            TableExclusiveLockHolder table_lock;
            if (database->getUUID() == UUIDHelpers::Nil)
                table_lock = table->lockExclusively(context_->getCurrentQueryId(), context_->getSettingsRef().lock_acquire_timeout);
            DatabaseCatalog::instance().tryRemoveLoadingDependencies(table_id, context_->getSettingsRef().check_table_dependencies,
                                                                     is_drop_or_detach_database);
            database->dropTable(context_, table_id.table_name, query.sync);
        }

        db = database;
        uuid_to_wait = table_id.uuid;
    }

}


TemporaryDatabaseHolder::TemporaryDatabaseHolder(ContextPtr context_, String temp_name, String global_name)
    : WithContext(context_->getGlobalContext()), temporary_database_name(temp_name), global_database_name(global_name)
{
}


TemporaryDatabaseHolder::TemporaryDatabaseHolder(TemporaryDatabaseHolder && rhs) noexcept
        : WithContext(rhs.context), temporary_database_name(rhs.temporary_database_name), global_database_name(rhs.global_database_name)
{
    rhs.temporary_database_name = "";
    rhs.global_database_name = "";
}

TemporaryDatabaseHolder & TemporaryDatabaseHolder::operator=(TemporaryDatabaseHolder && rhs) noexcept
{
    global_database_name = rhs.global_database_name;
    temporary_database_name = rhs.temporary_database_name;
    rhs.global_database_name = "";
    rhs.temporary_database_name = "";
    return *this;
}

TemporaryDatabaseHolder::~TemporaryDatabaseHolder()
{
    if (global_database_name == "")
        return;
    const auto & database_name = global_database_name;
    auto ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, "");

    auto database = DatabaseCatalog::instance().tryGetDatabase(database_name);
    if (database)
    {
        if (database->shouldBeEmptyOnDetach())
        {
            ASTDropQuery query_for_table;
            query_for_table.kind = ASTDropQuery::Kind::Drop;
            query_for_table.if_exists = true;
            query_for_table.setDatabase(database_name);
            query_for_table.sync = false;

            /// Flush should not be done if shouldBeEmptyOnDetach() == false,
            /// since in this case getTablesIterator() may do some additional work,
            /// see DatabaseMaterializedMySQL::getTablesIterator()
            for (auto iterator = database->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
            {
                iterator->table()->flush();
            }

            auto table_context = Context::createCopy(getContext());
            table_context->setInternalQuery(true);
            for (auto iterator = database->getTablesIterator(table_context); iterator->isValid(); iterator->next())
            {
                DatabasePtr db;
                UUID table_to_wait = UUIDHelpers::Nil;
                query_for_table.setTable(iterator->name());
                query_for_table.is_dictionary = iterator->table()->isDictionary();
                executeToTableImpl(table_context, query_for_table, db, table_to_wait);
            }
        }

        /// Protects from concurrent CREATE TABLE queries
        auto db_guard = DatabaseCatalog::instance().getExclusiveDDLGuardForDatabase(database_name);


        /// DETACH or DROP database itself
        DatabaseCatalog::instance().detachDatabase(getContext(), database_name, true /*drop*/, database->shouldBeEmptyOnDetach());
    }
}
}
