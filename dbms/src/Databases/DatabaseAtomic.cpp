#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseOnDisk.h>
#include <Poco/File.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int FILE_DOESNT_EXIST;
}

DatabaseAtomic::DatabaseAtomic(String name_, String metadata_path_, Context & context_)
    : DatabaseOrdinary(name_, metadata_path_, context_)
{
    data_path = "store/";
    auto log_name = "DatabaseAtomic (" + name_ + ")";
    log = &Logger::get(log_name);
    drop_task = context_.getSchedulePool().createTask(log_name, [this](){ this->dropTableDataTask(); });
}

String DatabaseAtomic::getTableDataPath(const String & table_name) const
{
    std::lock_guard lock(mutex);
    auto it = table_name_to_path.find(table_name);
    if (it == table_name_to_path.end())
        throw Exception("Table " + table_name + " not found in database " + getDatabaseName(), ErrorCodes::UNKNOWN_TABLE);
    assert(it->second != data_path && !it->second.empty());
    return it->second;
}

String DatabaseAtomic::getTableDataPath(const ASTCreateQuery & query) const
{
    //stringToUUID(query.uuid);   /// Check UUID is valid
    const size_t uuid_prefix_len = 3;
    auto tmp = data_path + toString(query.uuid).substr(0, uuid_prefix_len) + '/' + toString(query.uuid) + '/';
    assert(tmp != data_path && !tmp.empty());
    return tmp;

}

void DatabaseAtomic::drop(const Context &)
{
    Poco::File(getMetadataPath()).remove(false);
    //TODO check all tables dropped
}

void DatabaseAtomic::attachTable(const String & name, const StoragePtr & table, const String & relative_table_path)
{
    assert(relative_table_path != data_path && !relative_table_path.empty());
    DatabaseWithDictionaries::attachTable(name, table, relative_table_path);
    std::lock_guard lock(mutex);
    table_name_to_path.emplace(std::make_pair(name, relative_table_path));
}

StoragePtr DatabaseAtomic::detachTable(const String & name)
{
    {
        std::lock_guard lock(mutex);
        table_name_to_path.erase(name);
    }
    return DatabaseWithDictionaries::detachTable(name);
}

void DatabaseAtomic::dropTable(const Context & context, const String & table_name)
{
    String table_metadata_path = getObjectMetadataPath(table_name);
    String table_metadata_path_drop = table_metadata_path + drop_suffix;
    String table_data_path_relative = getTableDataPath(table_name);
    assert(!table_data_path_relative.empty());

    StoragePtr table = detachTable(table_name);
    try
    {
        // FIXME
        // 1. CREATE table_name: + table_name.sql
        // 2. DROP table_name: table_name.sql -> table_name.sql.tmp_drop
        // 3. CREATE table_name: + table_name.sql
        // 4. DROP table_name: table_name.sql -> table_name.sql.tmp_drop overwrites table_name.sql.tmp_drop ?
        Poco::File(table_metadata_path).renameTo(table_metadata_path_drop);

        {
            LOG_INFO(log, "Mark table " + table->getStorageID().getNameForLogs() + " to drop.");
            std::lock_guard lock(tables_to_drop_mutex);
            time_t current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            tables_to_drop.push_back({table, context.getPath() + table_data_path_relative, current_time});
        }
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessage(__PRETTY_FUNCTION__));
        attachTable(table_name, table, table_data_path_relative);
        Poco::File(table_metadata_path_drop).renameTo(table_metadata_path);
        throw;
    }
}

void DatabaseAtomic::renameTable(const Context & context, const String & table_name, IDatabase & to_database,
                                 const String & to_table_name)
{
    if (typeid(*this) != typeid(to_database))
    {
        if (!typeid_cast<DatabaseOrdinary *>(&to_database))
            throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);
        /// Allow moving tables from Atomic to Ordinary (with table lock)
        DatabaseOnDisk::renameTable(context, table_name, to_database, to_table_name);
        return;
    }

    StoragePtr table = tryGetTable(context, table_name);

    if (!table)
        throw Exception("Table " + backQuote(getDatabaseName()) + "." + backQuote(table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    /// Update database and table name in memory without moving any data on disk
    table->renameInMemory(to_database.getDatabaseName(), to_table_name);

    /// NOTE Non-atomic.
    to_database.attachTable(to_table_name, table, getTableDataPath(table_name));
    detachTable(table_name);
    Poco::File(getObjectMetadataPath(table_name)).renameTo(to_database.getObjectMetadataPath(to_table_name));
}

void DatabaseAtomic::loadStoredObjects(Context & context, bool has_force_restore_data_flag)
{
    DatabaseOrdinary::loadStoredObjects(context, has_force_restore_data_flag);
    drop_task->activateAndSchedule();
}

void DatabaseAtomic::shutdown()
{
    drop_task->deactivate();
    DatabaseWithDictionaries::shutdown();
    //TODO try drop tables
}

void DatabaseAtomic::dropTableDataTask()
{
    LOG_INFO(log, String("Wake up ") + __PRETTY_FUNCTION__);
    TableToDrop table;
    try
    {
        std::lock_guard lock(tables_to_drop_mutex);
        LOG_INFO(log, "There are " + std::to_string(tables_to_drop.size()) + " tables to drop");
        time_t current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        auto it = std::find_if(tables_to_drop.begin(), tables_to_drop.end(), [current_time, this](const TableToDrop & elem)
        {
            LOG_INFO(log, "Check table " + elem.table->getStorageID().getNameForLogs() + ": " +
                      "refcount = " + std::to_string(elem.table.unique()) + ", " +
                      "time elapsed = " + std::to_string(current_time - elem.drop_time));
            return elem.table.unique() && elem.drop_time + drop_delay_s < current_time;
        });
        if (it != tables_to_drop.end())
        {
            table = std::move(*it);
            LOG_INFO(log, "Will try drop " + table.table->getStorageID().getNameForLogs());
            tables_to_drop.erase(it);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    if (table.table)
    {
        try
        {
            LOG_INFO(log, "Trying to drop table " + table.table->getStorageID().getNameForLogs());
            table.table->drop();
            table.table->is_dropped = true;
            Poco::File table_data_dir{table.data_path};
            if (table_data_dir.exists())
                table_data_dir.remove(true);

            String metadata_tmp_drop = getObjectMetadataPath(table.table->getStorageID().getTableName()) + drop_suffix;
            Poco::File(metadata_tmp_drop).remove();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot drop table " + table.table->getStorageID().getNameForLogs() +
                                        ". Will retry later.");
            {
                std::lock_guard lock(tables_to_drop_mutex);
                tables_to_drop.emplace_back(std::move(table));
            }
        }
    }

    drop_task->scheduleAfter(reschedule_time_ms);
}


}

