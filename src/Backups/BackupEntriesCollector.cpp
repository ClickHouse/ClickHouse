#include <Backups/BackupEntriesCollector.h>
#include <Backups/IBackupCoordination.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Databases/IDatabase.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Common/escapeForFileName.h>
#include <base/insertAtEnd.h>
#include <unordered_set>


namespace DB
{
 
namespace ErrorCodes
{
    extern const int BACKUP_IS_EMPTY;
}


std::shared_ptr<BackupEntriesCollector> BackupEntriesCollector::create(
    const ASTBackupQuery::Elements & backup_query_elements_,
    const BackupSettings & backup_settings_,
    std::shared_ptr<IBackupCoordination> backup_coordination_,
    const ContextPtr & context_,
    std::chrono::seconds timeout_)
{
    return std::shared_ptr<BackupEntriesCollector>(
        new BackupEntriesCollector(backup_query_elements_, backup_settings_, backup_coordination_, context_, timeout_));
}

BackupEntriesCollector::BackupEntriesCollector(
    const ASTBackupQuery::Elements & backup_query_elements_,
    const BackupSettings & backup_settings_,
    std::shared_ptr<IBackupCoordination> backup_coordination_,
    const ContextPtr & context_,
    std::chrono::seconds timeout_)
    : backup_query_elements(backup_query_elements_)
    , backup_settings(backup_settings_)
    , backup_coordination(backup_coordination_)
    , context(context_)
    , timeout(timeout_)
{
}

BackupEntriesCollector::~BackupEntriesCollector() = default;

BackupEntries BackupEntriesCollector::collectBackupEntries()
{
    /// collectBackupEntries() must not be called again!
    assert(!collecting_backup_entries.exchange(true));

    try
    {
        /// Calculate the root path for collecting backup entries, it's either empty or has the format "shards/<shard_num>/replicas/<replica_num>/".
        calculateRootPathInBackup();

        /// Do renaming in the create queries according to the renaming config.
        renaming_settings.setFromBackupQuery(backup_query_elements);

        /// Find all the databases which we will put to the backup.
        prepareDatabaseInfos();

        /// Call IDatabase::backup() for all the databases found.
        backupDatabases();
    }
    catch (...)
    {
        backup_coordination->finishCollectingBackupEntries(backup_settings.host_id, getCurrentExceptionMessage(false));
        throw;
    }

    backup_coordination->finishCollectingBackupEntries(backup_settings.host_id);

    if (!post_collecting_tasks.empty())
    {
        /// We've finished collecting backup entries, now we must wait for other replicas and shards to do that too.
        /// We must do that waiting because post collecting tasks require data collected on other hosts.
        backup_coordination->waitForAllHostsCollectedBackupEntries(
            BackupSettings::Util::filterHostIDs(
                backup_settings.cluster_host_ids, backup_settings.shard_num, backup_settings.replica_num),
            timeout);

        /// Run all the tasks added with addPostCollectingTask().
        runPostCollectingTasks();
    }

    /// No more backup entries or tasks are allowed at this point!
    assert(allow_adding_entries_or_tasks.exchange(false));

    /// A backup cannot be empty.
    if (backup_entries.empty())
        throw Exception("Backup must not be empty", ErrorCodes::BACKUP_IS_EMPTY);

    return std::move(backup_entries);
}

/// Calculates the root path for collecting backup entries,
/// it's either empty or has the format "shards/<shard_num>/replicas/<replica_num>/".
void BackupEntriesCollector::calculateRootPathInBackup()
{
    root_path_in_backup.clear();
    if (backup_settings.host_id.empty())
        return;

    auto [shard_index, replica_index]
        = BackupSettings::Util::findShardNumAndReplicaNum(backup_settings.cluster_host_ids, backup_settings.host_id);
    if (shard_index || replica_index)
        root_path_in_backup = fmt::format("shards/{}/replicas/{}/", shard_index, replica_index);
}

/// Finds all the databases which we will put to the backup.
void BackupEntriesCollector::prepareDatabaseInfos()
{
    database_infos.clear();
    for (const auto & element : backup_query_elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::ElementType::TABLE:
            {
                const String & database_name = element.name.first;
                const String & table_name = element.name.second;
                auto it = database_infos.find(database_name);

                DatabasePtr database;
                if (it != database_infos.end())
                    database = it->second.database;
                if (!database)
                    database = DatabaseCatalog::instance().getDatabase(database_name);

                if (it == database_infos.end())
                    it = database_infos.emplace(database_name, DatabaseInfo{}).first;
                
                auto & database_info = it->second;
                database_info.database = database;

                database_info.except_table_names.erase(table_name);
                if (!database_info.all_tables)
                    database_info.table_names.insert(table_name);
                if (element.partitions)
                    insertAtEnd(database_info.partitions[table_name], *element.partitions);
                break;
            }

            case ASTBackupQuery::ElementType::DATABASE:
            {
                const String & database_name = element.name.first;
                auto it = database_infos.find(database_name);

                DatabasePtr database;
                if (it != database_infos.end())
                    database = it->second.database;
                if (!database)
                    database = DatabaseCatalog::instance().getDatabase(database_name);

                ASTPtr create_database_query;
                if (it != database_infos.end())
                    create_database_query = it->second.create_database_query;
                if (!create_database_query)
                    create_database_query = database->getCreateDatabaseQuery();

                if (it == database_infos.end())
                    it = database_infos.emplace(database_name, DatabaseInfo{}).first;

                auto & database_info = it->second;
                database_info.database = database;
                database_info.create_database_query = create_database_query;

                if (database_info.all_tables)
                {
                    std::erase_if(
                        database_info.except_table_names,
                        [&](const String & table_name) { return !element.except_list.contains(table_name); });
                }
                else
                {
                    database_info.all_tables = true;
                    database_info.except_table_names = std::unordered_set<String>(element.except_list.begin(), element.except_list.end());
                    for (const auto & table_name : database_info.table_names)
                        database_info.except_table_names.erase(table_name);
                    database_info.table_names.clear();
                }
                break;
            }

            case ASTBackupQuery::ElementType::ALL_DATABASES:
            {
                for (const auto & [database_name, database] : DatabaseCatalog::instance().getDatabases())
                {
                    if (element.except_list.contains(database_name))
                        continue;

                    auto it = database_infos.find(database_name);

                    ASTPtr create_database_query;
                    if (it != database_infos.end())
                        create_database_query = it->second.create_database_query;
                    if (!create_database_query)
                        create_database_query = database->getCreateDatabaseQuery(); // TODO: Replace with tryGetCreateDatabaseQuery()
                    if (!create_database_query)
                        continue; /// A databases has just been dropped, we silently skip it.

                    if (it == database_infos.end())
                        it = database_infos.emplace(database_name, DatabaseInfo{}).first;

                    auto & database_info = it->second;
                    database_info.create_database_query = create_database_query;
                    database_info.database = database;
                    database_info.all_tables = true;
                    database_info.table_names.clear();
                    database_info.except_table_names.clear();
                }
                break;
            }
        }
    }
}

/// Calls IDatabase::backup() for all the databases found.
void BackupEntriesCollector::backupDatabases()
{
    backup_entries.clear();
    assert(!allow_adding_entries_or_tasks.exchange(true));

    for (const auto & database_info : database_infos | boost::adaptors::map_values)
    {
        auto database = database_info.database;
        database->backup(
            database_info.create_database_query,
            database_info.table_names,
            database_info.all_tables,
            database_info.except_table_names,
            database_info.partitions,
            shared_from_this());
    }
}

void BackupEntriesCollector::addBackupEntry(const String & file_name, BackupEntryPtr backup_entry)
{
    std::lock_guard lock{mutex};
    assert(allow_adding_entries_or_tasks);
    backup_entries.emplace_back(file_name, backup_entry);
}

void BackupEntriesCollector::addBackupEntries(const BackupEntries & backup_entries_)
{
    std::lock_guard lock{mutex};
    assert(allow_adding_entries_or_tasks);
    insertAtEnd(backup_entries, backup_entries_);
}

void BackupEntriesCollector::addBackupEntries(BackupEntries && backup_entries_)
{
    std::lock_guard lock{mutex};
    assert(allow_adding_entries_or_tasks);
    insertAtEnd(backup_entries, std::move(backup_entries_));
}
    
void BackupEntriesCollector::addBackupEntryForCreateQuery(const ASTPtr & create_query)
{
    std::lock_guard lock{mutex};
    assert(allow_adding_entries_or_tasks);
    
    ASTPtr new_create_query = create_query;
    renameInCreateQuery(new_create_query, context, renaming_settings);

    const auto & create = new_create_query->as<const ASTCreateQuery &>();
    String new_table_name = create.getTable();
    String new_database_name = create.getDatabase();

    String metadata_path_in_backup;
    if (new_table_name.empty())
    {
        /// CREATE DATABASE
        metadata_path_in_backup = root_path_in_backup + "metadata/" + escapeForFileName(new_database_name) + ".sql";
    }
    else
    {
        /// CREATE TABLE
        metadata_path_in_backup = root_path_in_backup + "metadata/" + escapeForFileName(new_database_name) + "/" + escapeForFileName(new_table_name) + ".sql";
    }

    backup_entries.emplace_back(metadata_path_in_backup, std::make_shared<BackupEntryFromMemory>(serializeAST(*create_query)));
}

String BackupEntriesCollector::getDataPathInBackup(const ASTPtr & create_query) const
{
    const auto & create = create_query->as<const ASTCreateQuery &>();
    String table_name = create.getTable();
    String database_name = create.getDatabase();
    std::lock_guard lock{mutex};
    auto [new_database_name, new_table_name] = renaming_settings.getNewTableName(DatabaseAndTableName{database_name, table_name});
    return root_path_in_backup + "data/" + escapeForFileName(new_database_name) + "/" + escapeForFileName(new_table_name);
}

void BackupEntriesCollector::addPostCollectingTask(std::function<void()> task)
{
    std::lock_guard lock{mutex};
    assert(allow_adding_entries_or_tasks);
    post_collecting_tasks.push(std::move(task));
}

/// Runs all the tasks added with addPostCollectingTask().
void BackupEntriesCollector::runPostCollectingTasks()
{
    /// Post collecting tasks can add other post collecting tasks, our code is fine with that.
    std::unique_lock lock{mutex};
    while (!post_collecting_tasks.empty())
    {
        auto task = std::move(post_collecting_tasks.front());
        post_collecting_tasks.pop();
        lock.unlock();
        std::move(task)();
        lock.lock();
    }
}

}
