#include <Backups/BackupUtils.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupSettings.h>
#include <Backups/DDLCompareUtils.h>
#include <Backups/DDLRenamingVisitor.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupCoordination.h>
#include <Backups/replaceTableUUIDWithMacroInReplicatedTableDef.h>
#include <Common/escapeForFileName.h>
#include <Access/Common/AccessRightsElement.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/formatAST.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <base/sleep.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_BACKUP_TABLE;
    extern const int CANNOT_BACKUP_DATABASE;
    extern const int BACKUP_IS_EMPTY;
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// Helper to calculate paths inside a backup.
    class PathsInBackup
    {
    public:
        /// Returns the path to metadata in backup.
        static String getMetadataPath(const DatabaseAndTableName & table_name, size_t shard_index, size_t replica_index)
        {
            if (table_name.first.empty() || table_name.second.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Database name and table name must not be empty");
            return getPathForShardAndReplica(shard_index, replica_index) + String{"metadata/"} + escapeForFileName(table_name.first) + "/"
                + escapeForFileName(table_name.second) + ".sql";
        }

        static String getMetadataPath(const String & database_name, size_t shard_index, size_t replica_index)
        {
            if (database_name.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Database name must not be empty");
            return getPathForShardAndReplica(shard_index, replica_index) + String{"metadata/"} + escapeForFileName(database_name) + ".sql";
        }

        /// Returns the path to table's data in backup.
        static String getDataPath(const DatabaseAndTableName & table_name, size_t shard_index, size_t replica_index)
        {
            if (table_name.first.empty() || table_name.second.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Database name and table name must not be empty");
            assert(!table_name.first.empty() && !table_name.second.empty());
            return getPathForShardAndReplica(shard_index, replica_index) + String{"data/"} + escapeForFileName(table_name.first) + "/"
                + escapeForFileName(table_name.second) + "/";
        }

    private:
        static String getPathForShardAndReplica(size_t shard_index, size_t replica_index)
        {
            if (shard_index || replica_index)
                return fmt::format("shards/{}/replicas/{}/", shard_index, replica_index);
            else
                return "";
        }
    };

    using Kind = ASTBackupQuery::Kind;
    using Element = ASTBackupQuery::Element;
    using Elements = ASTBackupQuery::Elements;
    using ElementType = ASTBackupQuery::ElementType;

    /// Makes backup entries to backup databases and tables according to the elements of ASTBackupQuery.
    /// Keep this class consistent with RestoreTasksBuilder.
    class BackupEntriesBuilder
    {
    public:
        BackupEntriesBuilder(const ContextPtr & context_, const BackupSettings & backup_settings_, std::shared_ptr<IBackupCoordination> backup_coordination_)
            : context(context_), backup_settings(backup_settings_), backup_coordination(backup_coordination_)
        {
        }

        /// Prepares internal structures for making backup entries.
        void prepare(const ASTBackupQuery::Elements & elements, std::chrono::seconds timeout)
        {
            try
            {
                calculateShardNumAndReplicaNumInBackup();
                renaming_settings.setFromBackupQuery(elements);
                collectDatabasesAndTables(elements, timeout);
                collectTablesData();
                releaseTables();
            }
            catch (...)
            {
                backup_coordination->finishPreparing(backup_settings.host_id, getCurrentExceptionMessage(false));
                throw;
            }

            /// We've finished restoring metadata, now we will wait for other replicas and shards to finish too.
            /// We need this waiting because we're going to call some functions which requires data collected from other nodes too,
            /// see IRestoreCoordination::checkTablesNotExistedInReplicatedDBs(), IRestoreCoordination::getReplicatedTableDataPath().
            backup_coordination->finishPreparing(backup_settings.host_id);

            backup_coordination->waitForAllHostsPrepared(
                BackupSettings::Util::filterHostIDs(
                    backup_settings.cluster_host_ids, backup_settings.shard_num, backup_settings.replica_num),
                timeout);
        }

        /// Makes backup entries, should be called after prepare().
        BackupEntries makeBackupEntries() &&
        {
            renameAndAdjustCreateQueries();

            BackupEntries res;
            for (const auto & [db_name_in_backup, db_info] : databases)
                res.push_back(makeBackupEntryFromMetadata(db_name_in_backup, *db_info.create_query));

            for (const auto & [table_name_in_backup, table_info] : tables)
            {
                res.push_back(makeBackupEntryFromMetadata(table_name_in_backup, *table_info.create_query));
                appendBackupEntriesWithData(res, table_info);
            }

            /// A backup cannot be empty.
            if (res.empty())
                throw Exception("Backup must not be empty", ErrorCodes::BACKUP_IS_EMPTY);

            return res;
        }

    private:
        void calculateShardNumAndReplicaNumInBackup()
        {
            size_t shard_num = 0;
            size_t replica_num = 0;
            if (!backup_settings.host_id.empty())
            {
                std::tie(shard_num, replica_num)
                    = BackupSettings::Util::findShardNumAndReplicaNum(backup_settings.cluster_host_ids, backup_settings.host_id);
            }
            shard_num_in_backup = shard_num;
            replica_num_in_backup = replica_num;
        }

        /// Collects information about databases and tables which will be in the backup.
        void collectDatabasesAndTables(const ASTBackupQuery::Elements & elements, std::chrono::seconds timeout)
        {
            /// Tables can be dropped or renamed while we're collecting them.
            /// That's why we will sometimes have to do that in multiple passes.
            bool use_timeout = timeout.count() >= 0;
            auto start_time = std::chrono::steady_clock::now();
            bool ok = false;
            while (!use_timeout || (std::chrono::steady_clock::now() - start_time < timeout))
            {
                ok = tryCollectDatabasesAndTables(elements, /* throw_if_not_locked_or_renamed= */ false);
                if (ok)
                    break;
                sleepForMilliseconds(50);
            }

            if (!ok)
                tryCollectDatabasesAndTables(elements, /* throw_if_not_locked_or_renamed= */ true);
        }

        bool tryCollectDatabasesAndTables(const ASTBackupQuery::Elements & elements, bool throw_if_not_locked_or_renamed)
        {
            bool ok = true;

            for (const auto & element : elements)
            {
                switch (element.type)
                {
                    case ElementType::TABLE:
                    {
                        ok &= tryCollectTable(element.name, element.partitions, throw_if_not_locked_or_renamed);
                        break;
                    }

                    case ElementType::DATABASE:
                    {
                        const String & database_name = element.name.first;
                        ok &= tryCollectDatabase(database_name, element.except_list, throw_if_not_locked_or_renamed);
                        break;
                    }

                    case ElementType::ALL_DATABASES:
                    {
                        ok &= tryCollectAllDatabases(element.except_list, throw_if_not_locked_or_renamed);
                        break;
                    }
                }

                if (!ok)
                    break;
            }

            if (!ok)
            {
                databases.clear();
                tables.clear();
            }

            return ok;
        }

        bool tryCollectTable(const DatabaseAndTableName & table_name, const ASTs & partitions, bool throw_if_not_locked_or_renamed)
        {
            auto database_and_table = DatabaseCatalog::instance().getDatabaseAndTable({table_name.first, table_name.second}, context);
            return tryCollectTable(table_name, database_and_table, partitions, throw_if_not_locked_or_renamed);
        }

        bool tryCollectTable(const DatabaseAndTableName & table_name, const DatabaseAndTable & database_and_table, const ASTs & partitions, bool throw_if_not_locked_or_renamed)
        {
            const auto & database = database_and_table.first;
            const auto & storage = database_and_table.second;
            
            auto table_lock = storage->tryLockForShare(context->getInitialQueryId(), context->getSettingsRef().lock_acquire_timeout);
            auto table_id = storage->getStorageID();

            if (!table_lock)
            {
                if (throw_if_not_locked_or_renamed)
                    throw Exception(ErrorCodes::CANNOT_BACKUP_TABLE, "Couldn't lock the table {} for backup", table_id.getFullTableName());
                return false;
            }

            if ((table_id.database_name != table_name.first) || (table_id.table_name != table_name.second))
            {
                if (throw_if_not_locked_or_renamed)
                    throw Exception(ErrorCodes::CANNOT_BACKUP_TABLE, "Table {}.{} suddenly changed its name to {}",
                                    backQuoteIfNeed(table_name.first), backQuoteIfNeed(table_name.second), table_id.getFullTableName());
                return false;
            }

            auto create_query = typeid_cast<std::shared_ptr<ASTCreateQuery>>(database->tryGetCreateTableQuery(table_name.second, context));
            if (!create_query || (create_query->getDatabase() != table_name.first) || (create_query->getTable() != table_name.second))
            {
                if (throw_if_not_locked_or_renamed)
                    throw Exception(ErrorCodes::CANNOT_BACKUP_TABLE, "Couldn't get a create query for the table {}", table_id.getFullTableName());
                return false;
            }
            
            if (!database->hasTablesToBackup())
                throw Exception(
                    ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Cannot backup the table {} because it's contained in a hollow database (engine: {})",
                    table_id.getFullTableName(),
                    database->getEngineName());

            /// Check that we are not trying to backup the same table again.
            DatabaseAndTableName name_in_backup
                = renaming_settings.getNewTableName(DatabaseAndTableName{table_id.database_name, table_id.table_name});
            if (tables.contains(name_in_backup))
                throw Exception(
                    ErrorCodes::CANNOT_BACKUP_TABLE, "Cannot backup the table {}.{} twice", backQuoteIfNeed(name_in_backup.first), backQuoteIfNeed(name_in_backup.second));

            TableInfo & table_info = tables.emplace(name_in_backup, table_id).first->second;
            table_info.storage = storage;
            table_info.create_query = create_query;
            table_info.partitions = partitions;
            table_info.table_lock = std::move(table_lock);
            return true;
        }

        bool tryCollectDatabase(const String & database_name_, const std::set<String> & except_list_, bool throw_if_not_locked_or_renamed)
        {
            auto database = DatabaseCatalog::instance().getDatabase(database_name_, context);
            return tryCollectDatabase(database_name_, database, except_list_, throw_if_not_locked_or_renamed);
        }

        bool tryCollectDatabase(const String & database_name_, const DatabasePtr & database_, const std::set<String> & except_list_, bool throw_if_not_locked_or_renamed)
        {
            auto create_query = typeid_cast<std::shared_ptr<ASTCreateQuery>>(database_->getCreateDatabaseQuery());
            if (!create_query)
                throw Exception(ErrorCodes::CANNOT_BACKUP_DATABASE, "Couldn't get a create query for database {}", backQuoteIfNeed(database_name_));

            if ((create_query->getDatabase() != database_name_))
            {
                if (throw_if_not_locked_or_renamed)
                    throw Exception(ErrorCodes::CANNOT_BACKUP_TABLE, "Database {} suddenly changed its name to {}",
                                    backQuoteIfNeed(database_name_), backQuoteIfNeed(create_query->getDatabase()));
                return false;
            }

            /// Check that we are not trying to restore the same database again.
            String name_in_backup = renaming_settings.getNewDatabaseName(database_name_);
            if (databases.contains(name_in_backup))
                throw Exception(ErrorCodes::CANNOT_BACKUP_DATABASE, "Cannot backup the database {} twice", backQuoteIfNeed(name_in_backup));

            DatabaseInfo & db_info = databases.emplace(name_in_backup, DatabaseInfo{}).first->second;
            db_info.create_query = create_query;

            /// Backup tables in this database.
            if (database_->hasTablesToBackup())
            {
                for (auto it = database_->getTablesIterator(context); it->isValid(); it->next())
                {
                    if (except_list_.contains(it->name()))
                        continue;
                    if (!tryCollectTable({database_name_, it->name()}, {database_, it->table()}, {}, throw_if_not_locked_or_renamed))
                        return false;
                }
            }

            return true;
        }

        bool tryCollectAllDatabases(const std::set<String> & except_list_, bool throw_if_not_locked_or_renamed)
        {
            for (const auto & [database_name, database] : DatabaseCatalog::instance().getDatabases())
            {
                if (except_list_.contains(database_name))
                    continue;
                if (!tryCollectDatabase(database_name, database, {}, throw_if_not_locked_or_renamed))
                    return false;
            }
            return true;
        }

        /// Collects the data of all tables. It means we will make temporary hard links for all the data files.
        void collectTablesData()
        {
            if (backup_settings.structure_only)
                return;

            for (auto & [name_in_backup, table_info] : tables)
            {
                auto storage = table_info.storage;
                if (!storage->hasDataToBackup())
                    continue;

                const auto & table_id = table_info.table_id;
                auto data = storage->backupData(context, table_info.partitions, backup_settings, backup_coordination);
                String data_path = PathsInBackup::getDataPath(name_in_backup, shard_num_in_backup, replica_num_in_backup);

                bool has_replicated_parts = backup_coordination->hasReplicatedPartNames(backup_settings.host_id, table_id);
                if (has_replicated_parts)
                    backup_coordination->addReplicatedTableDataPath(backup_settings.host_id, table_id, data_path);

                table_info.data = std::move(data);
                table_info.data_path = std::move(data_path);
                table_info.has_replicated_parts = has_replicated_parts;

                table_info.table_lock.reset();
                table_info.storage.reset();
            }
        }

        /// Releases table locks because we can continue making the backup without them.
        void releaseTables()
        {
            for (auto & table_info : tables | boost::adaptors::map_values)
            {
                table_info.table_lock.reset();
                table_info.storage.reset();
            }
        }

        /// Do renaming in the create queries according to the renaming config.
        void renameAndAdjustCreateQueries()
        {
            for (auto & info : databases | boost::adaptors::map_values)
                info.create_query = renameAndAdjustCreateQuery(info.create_query);

            for (auto & info : tables | boost::adaptors::map_values)
                info.create_query = renameAndAdjustCreateQuery(info.create_query);
        }

        std::shared_ptr<ASTCreateQuery> renameAndAdjustCreateQuery(const ASTPtr & ast) const
        {
            ASTPtr query = ast;
            renameInCreateQuery(query, context, renaming_settings);
            auto create_query = typeid_cast<std::shared_ptr<ASTCreateQuery>>(query);
            replaceTableUUIDWithMacroInReplicatedTableDef(*create_query, create_query->uuid);
            create_query->uuid = UUIDHelpers::Nil;
            create_query->to_inner_uuid = UUIDHelpers::Nil;
            return create_query;
        }

        /// Makes a backup entry to backup table's metadata.
        std::pair<String, BackupEntryPtr> makeBackupEntryFromMetadata(const String & database_name, const IAST & create_query) const
        {
            auto metadata_entry = std::make_unique<BackupEntryFromMemory>(serializeAST(create_query));
            String metadata_path = PathsInBackup::getMetadataPath(database_name, shard_num_in_backup, replica_num_in_backup);
            return {metadata_path, std::move(metadata_entry)};
        }

        /// Makes a backup entry to backup database's metadata.
        std::pair<String, BackupEntryPtr> makeBackupEntryFromMetadata(const DatabaseAndTableName & table_name, const IAST & create_query) const
        {
            auto metadata_entry = std::make_unique<BackupEntryFromMemory>(serializeAST(create_query));
            String metadata_path = PathsInBackup::getMetadataPath(table_name, shard_num_in_backup, replica_num_in_backup);
            return {metadata_path, std::move(metadata_entry)};
        }

        struct TableInfo;

        /// Appends backup entries with tables' data to the list.
        void appendBackupEntriesWithData(BackupEntries & res, const TableInfo & info) const
        {
            if (!info.has_replicated_parts)
            {
                for (const auto & [relative_path, backup_entry] : info.data)
                    res.emplace_back(info.data_path + relative_path, backup_entry);
                return;
            }

            Strings data_paths = backup_coordination->getReplicatedTableDataPaths(backup_settings.host_id, info.table_id);
            Strings part_names = backup_coordination->getReplicatedPartNames(backup_settings.host_id, info.table_id);
            std::unordered_set<std::string_view> part_names_set{part_names.begin(), part_names.end()};
            for (const auto & [relative_path, backup_entry] : info.data)
            {
                size_t slash_pos = relative_path.find('/');
                if (slash_pos != String::npos)
                {
                    String part_name = relative_path.substr(0, slash_pos);
                    if (MergeTreePartInfo::tryParsePartName(part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING))
                    {
                        if (!part_names_set.contains(part_name))
                            continue;
                        for (const auto & data_path : data_paths)
                            res.emplace_back(data_path + relative_path, backup_entry);
                        continue;
                    }
                }
                res.emplace_back(info.data_path + relative_path, backup_entry);
            }
        }

        /// Information which is used to backup the definition and the data of a table.
        struct TableInfo
        {
            explicit TableInfo(const StorageID & table_id_) : table_id(table_id_) {}
            StorageID table_id;
            std::shared_ptr<ASTCreateQuery> create_query;
            StoragePtr storage;
            TableLockHolder table_lock;
            ASTs partitions;
            BackupEntries data;
            String data_path;
            bool has_replicated_parts = false;
        };

        /// Information which is used to backup the definition of a database.
        struct DatabaseInfo
        {
            ASTPtr create_query;
        };

        ContextPtr context;
        BackupSettings backup_settings;
        std::shared_ptr<IBackupCoordination> backup_coordination;
        size_t shard_num_in_backup = 0;
        size_t replica_num_in_backup = 0;
        DDLRenamingSettings renaming_settings;
        std::unordered_map<String /* db_name_in_backup */, DatabaseInfo> databases;
        std::map<DatabaseAndTableName /* table_name_in_backup */, TableInfo> tables;
    };
}


BackupEntries makeBackupEntries(
    const ContextPtr & context,
    const Elements & elements,
    const BackupSettings & backup_settings,
    std::shared_ptr<IBackupCoordination> backup_coordination,
    std::chrono::seconds timeout_for_preparing)
{
    BackupEntriesBuilder builder{context, backup_settings, backup_coordination};
    builder.prepare(elements, timeout_for_preparing);
    return std::move(builder).makeBackupEntries();
}


void writeBackupEntries(BackupMutablePtr backup, BackupEntries && backup_entries, ThreadPool & thread_pool)
{
    size_t num_active_jobs = 0;
    std::mutex mutex;
    std::condition_variable event;
    std::exception_ptr exception;

    bool always_single_threaded = !backup->supportsWritingInMultipleThreads();

    for (auto & name_and_entry : backup_entries)
    {
        auto & name = name_and_entry.first;
        auto & entry = name_and_entry.second;

        {
            std::unique_lock lock{mutex};
            if (exception)
                break;
            ++num_active_jobs;
        }

        auto job = [&]()
        {
            SCOPE_EXIT({
                std::lock_guard lock{mutex};
                if (!--num_active_jobs)
                    event.notify_all();
            });

            {
                std::lock_guard lock{mutex};
                if (exception)
                    return;
            }

            try
            {
                backup->writeFile(name, std::move(entry));
            }
            catch (...)
            {
                std::lock_guard lock{mutex};
                if (!exception)
                    exception = std::current_exception();
            }
        };

        if (always_single_threaded || !thread_pool.trySchedule(job))
            job();
    }

    {
        std::unique_lock lock{mutex};
        event.wait(lock, [&] { return !num_active_jobs; });
    }

    backup_entries.clear();

    if (exception)
    {
        /// We don't call finalizeWriting() if an error occurs.
        /// And IBackup's implementation should remove the backup in its destructor if finalizeWriting() hasn't called before.
        std::rethrow_exception(exception);
    }

    backup->finalizeWriting();
}


/// Returns access required to execute BACKUP query.
AccessRightsElements getRequiredAccessToBackup(const ASTBackupQuery::Elements & elements, const BackupSettings & backup_settings)
{
    AccessRightsElements required_access;
    for (const auto & element : elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::TABLE:
            {
                if (element.is_temp_db)
                    break;
                AccessFlags flags = AccessType::SHOW_TABLES;
                if (!backup_settings.structure_only)
                    flags |= AccessType::SELECT;
                required_access.emplace_back(flags, element.name.first, element.name.second);
                break;
            }
            case ASTBackupQuery::DATABASE:
            {
                if (element.is_temp_db)
                    break;
                AccessFlags flags = AccessType::SHOW_TABLES | AccessType::SHOW_DATABASES;
                if (!backup_settings.structure_only)
                    flags |= AccessType::SELECT;
                required_access.emplace_back(flags, element.name.first);
                /// TODO: It's better to process `element.except_list` somehow.
                break;
            }
            case ASTBackupQuery::ALL_DATABASES:
            {
                AccessFlags flags = AccessType::SHOW_TABLES | AccessType::SHOW_DATABASES;
                if (!backup_settings.structure_only)
                    flags |= AccessType::SELECT;
                required_access.emplace_back(flags);
                /// TODO: It's better to process `element.except_list` somehow.
                break;
            }
        }
    }
    return required_access;
}

}
