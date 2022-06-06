#include <Backups/BackupUtils.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupSettings.h>
#include <Backups/DDLCompareUtils.h>
#include <Backups/DDLRenamingVisitor.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupCoordination.h>
#include <Backups/formatTableNameOrTemporaryTableName.h>
#include <Backups/replaceTableUUIDWithMacroInReplicatedTableDef.h>
#include <Common/escapeForFileName.h>
#include <Access/Common/AccessRightsElement.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/formatAST.h>
#include <Storages/IStorage.h>
#include <Storages/StorageReplicatedMergeTree.h>


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

        static String getMetadataPath(const IAST & create_query, size_t shard_index, size_t replica_index)
        {
            const auto & create = create_query.as<const ASTCreateQuery &>();
            if (!create.table)
                return getMetadataPath(create.getDatabase(), shard_index, replica_index);
            if (create.temporary)
                return getMetadataPath({DatabaseCatalog::TEMPORARY_DATABASE, create.getTable()}, shard_index, replica_index);
            return getMetadataPath({create.getDatabase(), create.getTable()}, shard_index, replica_index);
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

        static String getDataPath(const IAST & create_query, size_t shard_index, size_t replica_index)
        {
            const auto & create = create_query.as<const ASTCreateQuery &>();
            if (!create.table)
                return {};
            if (create.temporary)
                return getDataPath({DatabaseCatalog::TEMPORARY_DATABASE, create.getTable()}, shard_index, replica_index);
            return getDataPath({create.getDatabase(), create.getTable()}, shard_index, replica_index);
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
        void prepare(const ASTBackupQuery::Elements & elements, std::chrono::seconds timeout_for_other_nodes_to_prepare)
        {
            try
            {
                prepareImpl(elements);
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
                timeout_for_other_nodes_to_prepare);
        }

        /// Makes backup entries, should be called after prepare().
        BackupEntries makeBackupEntries() const
        {
            BackupEntries res;
            for (const auto & info : databases | boost::adaptors::map_values)
                res.push_back(makeBackupEntryForMetadata(*info.create_query));

            for (const auto & info : tables | boost::adaptors::map_values)
            {
                res.push_back(makeBackupEntryForMetadata(*info.create_query));
                appendBackupEntriesForData(res, info);
            }

            /// A backup cannot be empty.
            if (res.empty())
                throw Exception("Backup must not be empty", ErrorCodes::BACKUP_IS_EMPTY);

            return res;
        }

    private:
        void prepareImpl(const ASTBackupQuery::Elements & elements)
        {
            calculateShardNumAndReplicaNumInBackup();
            renaming_settings.setFromBackupQuery(elements);

            for (const auto & element : elements)
            {
                switch (element.type)
                {
                    case ElementType::TABLE:
                    {
                        prepareToBackupTable(element.name, element.partitions);
                        break;
                    }

                    case ElementType::DATABASE:
                    {
                        const String & database_name = element.name.first;
                        prepareToBackupDatabase(database_name, element.except_list);
                        break;
                    }

                    case ElementType::ALL_DATABASES:
                    {
                        prepareToBackupAllDatabases(element.except_list);
                        break;
                    }
                }
            }
        }

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

        /// Prepares to backup a single table and probably its database's definition.
        void prepareToBackupTable(const DatabaseAndTableName & table_name_, const ASTs & partitions_)
        {
            auto [database, storage] = DatabaseCatalog::instance().getDatabaseAndTable({table_name_.first, table_name_.second}, context);
            prepareToBackupTable(table_name_, {database, storage}, partitions_);
        }

        void prepareToBackupTable(const DatabaseAndTableName & table_name_, const DatabaseAndTable & table_, const ASTs & partitions_)
        {
            const auto & database = table_.first;
            const auto & storage = table_.second;

            if (!database->hasTablesToBackup())
                throw Exception(
                    ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Cannot backup the {} because it's contained in a hollow database (engine: {})",
                    formatTableNameOrTemporaryTableName(table_name_),
                    database->getEngineName());

            /// Check that we are not trying to backup the same table again.
            DatabaseAndTableName name_in_backup = renaming_settings.getNewTableName(table_name_);
            if (tables.contains(name_in_backup))
                throw Exception(ErrorCodes::CANNOT_BACKUP_TABLE, "Cannot backup the {} twice", formatTableNameOrTemporaryTableName(name_in_backup));

            /// Make a create query for this table.
            auto create_query = prepareCreateQueryForBackup(database->getCreateTableQuery(table_name_.second, context));
            String data_path = PathsInBackup::getDataPath(*create_query, shard_num_in_backup, replica_num_in_backup);

            String zk_path;
            BackupEntries data = prepareToBackupTableData(table_name_, storage, partitions_, data_path, zk_path);

            TableInfo info;
            info.table_name = table_name_;
            info.create_query = create_query;
            info.storage = storage;
            info.data = std::move(data);
            info.data_path = std::move(data_path);
            info.zk_path = std::move(zk_path);
            tables[name_in_backup] = std::move(info);
        }

        BackupEntries prepareToBackupTableData(const DatabaseAndTableName & table_name_, const StoragePtr & storage_, const ASTs & partitions_, const String & data_path, String & zk_path)
        {
            zk_path.clear();

            const StorageReplicatedMergeTree * replicated_table = typeid_cast<const StorageReplicatedMergeTree *>(storage_.get());
            bool has_data = (storage_->hasDataToBackup() || replicated_table) && !backup_settings.structure_only;
            if (!has_data)
                return {};

            BackupEntries data = storage_->backupData(context, partitions_);
            if (!replicated_table)
                return data;

            zk_path = replicated_table->getZooKeeperName() + replicated_table->getZooKeeperPath();
            backup_coordination->addReplicatedTableDataPath(zk_path, data_path);
            std::unordered_map<String, SipHash> parts;
            for (const auto & [relative_path, backup_entry] : data)
            {
                size_t slash_pos = relative_path.find('/');
                if (slash_pos != String::npos)
                {
                    String part_name = relative_path.substr(0, slash_pos);
                    if (MergeTreePartInfo::tryParsePartName(part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING))
                    {
                        auto & hash = parts[part_name];
                        if (relative_path.ends_with(".bin"))
                        {
                            auto checksum = backup_entry->getChecksum();
                            hash.update(relative_path);
                            hash.update(backup_entry->getSize());
                            hash.update(*checksum);
                        }
                    }
                }
            }

            std::vector<IBackupCoordination::PartNameAndChecksum> part_names_and_checksums;
            part_names_and_checksums.reserve(parts.size());
            for (auto & [part_name, hash] : parts)
            {
                UInt128 checksum;
                hash.get128(checksum);
                auto & part_name_and_checksum = part_names_and_checksums.emplace_back();
                part_name_and_checksum.part_name = part_name;
                part_name_and_checksum.checksum = checksum;
            }
            backup_coordination->addReplicatedTablePartNames(backup_settings.host_id, table_name_, zk_path, part_names_and_checksums);

            return data;
        }

        /// Prepares to restore a database and all tables in it.
        void prepareToBackupDatabase(const String & database_name_, const std::set<String> & except_list_)
        {
            auto database = DatabaseCatalog::instance().getDatabase(database_name_, context);
            prepareToBackupDatabase(database_name_, database, except_list_);
        }

        void prepareToBackupDatabase(const String & database_name_, const DatabasePtr & database_, const std::set<String> & except_list_)
        {
            /// Check that we are not trying to restore the same database again.
            String name_in_backup = renaming_settings.getNewDatabaseName(database_name_);
            if (databases.contains(name_in_backup))
                throw Exception(ErrorCodes::CANNOT_BACKUP_DATABASE, "Cannot backup the database {} twice", backQuoteIfNeed(name_in_backup));

            /// Of course we're not going to backup the definition of the system or the temporary database.
            if (!isSystemOrTemporaryDatabase(database_name_))
            {
                /// Make a create query for this database.
                auto create_query = prepareCreateQueryForBackup(database_->getCreateDatabaseQuery());

                DatabaseInfo info;
                info.create_query = create_query;
                databases[name_in_backup] = std::move(info);
            }

            /// Backup tables in this database.
            if (database_->hasTablesToBackup())
            {
                for (auto it = database_->getTablesIterator(context); it->isValid(); it->next())
                {
                    if (except_list_.contains(it->name()))
                        continue;
                    prepareToBackupTable({database_name_, it->name()}, {database_, it->table()}, {});
                }
            }
        }

        /// Prepares to backup all the databases contained in the backup.
        void prepareToBackupAllDatabases(const std::set<String> & except_list_)
        {
            for (const auto & [database_name, database] : DatabaseCatalog::instance().getDatabases())
            {
                if (except_list_.contains(database_name))
                    continue;
                if (isSystemOrTemporaryDatabase(database_name))
                    continue;
                prepareToBackupDatabase(database_name, database, {});
            }
        }

        /// Do renaming in the create query according to the renaming config.
        std::shared_ptr<ASTCreateQuery> prepareCreateQueryForBackup(const ASTPtr & ast) const
        {
            ASTPtr query = ast;
            ::DB::renameInCreateQuery(query, context, renaming_settings);
            auto create_query = typeid_cast<std::shared_ptr<ASTCreateQuery>>(query);
            replaceTableUUIDWithMacroInReplicatedTableDef(*create_query, create_query->uuid);
            create_query->uuid = UUIDHelpers::Nil;
            create_query->to_inner_uuid = UUIDHelpers::Nil;
            return create_query;
        }

        static bool isSystemOrTemporaryDatabase(const String & database_name)
        {
            return (database_name == DatabaseCatalog::SYSTEM_DATABASE) || (database_name == DatabaseCatalog::TEMPORARY_DATABASE);
        }

        std::pair<String, BackupEntryPtr> makeBackupEntryForMetadata(const IAST & create_query) const
        {
            auto metadata_entry = std::make_unique<BackupEntryFromMemory>(serializeAST(create_query));
            String metadata_path = PathsInBackup::getMetadataPath(create_query, shard_num_in_backup, replica_num_in_backup);
            return {metadata_path, std::move(metadata_entry)};
        }

        struct TableInfo;

        void appendBackupEntriesForData(BackupEntries & res, const TableInfo & info) const
        {
            if (info.zk_path.empty())
            {
                for (const auto & [relative_path, backup_entry] : info.data)
                    res.emplace_back(info.data_path + relative_path, backup_entry);
                return;
            }

            Strings data_paths = backup_coordination->getReplicatedTableDataPaths(info.zk_path);
            Strings part_names = backup_coordination->getReplicatedTablePartNames(backup_settings.host_id, info.table_name, info.zk_path);
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

        /// Information which is used to make an instance of RestoreTableFromBackupTask.
        struct TableInfo
        {
            DatabaseAndTableName table_name;
            ASTPtr create_query;
            StoragePtr storage;
            BackupEntries data;
            String data_path;
            String zk_path;
        };

        /// Information which is used to make an instance of RestoreDatabaseFromBackupTask.
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
    std::chrono::seconds timeout_for_other_nodes_to_prepare)
{
    BackupEntriesBuilder builder{context, backup_settings, backup_coordination};
    builder.prepare(elements, timeout_for_other_nodes_to_prepare);
    return builder.makeBackupEntries();
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
