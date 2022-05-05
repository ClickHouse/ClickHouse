#include <Backups/BackupUtils.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupSettings.h>
#include <Backups/DDLCompareUtils.h>
#include <Backups/DDLRenamingVisitor.h>
#include <Backups/IBackup.h>
#include <Backups/formatTableNameOrTemporaryTableName.h>
#include <Backups/replaceTableUUIDWithMacroInReplicatedTableDef.h>
#include <Common/escapeForFileName.h>
#include <Access/Common/AccessFlags.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/formatAST.h>
#include <Storages/IStorage.h>


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
        BackupEntriesBuilder(const ContextPtr & context_, const BackupSettings & backup_settings_)
            : context(context_), backup_settings(backup_settings_)
        {
        }

        /// Prepares internal structures for making backup entries.
        void prepare(const ASTBackupQuery::Elements & elements)
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

        /// Makes backup entries, should be called after prepare().
        BackupEntries makeBackupEntries() const
        {
            BackupEntries res;
            for (const auto & info : databases | boost::adaptors::map_values)
                res.push_back(makeBackupEntryForMetadata(*info.create_query));

            for (const auto & info : tables | boost::adaptors::map_values)
            {
                res.push_back(makeBackupEntryForMetadata(*info.create_query));
                if (info.has_data)
                {
                    auto data_backup = info.storage->backupData(context, info.partitions);
                    if (!data_backup.empty())
                    {
                        String data_path = PathsInBackup::getDataPath(*info.create_query, shard_num_in_backup, replica_num_in_backup);
                        for (auto & [path_in_backup, backup_entry] : data_backup)
                            res.emplace_back(data_path + path_in_backup, std::move(backup_entry));
                    }
                }
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

        /// Prepares to backup a single table and probably its database's definition.
        void prepareToBackupTable(const DatabaseAndTableName & table_name_, const ASTs & partitions_)
        {
            auto [database, storage] = DatabaseCatalog::instance().getDatabaseAndTable({table_name_.first, table_name_.second}, context);
            prepareToBackupTable(table_name_, {database, storage}, partitions_);
        }

        void prepareToBackupTable(const DatabaseAndTableName & table_name_, const DatabaseAndTable & table_, const ASTs & partitions_)
        {
            context->checkAccess(AccessType::SHOW_TABLES, table_name_.first, table_name_.second);

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

            bool has_data = storage->hasDataToBackup() && !backup_settings.structure_only;
            if (has_data)
            {
                /// We check for SELECT privilege only if we're going to read data from the table.
                context->checkAccess(AccessType::SELECT, table_name_.first, table_name_.second);
            }

            CreateTableInfo info;
            info.create_query = create_query;
            info.storage = storage;
            info.partitions = partitions_;
            info.has_data = has_data;
            tables[name_in_backup] = std::move(info);
        }

        /// Prepares to restore a database and all tables in it.
        void prepareToBackupDatabase(const String & database_name_, const std::set<String> & except_list_)
        {
            auto database = DatabaseCatalog::instance().getDatabase(database_name_, context);
            prepareToBackupDatabase(database_name_, database, except_list_);
        }

        void prepareToBackupDatabase(const String & database_name_, const DatabasePtr & database_, const std::set<String> & except_list_)
        {
            context->checkAccess(AccessType::SHOW_DATABASES, database_name_);

            /// Check that we are not trying to restore the same database again.
            String name_in_backup = renaming_settings.getNewDatabaseName(database_name_);
            if (databases.contains(name_in_backup))
                throw Exception(ErrorCodes::CANNOT_BACKUP_DATABASE, "Cannot backup the database {} twice", backQuoteIfNeed(name_in_backup));

            /// Of course we're not going to backup the definition of the system or the temporary database.
            if (!isSystemOrTemporaryDatabase(database_name_))
            {
                /// Make a create query for this database.
                auto create_query = prepareCreateQueryForBackup(database_->getCreateDatabaseQuery());

                CreateDatabaseInfo info;
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

        /// Information which is used to make an instance of RestoreTableFromBackupTask.
        struct CreateTableInfo
        {
            ASTPtr create_query;
            StoragePtr storage;
            ASTs partitions;
            bool has_data = false;
        };

        /// Information which is used to make an instance of RestoreDatabaseFromBackupTask.
        struct CreateDatabaseInfo
        {
            ASTPtr create_query;
        };

        ContextPtr context;
        BackupSettings backup_settings;
        size_t shard_num_in_backup = 0;
        size_t replica_num_in_backup = 0;
        DDLRenamingSettings renaming_settings;
        std::unordered_map<String /* db_name_in_backup */, CreateDatabaseInfo> databases;
        std::map<DatabaseAndTableName /* table_name_in_backup */, CreateTableInfo> tables;
    };
}


BackupEntries makeBackupEntries(const ContextPtr & context, const Elements & elements, const BackupSettings & backup_settings)
{
    BackupEntriesBuilder builder{context, backup_settings};
    builder.prepare(elements);
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

}
