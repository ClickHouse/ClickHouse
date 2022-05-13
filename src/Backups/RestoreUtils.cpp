#include <Backups/RestoreUtils.h>
#include <Backups/BackupUtils.h>
#include <Backups/BackupSettings.h>
#include <Backups/RestoreSettings.h>
#include <Backups/DDLCompareUtils.h>
#include <Backups/DDLRenamingVisitor.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupEntry.h>
#include <Backups/IRestoreTask.h>
#include <Backups/IRestoreCoordination.h>
#include <Backups/formatTableNameOrTemporaryTableName.h>
#include <Common/escapeForFileName.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseReplicated.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <base/chrono_io.h>
#include <base/insertAtEnd.h>
#include <boost/range/algorithm_ext/erase.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_TABLE;
    extern const int CANNOT_RESTORE_DATABASE;
    extern const int BACKUP_ENTRY_NOT_FOUND;
}

namespace
{
    class PathsInBackup
    {
    public:
        explicit PathsInBackup(const IBackup & backup_) : backup(backup_) { }

        std::vector<size_t> getShards() const
        {
            std::vector<size_t> res;
            for (const String & shard_index : backup.listFiles("shards/"))
                res.push_back(parse<UInt64>(shard_index));
            if (res.empty())
                res.push_back(1);
            return res;
        }

        std::vector<size_t> getReplicas(size_t shard_index) const
        {
            std::vector<size_t> res;
            for (const String & replica_index : backup.listFiles(fmt::format("shards/{}/replicas/", shard_index)))
                res.push_back(parse<UInt64>(replica_index));
            if (res.empty())
                res.push_back(1);
            return res;
        }

        std::vector<String> getDatabases(size_t shard_index, size_t replica_index) const
        {
            std::vector<String> res;

            insertAtEnd(res, backup.listFiles(fmt::format("shards/{}/replicas/{}/metadata/", shard_index, replica_index)));
            insertAtEnd(res, backup.listFiles(fmt::format("shards/{}/metadata/", shard_index)));
            insertAtEnd(res, backup.listFiles(fmt::format("metadata/")));

            boost::range::remove_erase_if(
                res,
                [](String & str)
                {
                    if (str.ends_with(".sql"))
                    {
                        str.resize(str.length() - strlen(".sql"));
                        str = unescapeForFileName(str);
                        return false;
                    }
                    return true;
                });

            std::sort(res.begin(), res.end());
            res.erase(std::unique(res.begin(), res.end()), res.end());
            return res;
        }

        std::vector<String> getTables(const String & database_name, size_t shard_index, size_t replica_index) const
        {
            std::vector<String> res;

            String escaped_database_name = escapeForFileName(database_name);
            insertAtEnd(
                res,
                backup.listFiles(fmt::format("shards/{}/replicas/{}/metadata/{}/", shard_index, replica_index, escaped_database_name)));
            insertAtEnd(res, backup.listFiles(fmt::format("shards/{}/metadata/{}/", shard_index, escaped_database_name)));
            insertAtEnd(res, backup.listFiles(fmt::format("metadata/{}/", escaped_database_name)));

            boost::range::remove_erase_if(
                res,
                [](String & str)
                {
                    if (str.ends_with(".sql"))
                    {
                        str.resize(str.length() - strlen(".sql"));
                        str = unescapeForFileName(str);
                        return false;
                    }
                    return true;
                });

            std::sort(res.begin(), res.end());
            res.erase(std::unique(res.begin(), res.end()), res.end());
            return res;
        }

        /// Returns the path to metadata in backup.
        String getMetadataPath(const DatabaseAndTableName & table_name, size_t shard_index, size_t replica_index) const
        {
            String escaped_table_name = escapeForFileName(table_name.first) + "/" + escapeForFileName(table_name.second);
            String path1 = fmt::format("shards/{}/replicas/{}/metadata/{}.sql", shard_index, replica_index, escaped_table_name);
            if (backup.fileExists(path1))
                return path1;
            String path2 = fmt::format("shards/{}/metadata/{}.sql", shard_index, escaped_table_name);
            if (backup.fileExists(path2))
                return path2;
            String path3 = fmt::format("metadata/{}.sql", escaped_table_name);
            return path3;
        }

        String getMetadataPath(const String & database_name, size_t shard_index, size_t replica_index) const
        {
            String escaped_database_name = escapeForFileName(database_name);
            String path1 = fmt::format("shards/{}/replicas/{}/metadata/{}.sql", shard_index, replica_index, escaped_database_name);
            if (backup.fileExists(path1))
                return path1;
            String path2 = fmt::format("shards/{}/metadata/{}.sql", shard_index, escaped_database_name);
            if (backup.fileExists(path2))
                return path2;
            String path3 = fmt::format("metadata/{}.sql", escaped_database_name);
            return path3;
        }

        String getDataPath(const DatabaseAndTableName & table_name, size_t shard_index, size_t replica_index) const
        {
            String escaped_table_name = escapeForFileName(table_name.first) + "/" + escapeForFileName(table_name.second);
            if (backup.fileExists(fmt::format("shards/{}/replicas/{}/metadata/{}.sql", shard_index, replica_index, escaped_table_name)))
                return fmt::format("shards/{}/replicas/{}/data/{}/", shard_index, replica_index, escaped_table_name);
            if (backup.fileExists(fmt::format("shards/{}/metadata/{}.sql", shard_index, escaped_table_name)))
                return fmt::format("shards/{}/data/{}/", shard_index, escaped_table_name);
            return fmt::format("data/{}/", escaped_table_name);
        }

    private:
        const IBackup & backup;
    };


    using Kind = ASTBackupQuery::Kind;
    using Element = ASTBackupQuery::Element;
    using Elements = ASTBackupQuery::Elements;
    using ElementType = ASTBackupQuery::ElementType;
    using RestoreSettingsPtr = std::shared_ptr<const RestoreSettings>;


    /// Restores a database (without tables inside), should be executed before executing
    /// RestoreTableTask.
    class RestoreDatabaseTask : public IRestoreTask
    {
    public:
        RestoreDatabaseTask(ContextMutablePtr context_, const ASTPtr & create_query_, const RestoreSettingsPtr & restore_settings_)
            : context(context_)
            , create_query(typeid_cast<std::shared_ptr<ASTCreateQuery>>(create_query_))
            , restore_settings(restore_settings_)
        {
        }

        RestoreTasks run() override
        {
            createDatabase();
            getDatabase();
            checkDatabaseCreateQuery();
            return {};
        }

        RestoreKind getRestoreKind() const override { return RestoreKind::METADATA; }

    private:
        void createDatabase()
        {
            if (restore_settings->create_database == RestoreDatabaseCreationMode::kMustExist)
                return;

            auto cloned_create_query = typeid_cast<std::shared_ptr<ASTCreateQuery>>(create_query->clone());
            cloned_create_query->if_not_exists = (restore_settings->create_database == RestoreDatabaseCreationMode::kCreateIfNotExists);
            InterpreterCreateQuery create_interpreter{cloned_create_query, context};
            create_interpreter.setInternal(true);
            create_interpreter.execute();
        }

        DatabasePtr getDatabase()
        {
            if (!database)
                database = DatabaseCatalog::instance().getDatabase(create_query->getDatabase());
            return database;
        }

        ASTPtr getDatabaseCreateQuery()
        {
            if (!database_create_query)
                database_create_query = getDatabase()->getCreateDatabaseQuery();
            return database_create_query;
        }

        void checkDatabaseCreateQuery()
        {
            if (restore_settings->allow_different_database_def)
                return;

            getDatabaseCreateQuery();
            if (areDatabaseDefinitionsSame(*create_query, *database_create_query))
                return;

            throw Exception(
                ErrorCodes::CANNOT_RESTORE_DATABASE,
                "The database {} already exists but has a different definition: {}, "
                "compare to its definition in the backup: {}",
                backQuoteIfNeed(create_query->getDatabase()),
                serializeAST(*database_create_query),
                serializeAST(*create_query));
        }

        ContextMutablePtr context;
        std::shared_ptr<ASTCreateQuery> create_query;
        RestoreSettingsPtr restore_settings;
        DatabasePtr database;
        ASTPtr database_create_query;
    };


    /// Restores a table.
    class RestoreTableTask : public IRestoreTask
    {
    public:
        RestoreTableTask(
            ContextMutablePtr context_,
            const ASTPtr & create_query_,
            const ASTs & partitions_,
            const BackupPtr & backup_,
            const DatabaseAndTableName & table_name_in_backup_,
            const RestoreSettingsPtr & restore_settings_,
            const std::shared_ptr<IRestoreCoordination> & restore_coordination_,
            std::chrono::seconds timeout_for_restoring_metadata_)
            : context(context_)
            , create_query(typeid_cast<std::shared_ptr<ASTCreateQuery>>(create_query_))
            , partitions(partitions_)
            , backup(backup_)
            , table_name_in_backup(table_name_in_backup_)
            , restore_settings(restore_settings_)
            , restore_coordination(restore_coordination_)
            , timeout_for_restoring_metadata(timeout_for_restoring_metadata_)
        {
            table_name = DatabaseAndTableName{create_query->getDatabase(), create_query->getTable()};
            if (create_query->temporary)
                table_name.first = DatabaseCatalog::TEMPORARY_DATABASE;
        }

        RestoreTasks run() override
        {
            getDatabase();
            createStorage();
            getStorage();
            checkStorageCreateQuery();
            checkTableIsEmpty();
            checkTableDataCompatible();
            return insertData();
        }

        RestoreKind getRestoreKind() const override { return RestoreKind::METADATA; }

    private:
        void getDatabase()
        {
            database = DatabaseCatalog::instance().getDatabase(table_name.first);
            replicated_database = typeid_cast<std::shared_ptr<DatabaseReplicated>>(database);
        }

        void createStorage()
        {
            if (restore_settings->create_table == RestoreTableCreationMode::kMustExist)
                return;

            auto cloned_create_query = typeid_cast<std::shared_ptr<ASTCreateQuery>>(create_query->clone());
            cloned_create_query->if_not_exists = (restore_settings->create_table == RestoreTableCreationMode::kCreateIfNotExists);

            /// We need a special processing for tables in replicated databases.
            /// Because of the replication multiple nodes can try to restore the same tables again and failed with "Table already exists"
            /// because of some table could be restored already on other node and then replicated to this node.
            /// To solve this problem we use the restore coordination: the first node calls
            /// IRestoreCoordination::startCreatingTableInReplicatedDB() and then for other nodes this function returns false which means
            /// this table is already being created by some other node.
            bool wait_instead_of_creating = false;
            if (replicated_database)
                wait_instead_of_creating = !restore_coordination->startCreatingTableInReplicatedDB(
                    restore_settings->host_id, table_name.first, replicated_database->getZooKeeperPath(), table_name.second);

            if (wait_instead_of_creating)
            {
                waitForReplicatedDatabaseToSyncTable();
            }
            else
            {
                try
                {
                    InterpreterCreateQuery create_interpreter{cloned_create_query, context};
                    create_interpreter.setInternal(true);
                    create_interpreter.execute();
                }
                catch (...)
                {
                    if (replicated_database)
                    {
                        restore_coordination->finishCreatingTableInReplicatedDB(
                            restore_settings->host_id,
                            table_name.first,
                            replicated_database->getZooKeeperPath(),
                            table_name.second,
                            getCurrentExceptionMessage(false));
                    }
                    throw;
                }

                if (replicated_database)
                    restore_coordination->finishCreatingTableInReplicatedDB(
                        restore_settings->host_id, table_name.first, replicated_database->getZooKeeperPath(), table_name.second);
            }
        }

        void waitForReplicatedDatabaseToSyncTable()
        {
            if (!replicated_database)
                return;

            restore_coordination->waitForTableCreatedInReplicatedDB(
                table_name.first, replicated_database->getZooKeeperPath(), table_name.second);

            /// The table `table_name` was created on other host, must be in the replicated database's queue,
            /// we have to wait until the replicated database syncs that.
            bool replicated_database_synced = false;
            auto start_time = std::chrono::steady_clock::now();
            bool use_timeout = (timeout_for_restoring_metadata.count() > 0);
            while (!database->isTableExist(table_name.second, context))
            {
                if (replicated_database_synced
                    || (use_timeout && (std::chrono::steady_clock::now() - start_time) >= timeout_for_restoring_metadata))
                {
                    throw Exception(
                        ErrorCodes::CANNOT_RESTORE_TABLE,
                        "Table {}.{} in the replicated database {} was not synced from another node in {}",
                        table_name.first,
                        table_name.second,
                        table_name.first,
                        to_string(timeout_for_restoring_metadata));
                }
                replicated_database_synced = replicated_database->waitForReplicaToProcessAllEntries(50);
            }
        }

        void getStorage()
        {
            storage = database->getTable(table_name.second, context);
            storage_create_query = database->getCreateTableQuery(table_name.second, context);

            if (!restore_settings->structure_only)
            {
                data_path_in_backup = PathsInBackup{*backup}.getDataPath(
                    table_name_in_backup, restore_settings->shard_num_in_backup, restore_settings->replica_num_in_backup);
                has_data = !backup->listFiles(data_path_in_backup).empty();

                const auto * replicated_table = typeid_cast<const StorageReplicatedMergeTree *>(storage.get());
                if (replicated_table)
                {
                    /// We need to be consistent when we're restoring replicated tables.
                    /// It's allowed for a backup to contain multiple replicas of the same replicated table,
                    /// and when we restore it we need to choose single data path in the backup to restore this table on each replica.
                    /// That's why we use the restore coordination here: on restoring metadata stage each replica sets its own
                    /// `data_path_in_backup` for same zookeeper path, and then the restore coordination choose one `data_path_in_backup`
                    /// to use for restoring data.
                    restore_coordination->addReplicatedTableDataPath(
                        restore_settings->host_id,
                        table_name_in_backup,
                        replicated_table->getZooKeeperName() + replicated_table->getZooKeeperPath(),
                        data_path_in_backup);
                    has_data = true;
                }
            }
        }

        void checkStorageCreateQuery()
        {
            if (!restore_settings->allow_different_table_def && !areTableDefinitionsSame(*create_query, *storage_create_query))
            {
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_TABLE,
                    "The {} already exists but has a different definition: {}, "
                    "compare to its definition in the backup: {}",
                    formatTableNameOrTemporaryTableName(table_name),
                    serializeAST(*storage_create_query),
                    serializeAST(*create_query));
            }
        }

        void checkTableIsEmpty()
        {
            if (restore_settings->allow_non_empty_tables || restore_settings->structure_only || !has_data)
                return;

            bool empty = true;
            if (auto total_rows = storage->totalRows(context->getSettingsRef()))
                empty = (*total_rows == 0);
            else if (auto total_bytes = storage->totalBytes(context->getSettingsRef()))
                empty = (*total_bytes == 0);

            if (empty)
            {
                /// If this is a replicated table new parts could be in its queue but not fetched yet.
                /// In that case we consider the table as not empty.
                if (auto * replicated_table = typeid_cast<StorageReplicatedMergeTree *>(storage.get()))
                {
                    StorageReplicatedMergeTree::Status status;
                    replicated_table->getStatus(status, /* with_zk_fields = */ false);

                    if (status.queue.inserts_in_queue)
                    {
                        empty = false;
                    }
                    else
                    {
                        /// Check total_rows again to be sure.
                        if (auto total_rows = storage->totalRows(context->getSettingsRef()); *total_rows != 0)
                            empty = false;
                    }
                }
            }

            if (!empty)
            {
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Cannot restore {} because it already contains some data. You can set structure_only=true or "
                    "allow_non_empty_tables=true to overcome that in the way you want",
                    formatTableNameOrTemporaryTableName(table_name));
            }
        }

        void checkTableDataCompatible()
        {
            if (restore_settings->structure_only || !has_data)
                return;

            if (!areTableDataCompatible(*create_query, *storage_create_query))
            {
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Cannot attach data of the {} in the backup to the existing {} because of they are not compatible. "
                    "Here is the definition of the {} in the backup: {}, and here is the definition of the existing {}: {}",
                    formatTableNameOrTemporaryTableName(table_name_in_backup),
                    formatTableNameOrTemporaryTableName(table_name),
                    formatTableNameOrTemporaryTableName(table_name_in_backup),
                    serializeAST(*create_query),
                    formatTableNameOrTemporaryTableName(table_name),
                    serializeAST(*storage_create_query));
            }
        }

        RestoreTasks insertData()
        {
            if (restore_settings->structure_only || !has_data)
                return {};

            RestoreTasks tasks;
            tasks.emplace_back(
                storage->restoreData(context, partitions, backup, data_path_in_backup, *restore_settings, restore_coordination));
            return tasks;
        }

        ContextMutablePtr context;
        std::shared_ptr<ASTCreateQuery> create_query;
        DatabaseAndTableName table_name;
        ASTs partitions;
        BackupPtr backup;
        DatabaseAndTableName table_name_in_backup;
        RestoreSettingsPtr restore_settings;
        std::shared_ptr<IRestoreCoordination> restore_coordination;
        std::chrono::seconds timeout_for_restoring_metadata;
        DatabasePtr database;
        std::shared_ptr<DatabaseReplicated> replicated_database;
        StoragePtr storage;
        ASTPtr storage_create_query;
        bool has_data = false;
        String data_path_in_backup;
    };


    /// Makes tasks for restoring databases and tables according to the elements of ASTBackupQuery.
    /// Keep this class consistent with BackupEntriesBuilder.
    class RestoreTasksBuilder
    {
    public:
        RestoreTasksBuilder(
            ContextMutablePtr context_,
            const BackupPtr & backup_,
            const RestoreSettings & restore_settings_,
            const std::shared_ptr<IRestoreCoordination> & restore_coordination_,
            std::chrono::seconds timeout_for_restoring_metadata_)
            : context(context_)
            , backup(backup_)
            , restore_settings(restore_settings_)
            , restore_coordination(restore_coordination_)
            , timeout_for_restoring_metadata(timeout_for_restoring_metadata_)
        {
        }

        /// Prepares internal structures for making tasks for restoring.
        void prepare(const ASTBackupQuery::Elements & elements)
        {
            calculateShardNumAndReplicaNumInBackup();
            renaming_settings.setFromBackupQuery(elements);

            for (const auto & element : elements)
            {
                switch (element.type)
                {
                    case ElementType::TABLE: {
                        prepareToRestoreTable(element.name, element.partitions);
                        break;
                    }

                    case ElementType::DATABASE: {
                        const String & database_name = element.name.first;
                        prepareToRestoreDatabase(database_name, element.except_list);
                        break;
                    }

                    case ElementType::ALL_DATABASES: {
                        prepareToRestoreAllDatabases(element.except_list);
                        break;
                    }
                }
            }
        }

        /// Makes tasks for restoring, should be called after prepare().
        RestoreTasks makeTasks() const
        {
            auto restore_settings_ptr = std::make_shared<const RestoreSettings>(restore_settings);

            RestoreTasks res;
            for (const auto & info : databases | boost::adaptors::map_values)
                res.push_back(std::make_unique<RestoreDatabaseTask>(context, info.create_query, restore_settings_ptr));

            /// TODO: We need to restore tables according to their dependencies.
            for (const auto & info : tables | boost::adaptors::map_values)
                res.push_back(std::make_unique<RestoreTableTask>(
                    context,
                    info.create_query,
                    info.partitions,
                    backup,
                    info.name_in_backup,
                    restore_settings_ptr,
                    restore_coordination,
                    timeout_for_restoring_metadata));

            return res;
        }

    private:
        void calculateShardNumAndReplicaNumInBackup()
        {
            size_t shard_num = 0;
            size_t replica_num = 0;
            if (!restore_settings.host_id.empty())
            {
                std::tie(shard_num, replica_num)
                    = BackupSettings::Util::findShardNumAndReplicaNum(restore_settings.cluster_host_ids, restore_settings.host_id);
            }

            auto shards_in_backup = PathsInBackup{*backup}.getShards();
            if (!restore_settings.shard_num_in_backup)
            {
                if (shards_in_backup.size() == 1)
                    restore_settings.shard_num_in_backup = shards_in_backup[0];
                else
                    restore_settings.shard_num_in_backup = shard_num;
            }

            if (std::find(shards_in_backup.begin(), shards_in_backup.end(), restore_settings.shard_num_in_backup) == shards_in_backup.end())
                throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "No shard #{} in backup", restore_settings.shard_num_in_backup);

            auto replicas_in_backup = PathsInBackup{*backup}.getReplicas(restore_settings.shard_num_in_backup);
            if (!restore_settings.replica_num_in_backup)
            {
                if (replicas_in_backup.size() == 1)
                    restore_settings.replica_num_in_backup = replicas_in_backup[0];
                else if (std::find(replicas_in_backup.begin(), replicas_in_backup.end(), replica_num) != replicas_in_backup.end())
                    restore_settings.replica_num_in_backup = replica_num;
                else
                    restore_settings.replica_num_in_backup = replicas_in_backup[0];
            }
        }

        /// Prepares to restore a single table and probably its database's definition.
        void prepareToRestoreTable(const DatabaseAndTableName & table_name_, const ASTs & partitions_)
        {
            /// Check that we are not trying to restore the same table again.
            DatabaseAndTableName new_table_name = renaming_settings.getNewTableName(table_name_);
            if (tables.contains(new_table_name))
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_TABLE, "Cannot restore the {} twice", formatTableNameOrTemporaryTableName(new_table_name));

            /// Make a create query for this table.
            auto create_query = renameInCreateQuery(readCreateQueryFromBackup(table_name_));

            CreateTableInfo info;
            info.create_query = create_query;
            info.name_in_backup = table_name_;
            info.partitions = partitions_;
            tables[new_table_name] = std::move(info);
        }

        /// Prepares to restore a database and all tables in it.
        void prepareToRestoreDatabase(const String & database_name_, const std::set<String> & except_list_)
        {
            /// Check that we are not trying to restore the same database again.
            String new_database_name = renaming_settings.getNewDatabaseName(database_name_);
            if (databases.contains(new_database_name))
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_DATABASE, "Cannot restore the database {} twice", backQuoteIfNeed(new_database_name));

            Strings table_names = PathsInBackup{*backup}.getTables(
                database_name_, restore_settings.shard_num_in_backup, restore_settings.replica_num_in_backup);
            bool has_tables_in_backup = !table_names.empty();
            bool has_create_query_in_backup = hasCreateQueryInBackup(database_name_);

            if (!has_create_query_in_backup && !has_tables_in_backup)
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_DATABASE,
                    "Cannot restore the database {} because there is no such database in the backup",
                    backQuoteIfNeed(database_name_));

            /// Of course we're not going to restore the definition of the system or the temporary database.
            if (!isSystemOrTemporaryDatabase(new_database_name))
            {
                /// Make a create query for this database.
                std::shared_ptr<ASTCreateQuery> create_query;
                if (has_create_query_in_backup)
                {
                    create_query = renameInCreateQuery(readCreateQueryFromBackup(database_name_));
                }
                else
                {
                    create_query = std::make_shared<ASTCreateQuery>();
                    create_query->setDatabase(database_name_);
                }

                CreateDatabaseInfo info;
                info.create_query = create_query;
                databases[new_database_name] = std::move(info);
            }

            /// Restore tables in this database.
            for (const String & table_name : table_names)
            {
                if (except_list_.contains(table_name))
                    continue;
                prepareToRestoreTable(DatabaseAndTableName{database_name_, table_name}, ASTs{});
            }
        }

        /// Prepares to restore all the databases contained in the backup.
        void prepareToRestoreAllDatabases(const std::set<String> & except_list_)
        {
            for (const String & database_name :
                 PathsInBackup{*backup}.getDatabases(restore_settings.shard_num_in_backup, restore_settings.replica_num_in_backup))
            {
                if (except_list_.contains(database_name))
                    continue;
                prepareToRestoreDatabase(database_name, std::set<String>{});
            }
        }

        /// Reads a create query for creating a specified table from the backup.
        std::shared_ptr<ASTCreateQuery> readCreateQueryFromBackup(const DatabaseAndTableName & table_name) const
        {
            String create_query_path = PathsInBackup{*backup}.getMetadataPath(
                table_name, restore_settings.shard_num_in_backup, restore_settings.replica_num_in_backup);
            if (!backup->fileExists(create_query_path))
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Cannot restore the {} because there is no such table in the backup",
                    formatTableNameOrTemporaryTableName(table_name));
            auto read_buffer = backup->readFile(create_query_path)->getReadBuffer();
            String create_query_str;
            readStringUntilEOF(create_query_str, *read_buffer);
            read_buffer.reset();
            ParserCreateQuery create_parser;
            return typeid_cast<std::shared_ptr<ASTCreateQuery>>(
                parseQuery(create_parser, create_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH));
        }

        /// Reads a create query for creating a specified database from the backup.
        std::shared_ptr<ASTCreateQuery> readCreateQueryFromBackup(const String & database_name) const
        {
            String create_query_path = PathsInBackup{*backup}.getMetadataPath(
                database_name, restore_settings.shard_num_in_backup, restore_settings.replica_num_in_backup);
            if (!backup->fileExists(create_query_path))
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_DATABASE,
                    "Cannot restore the database {} because there is no such database in the backup",
                    backQuoteIfNeed(database_name));
            auto read_buffer = backup->readFile(create_query_path)->getReadBuffer();
            String create_query_str;
            readStringUntilEOF(create_query_str, *read_buffer);
            read_buffer.reset();
            ParserCreateQuery create_parser;
            return typeid_cast<std::shared_ptr<ASTCreateQuery>>(
                parseQuery(create_parser, create_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH));
        }

        /// Whether there is a create query for creating a specified database in the backup.
        bool hasCreateQueryInBackup(const String & database_name) const
        {
            String create_query_path = PathsInBackup{*backup}.getMetadataPath(
                database_name, restore_settings.shard_num_in_backup, restore_settings.replica_num_in_backup);
            return backup->fileExists(create_query_path);
        }

        /// Do renaming in the create query according to the renaming config.
        std::shared_ptr<ASTCreateQuery> renameInCreateQuery(const ASTPtr & ast) const
        {
            ASTPtr query = ast;
            ::DB::renameInCreateQuery(query, context, renaming_settings);
            auto create_query = typeid_cast<std::shared_ptr<ASTCreateQuery>>(query);
            return create_query;
        }

        static bool isSystemOrTemporaryDatabase(const String & database_name)
        {
            return (database_name == DatabaseCatalog::SYSTEM_DATABASE) || (database_name == DatabaseCatalog::TEMPORARY_DATABASE);
        }

        /// Information which is used to make an instance of RestoreTableTask.
        struct CreateTableInfo
        {
            ASTPtr create_query;
            DatabaseAndTableName name_in_backup;
            ASTs partitions;
        };

        /// Information which is used to make an instance of RestoreDatabaseTask.
        struct CreateDatabaseInfo
        {
            ASTPtr create_query;
        };

        ContextMutablePtr context;
        BackupPtr backup;
        RestoreSettings restore_settings;
        std::shared_ptr<IRestoreCoordination> restore_coordination;
        std::chrono::seconds timeout_for_restoring_metadata;
        DDLRenamingSettings renaming_settings;
        std::map<String /* new_db_name */, CreateDatabaseInfo> databases;
        std::map<DatabaseAndTableName /* new_table_name */, CreateTableInfo> tables;
    };


    RestoreTasks makeRestoreTasksImpl(
        ContextMutablePtr context,
        const BackupPtr & backup,
        const Elements & elements,
        const RestoreSettings & restore_settings,
        const std::shared_ptr<IRestoreCoordination> & restore_coordination,
        std::chrono::seconds timeout_for_restoring_metadata)
    {
        RestoreTasksBuilder builder{context, backup, restore_settings, restore_coordination, timeout_for_restoring_metadata};
        builder.prepare(elements);
        return builder.makeTasks();
    }


    void restoreMetadataImpl(RestoreTasks & restore_tasks)
    {
        /// There are two kinds of restore tasks: sequential and non-sequential ones.
        /// Sequential tasks are executed first and always in one thread.
        std::deque<std::unique_ptr<IRestoreTask>> restore_metadata_tasks;
        boost::range::remove_erase_if(
            restore_tasks,
            [&restore_metadata_tasks](RestoreTaskPtr & task)
            {
                if (task->getRestoreKind() == IRestoreTask::RestoreKind::METADATA)
                {
                    restore_metadata_tasks.push_back(std::move(task));
                    return true;
                }
                return false;
            });

        /// Sequential tasks.
        while (!restore_metadata_tasks.empty())
        {
            auto current_task = std::move(restore_metadata_tasks.front());
            restore_metadata_tasks.pop_front();

            RestoreTasks new_tasks = current_task->run();

            for (auto & task : new_tasks)
            {
                if (task->getRestoreKind() == IRestoreTask::RestoreKind::METADATA)
                    restore_metadata_tasks.push_back(std::move(task));
                else
                    restore_tasks.push_back(std::move(task));
            }
        }
    }
}


RestoreTasks makeRestoreTasks(
    ContextMutablePtr context,
    const BackupPtr & backup,
    const Elements & elements,
    const RestoreSettings & restore_settings,
    const std::shared_ptr<IRestoreCoordination> & restore_coordination,
    std::chrono::seconds timeout_for_restoring_metadata)
{
    try
    {
        return makeRestoreTasksImpl(context, backup, elements, restore_settings, restore_coordination, timeout_for_restoring_metadata);
    }
    catch (...)
    {
        restore_coordination->finishRestoringMetadata(restore_settings.host_id, getCurrentExceptionMessage(false));
        throw;
    }
}


void restoreMetadata(
    RestoreTasks & restore_tasks,
    const RestoreSettings & restore_settings,
    const std::shared_ptr<IRestoreCoordination> & restore_coordination,
    std::chrono::seconds timeout_for_restoring_metadata)
{
    try
    {
        restoreMetadataImpl(restore_tasks);
    }
    catch (...)
    {
        restore_coordination->finishRestoringMetadata(restore_settings.host_id, getCurrentExceptionMessage(false));
        throw;
    }

    /// We've finished restoring metadata, now we will wait for other replicas and shards to finish too.
    /// We need this waiting because we're going to call some functions which requires data collected from other nodes too,
    /// see IRestoreCoordination::checkTablesNotExistedInReplicatedDBs(), IRestoreCoordination::getReplicatedTableDataPath().
    restore_coordination->finishRestoringMetadata(restore_settings.host_id);

    restore_coordination->waitForAllHostsRestoredMetadata(
        BackupSettings::Util::filterHostIDs(
            restore_settings.cluster_host_ids, restore_settings.shard_num, restore_settings.replica_num),
        timeout_for_restoring_metadata);
}


void restoreData(RestoreTasks & restore_tasks, ThreadPool & thread_pool)
{
    std::deque<std::unique_ptr<IRestoreTask>> tasks(std::make_move_iterator(restore_tasks.begin()), std::make_move_iterator(restore_tasks.end()));
    restore_tasks.clear();

    /// Non-sequential tasks.
    size_t num_active_jobs = 0;
    std::mutex mutex;
    std::condition_variable event;
    std::exception_ptr exception;

    while (true)
    {
        std::unique_ptr<IRestoreTask> current_task;
        {
            std::unique_lock lock{mutex};
            event.wait(lock, [&] { return !tasks.empty() || exception || !num_active_jobs; });
            if ((tasks.empty() && !num_active_jobs) || exception)
                break;
            current_task = std::move(tasks.front());
            tasks.pop_front();
            ++num_active_jobs;
        }

        auto job = [current_task = std::shared_ptr<IRestoreTask>(std::move(current_task)), &tasks, &num_active_jobs, &exception, &mutex, &event]() mutable
        {
            SCOPE_EXIT({
                --num_active_jobs;
                event.notify_all();
            });

            {
                std::lock_guard lock{mutex};
                if (exception)
                    return;
            }

            RestoreTasks new_tasks;
            try
            {
                new_tasks = current_task->run();
            }
            catch (...)
            {
                std::lock_guard lock{mutex};
                if (!exception)
                    exception = std::current_exception();
            }

            {
                std::lock_guard lock{mutex};
                tasks.insert(tasks.end(), std::make_move_iterator(new_tasks.begin()), std::make_move_iterator(new_tasks.end()));
            }
        };

        if (!thread_pool.trySchedule(job))
            job();
    }

    {
        std::unique_lock lock{mutex};
        event.wait(lock, [&] { return !num_active_jobs; });
    }

    if (exception)
        std::rethrow_exception(exception);
}


/// Returns access required to execute RESTORE query.
AccessRightsElements getRequiredAccessToRestore(const ASTBackupQuery::Elements & elements, const RestoreSettings & restore_settings)
{
    AccessRightsElements required_access;
    for (const auto & element : elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::TABLE:
            {
                if (element.is_temp_db)
                {
                    if (restore_settings.create_table != RestoreTableCreationMode::kMustExist)
                        required_access.emplace_back(AccessType::CREATE_TEMPORARY_TABLE);
                    break;
                }
                AccessFlags flags = AccessType::SHOW_TABLES;
                if (restore_settings.create_table != RestoreTableCreationMode::kMustExist)
                    flags |= AccessType::CREATE_TABLE;
                if (!restore_settings.structure_only)
                    flags |= AccessType::INSERT;
                required_access.emplace_back(flags, element.new_name.first, element.new_name.second);
                break;
            }
            case ASTBackupQuery::DATABASE:
            {
                if (element.is_temp_db)
                {
                    if (restore_settings.create_table != RestoreTableCreationMode::kMustExist)
                        required_access.emplace_back(AccessType::CREATE_TEMPORARY_TABLE);
                    break;
                }
                AccessFlags flags = AccessType::SHOW_TABLES | AccessType::SHOW_DATABASES;
                if (restore_settings.create_table != RestoreTableCreationMode::kMustExist)
                    flags |= AccessType::CREATE_TABLE;
                if (restore_settings.create_database != RestoreDatabaseCreationMode::kMustExist)
                    flags |= AccessType::CREATE_DATABASE;
                if (!restore_settings.structure_only)
                    flags |= AccessType::INSERT;
                required_access.emplace_back(flags, element.new_name.first);
                break;
            }
            case ASTBackupQuery::ALL_DATABASES:
            {
                AccessFlags flags = AccessType::SHOW_TABLES | AccessType::SHOW_DATABASES;
                if (restore_settings.create_table != RestoreTableCreationMode::kMustExist)
                    flags |= AccessType::CREATE_TABLE;
                if (restore_settings.create_database != RestoreDatabaseCreationMode::kMustExist)
                    flags |= AccessType::CREATE_DATABASE;
                if (!restore_settings.structure_only)
                    flags |= AccessType::INSERT;
                required_access.emplace_back(flags);
                break;
            }
        }
    }
    return required_access;
}

}
