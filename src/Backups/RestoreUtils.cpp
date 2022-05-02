#include <Backups/RestoreUtils.h>
#include <Backups/BackupUtils.h>
#include <Backups/RestoreSettings.h>
#include <Backups/DDLCompareUtils.h>
#include <Backups/DDLRenamingVisitor.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupEntry.h>
#include <Backups/IRestoreTask.h>
#include <Backups/RestoreCoordinationDistributed.h>
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
#include <base/chrono_io.h>
#include <base/insertAtEnd.h>
#include <base/sleep.h>
#include <boost/range/adaptor/reversed.hpp>
#include <boost/range/algorithm_ext/erase.hpp>
#include <filesystem>


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
        explicit PathsInBackup(const IBackup & backup_) : backup(backup_) {}

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
            insertAtEnd(res, backup.listFiles(fmt::format("shards/{}/replicas/{}/metadata/{}/", shard_index, replica_index, escaped_database_name)));
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
        RestoreDatabaseTask(
            ContextMutablePtr context_,
            const ASTPtr & create_query_,
            const RestoreSettingsPtr & restore_settings_)
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

        bool isSequential() const override { return true; }

    private:
        void createDatabase()
        {
            if (restore_settings->create_database == RestoreDatabaseCreationMode::kMustExist)
                return;

            auto cloned_create_query = typeid_cast<std::shared_ptr<ASTCreateQuery>>(create_query->clone());
            cloned_create_query->if_not_exists = (restore_settings->create_database == RestoreDatabaseCreationMode::kCreateIfNotExists);
            InterpreterCreateQuery create_interpreter{cloned_create_query, context};
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


    /// Restores a table and fills it with data.
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
            const std::shared_ptr<IRestoreCoordination> & restore_coordination_)
            : context(context_), create_query(typeid_cast<std::shared_ptr<ASTCreateQuery>>(create_query_)),
              partitions(partitions_), backup(backup_), table_name_in_backup(table_name_in_backup_),
              restore_settings(restore_settings_), restore_coordination(restore_coordination_)
        {
            table_name = DatabaseAndTableName{create_query->getDatabase(), create_query->getTable()};
            if (create_query->temporary)
                table_name.first = DatabaseCatalog::TEMPORARY_DATABASE;
        }

        RestoreTasks run() override
        {
            if (acquireTableCreation())
            {
                try
                {
                    createStorage();
                    getStorage();
                    checkStorageCreateQuery();
                    setTableCreationResult(IRestoreCoordination::Result::SUCCEEDED);
                }
                catch (...)
                {
                    setTableCreationResult(IRestoreCoordination::Result::FAILED);
                    throw;
                }
            }
            else
            {
                waitForTableCreation();
                getStorage();
                checkStorageCreateQuery();
            }

            RestoreTasks tasks;
            if (auto task = insertData())
                tasks.push_back(std::move(task));
            return tasks;
        }

        bool isSequential() const override { return true; }

    private:
        bool acquireTableCreation()
        {
            if (restore_settings->create_table == RestoreTableCreationMode::kMustExist)
                return true;

            auto replicated_db
                = typeid_cast<std::shared_ptr<const DatabaseReplicated>>(DatabaseCatalog::instance().getDatabase(table_name.first));
            if (!replicated_db)
                return true;

            use_coordination_for_table_creation = true;
            replicated_database_zookeeper_path = replicated_db->getZooKeeperPath();
            return restore_coordination->acquireZkPathAndName(replicated_database_zookeeper_path, table_name.second);
        }

        void setTableCreationResult(IRestoreCoordination::Result res)
        {
            if (use_coordination_for_table_creation)
                restore_coordination->setResultForZkPathAndName(replicated_database_zookeeper_path, table_name.second, res);
        }

        void waitForTableCreation()
        {
            if (!use_coordination_for_table_creation)
                return;

            IRestoreCoordination::Result res;
            const auto & config = context->getConfigRef();
            auto timeout = std::chrono::seconds(config.getUInt("backups.create_table_in_replicated_db_timeout", 10));
            auto start_time = std::chrono::steady_clock::now();

            if (!restore_coordination->getResultForZkPathAndName(replicated_database_zookeeper_path, table_name.second, res, timeout))
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Waited too long ({}) for creating of {} on another replica",
                    to_string(timeout),
                    formatTableNameOrTemporaryTableName(table_name));

            if (res == IRestoreCoordination::Result::FAILED)
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Failed creating of {} on another replica",
                    formatTableNameOrTemporaryTableName(table_name));

            while (std::chrono::steady_clock::now() - start_time < timeout)
            {
                if (DatabaseCatalog::instance().tryGetDatabaseAndTable({table_name.first, table_name.second}, context).second)
                    return;
                sleepForMilliseconds(50);
            }

            throw Exception(
                ErrorCodes::CANNOT_RESTORE_TABLE,
                "Waited too long ({}) for creating of {} on another replica",
                to_string(timeout),
                formatTableNameOrTemporaryTableName(table_name));
        }

        void createStorage()
        {
            if (restore_settings->create_table == RestoreTableCreationMode::kMustExist)
                return;

            auto cloned_create_query = typeid_cast<std::shared_ptr<ASTCreateQuery>>(create_query->clone());
            cloned_create_query->if_not_exists = (restore_settings->create_table == RestoreTableCreationMode::kCreateIfNotExists);
            InterpreterCreateQuery create_interpreter{cloned_create_query, context};
            create_interpreter.setInternal(true);
            create_interpreter.execute();
        }

        StoragePtr getStorage()
        {
            if (!storage)
                std::tie(database, storage) = DatabaseCatalog::instance().getDatabaseAndTable({table_name.first, table_name.second}, context);
            return storage;
        }

        ASTPtr getStorageCreateQuery()
        {
            if (!storage_create_query)
            {
                getStorage();
                storage_create_query = database->getCreateTableQuery(table_name.second, context);
            }
            return storage_create_query;
        }

        void checkStorageCreateQuery()
        {
            if (restore_settings->allow_different_table_def)
                return;

            getStorageCreateQuery();
            if (areTableDefinitionsSame(*create_query, *storage_create_query))
                return;

            throw Exception(
                ErrorCodes::CANNOT_RESTORE_TABLE,
                "The {} already exists but has a different definition: {}, "
                "compare to its definition in the backup: {}",
                formatTableNameOrTemporaryTableName(table_name),
                serializeAST(*storage_create_query),
                serializeAST(*create_query));
        }

        bool hasData()
        {
            if (has_data)
                return *has_data;

            has_data = false;
            if (restore_settings->structure_only)
                return false;

            data_path_in_backup = PathsInBackup{*backup}.getDataPath(table_name_in_backup, restore_settings->shard_num_in_backup, restore_settings->replica_num_in_backup);
            if (backup->listFiles(data_path_in_backup).empty())
                return false;

            getStorageCreateQuery();
            if (!areTableDataCompatible(*create_query, *storage_create_query))
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

            /// We check for INSERT privilege only if we're going to write into table.
            context->checkAccess(AccessType::INSERT, table_name.first, table_name.second);

            has_data = true;
            return true;
        }

        RestoreTaskPtr insertData()
        {
            if (!hasData())
                return {};

            return storage->restoreData(context, partitions, backup, data_path_in_backup, *restore_settings, restore_coordination);
        }

        ContextMutablePtr context;
        std::shared_ptr<ASTCreateQuery> create_query;
        DatabaseAndTableName table_name;
        ASTs partitions;
        BackupPtr backup;
        DatabaseAndTableName table_name_in_backup;
        RestoreSettingsPtr restore_settings;
        std::shared_ptr<IRestoreCoordination> restore_coordination;
        bool use_coordination_for_table_creation = false;
        String replicated_database_zookeeper_path;
        DatabasePtr database;
        StoragePtr storage;
        ASTPtr storage_create_query;
        std::optional<bool> has_data;
        String data_path_in_backup;
    };


    /// Makes tasks for restoring databases and tables according to the elements of ASTBackupQuery.
    /// Keep this class consistent with BackupEntriesBuilder.
    class RestoreTasksBuilder
    {
    public:
        RestoreTasksBuilder(ContextMutablePtr context_, const BackupPtr & backup_, const RestoreSettings & restore_settings_)
            : context(context_), backup(backup_), restore_settings(restore_settings_)
        {
            if (!restore_settings.coordination_zk_path.empty())
                restore_coordination = std::make_shared<RestoreCoordinationDistributed>(restore_settings.coordination_zk_path, [context=context] { return context->getZooKeeper(); });
        }

        /// Prepares internal structures for making tasks for restoring.
        void prepare(const ASTBackupQuery::Elements & elements)
        {
            adjustIndicesOfSourceShardAndReplicaInBackup();

            String current_database = context->getCurrentDatabase();
            renaming_settings.setFromBackupQuery(elements, current_database);

            for (const auto & element : elements)
            {
                switch (element.type)
                {
                    case ElementType::TABLE:
                    {
                        const String & table_name = element.name.second;
                        String database_name = element.name.first;
                        if (database_name.empty())
                            database_name = current_database;
                        prepareToRestoreTable(DatabaseAndTableName{database_name, table_name}, element.partitions);
                        break;
                    }

                    case ElementType::DATABASE:
                    {
                        const String & database_name = element.name.first;
                        prepareToRestoreDatabase(database_name, element.except_list);
                        break;
                    }

                    case ElementType::ALL_DATABASES:
                    {
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
                res.push_back(std::make_unique<RestoreTableTask>(context, info.create_query, info.partitions, backup, info.name_in_backup, restore_settings_ptr, restore_coordination));

            return res;
        }

    private:
        void adjustIndicesOfSourceShardAndReplicaInBackup()
        {
            auto shards_in_backup = PathsInBackup{*backup}.getShards();
            if (!restore_settings.shard_num_in_backup)
            {
                if (shards_in_backup.size() == 1)
                    restore_settings.shard_num_in_backup = shards_in_backup[0];
                else
                    restore_settings.shard_num_in_backup = restore_settings.shard_num;
            }

            if (std::find(shards_in_backup.begin(), shards_in_backup.end(), restore_settings.shard_num_in_backup) == shards_in_backup.end())
                throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "No shard #{} in backup", restore_settings.shard_num_in_backup);

            auto replicas_in_backup = PathsInBackup{*backup}.getReplicas(restore_settings.shard_num_in_backup);
            if (!restore_settings.replica_num_in_backup)
            {
                if (replicas_in_backup.size() == 1)
                    restore_settings.replica_num_in_backup = replicas_in_backup[0];
                else
                    restore_settings.replica_num_in_backup = restore_settings.replica_num;
            }

            if (std::find(replicas_in_backup.begin(), replicas_in_backup.end(), restore_settings.replica_num_in_backup) == replicas_in_backup.end())
                throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "No replica #{} in backup", restore_settings.replica_num_in_backup);
        }

        /// Prepares to restore a single table and probably its database's definition.
        void prepareToRestoreTable(const DatabaseAndTableName & table_name_, const ASTs & partitions_)
        {
            /// Check that we are not trying to restore the same table again.
            DatabaseAndTableName new_table_name = renaming_settings.getNewTableName(table_name_);
            if (tables.contains(new_table_name))
                throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "Cannot restore the {} twice", formatTableNameOrTemporaryTableName(new_table_name));

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
                throw Exception(ErrorCodes::CANNOT_RESTORE_DATABASE, "Cannot restore the database {} twice", backQuoteIfNeed(new_database_name));

            Strings table_names = PathsInBackup{*backup}.getTables(database_name_, restore_settings.shard_num_in_backup, restore_settings.replica_num_in_backup);
            bool has_tables_in_backup = !table_names.empty();
            bool has_create_query_in_backup = hasCreateQueryInBackup(database_name_);

            if (!has_create_query_in_backup && !has_tables_in_backup)
                throw Exception(ErrorCodes::CANNOT_RESTORE_DATABASE, "Cannot restore the database {} because there is no such database in the backup", backQuoteIfNeed(database_name_));

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
            for (const String & database_name : PathsInBackup{*backup}.getDatabases(restore_settings.shard_num_in_backup, restore_settings.replica_num_in_backup))
            {
                if (except_list_.contains(database_name))
                    continue;
                prepareToRestoreDatabase(database_name, std::set<String>{});
            }
        }

        /// Reads a create query for creating a specified table from the backup.
        std::shared_ptr<ASTCreateQuery> readCreateQueryFromBackup(const DatabaseAndTableName & table_name) const
        {
            String create_query_path = PathsInBackup{*backup}.getMetadataPath(table_name, restore_settings.shard_num_in_backup, restore_settings.replica_num_in_backup);
            if (!backup->fileExists(create_query_path))
                throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "Cannot restore the {} because there is no such table in the backup",
                                formatTableNameOrTemporaryTableName(table_name));
            auto read_buffer = backup->readFile(create_query_path)->getReadBuffer();
            String create_query_str;
            readStringUntilEOF(create_query_str, *read_buffer);
            read_buffer.reset();
            ParserCreateQuery create_parser;
            return typeid_cast<std::shared_ptr<ASTCreateQuery>>(parseQuery(create_parser, create_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH));
        }

        /// Reads a create query for creating a specified database from the backup.
        std::shared_ptr<ASTCreateQuery> readCreateQueryFromBackup(const String & database_name) const
        {
            String create_query_path = PathsInBackup{*backup}.getMetadataPath(database_name, restore_settings.shard_num_in_backup, restore_settings.replica_num_in_backup);
            if (!backup->fileExists(create_query_path))
                throw Exception(ErrorCodes::CANNOT_RESTORE_DATABASE, "Cannot restore the database {} because there is no such database in the backup", backQuoteIfNeed(database_name));
            auto read_buffer = backup->readFile(create_query_path)->getReadBuffer();
            String create_query_str;
            readStringUntilEOF(create_query_str, *read_buffer);
            read_buffer.reset();
            ParserCreateQuery create_parser;
            return typeid_cast<std::shared_ptr<ASTCreateQuery>>(parseQuery(create_parser, create_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH));
        }

        /// Whether there is a create query for creating a specified database in the backup.
        bool hasCreateQueryInBackup(const String & database_name) const
        {
            String create_query_path = PathsInBackup{*backup}.getMetadataPath(database_name, restore_settings.shard_num_in_backup, restore_settings.replica_num_in_backup);
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
        DDLRenamingSettings renaming_settings;
        std::map<String /* new_db_name */, CreateDatabaseInfo> databases;
        std::map<DatabaseAndTableName /* new_table_name */, CreateTableInfo> tables;
    };


    /// Reverts completed restore tasks (in reversed order).
    void rollbackRestoreTasks(RestoreTasks && restore_tasks)
    {
        for (auto & restore_task : restore_tasks | boost::adaptors::reversed)
        {
            try
            {
                std::move(restore_task)->rollback();
            }
            catch (...)
            {
                tryLogCurrentException("Restore", "Couldn't rollback changes after failed RESTORE");
            }
        }
    }
}


RestoreTasks makeRestoreTasks(ContextMutablePtr context, const BackupPtr & backup, const Elements & elements, const RestoreSettings & restore_settings)
{
    RestoreTasksBuilder builder{context, backup, restore_settings};
    builder.prepare(elements);
    return builder.makeTasks();
}


void executeRestoreTasks(RestoreTasks && restore_tasks, size_t num_threads)
{
    if (!num_threads)
        num_threads = 1;

    RestoreTasks completed_tasks;
    bool need_rollback_completed_tasks = true;

    SCOPE_EXIT({
        if (need_rollback_completed_tasks)
            rollbackRestoreTasks(std::move(completed_tasks));
    });

    std::deque<std::unique_ptr<IRestoreTask>> sequential_tasks;
    std::deque<std::unique_ptr<IRestoreTask>> enqueued_tasks;

    /// There are two kinds of restore tasks: sequential and non-sequential ones.
    /// Sequential tasks are executed first and always in one thread.
    for (auto & task : restore_tasks)
    {
        if (task->isSequential())
            sequential_tasks.push_back(std::move(task));
        else
            enqueued_tasks.push_back(std::move(task));
    }

    /// Sequential tasks.
    while (!sequential_tasks.empty())
    {
        auto current_task = std::move(sequential_tasks.front());
        sequential_tasks.pop_front();

        RestoreTasks new_tasks = current_task->run();

        completed_tasks.push_back(std::move(current_task));
        for (auto & task : new_tasks)
        {
            if (task->isSequential())
                sequential_tasks.push_back(std::move(task));
            else
                enqueued_tasks.push_back(std::move(task));
        }
    }

    /// Non-sequential tasks.
    std::unordered_map<IRestoreTask *, std::unique_ptr<IRestoreTask>> running_tasks;
    std::vector<ThreadFromGlobalPool> threads;
    std::mutex mutex;
    std::condition_variable cond;
    std::exception_ptr exception;

    while (true)
    {
        IRestoreTask * current_task = nullptr;
        {
            std::unique_lock lock{mutex};
            cond.wait(lock, [&]
            {
                if (exception)
                    return true;
                if (enqueued_tasks.empty())
                    return running_tasks.empty();
                return (running_tasks.size() < num_threads);
            });

            if (exception || enqueued_tasks.empty())
                break;

            auto current_task_ptr = std::move(enqueued_tasks.front());
            current_task = current_task_ptr.get();
            enqueued_tasks.pop_front();
            running_tasks[current_task] = std::move(current_task_ptr);
        }

        assert(current_task);
        threads.emplace_back([current_task, &mutex, &cond, &enqueued_tasks, &running_tasks, &completed_tasks, &exception]() mutable
        {
            {
                std::lock_guard lock{mutex};
                if (exception)
                    return;
            }

            RestoreTasks new_tasks;
            std::exception_ptr new_exception;
            try
            {
                new_tasks = current_task->run();
            }
            catch (...)
            {
                new_exception = std::current_exception();
            }

            {
                std::lock_guard lock{mutex};
                auto current_task_it = running_tasks.find(current_task);
                auto current_task_ptr = std::move(current_task_it->second);
                running_tasks.erase(current_task_it);

                if (!new_exception)
                {
                    completed_tasks.push_back(std::move(current_task_ptr));
                    enqueued_tasks.insert(
                        enqueued_tasks.end(), std::make_move_iterator(new_tasks.begin()), std::make_move_iterator(new_tasks.end()));
                }

                if (!exception)
                    exception = new_exception;

                cond.notify_all();
            }
        });
    }

    for (auto & thread : threads)
        thread.join();

    if (exception)
        std::rethrow_exception(exception);
    else
        need_rollback_completed_tasks = false;
}


size_t getMinCountOfReplicas(const IBackup & backup)
{
    size_t min_count_of_replicas = static_cast<size_t>(-1);
    for (size_t shard_index : PathsInBackup(backup).getShards())
    {
        size_t count_of_replicas = PathsInBackup(backup).getReplicas(shard_index).size();
        min_count_of_replicas = std::min(min_count_of_replicas, count_of_replicas);
    }
    return min_count_of_replicas;
}

}
