#include <Backups/RestoreUtils.h>
#include <Backups/BackupUtils.h>
#include <Backups/DDLCompareUtils.h>
#include <Backups/DDLRenamingVisitor.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupEntry.h>
#include <Backups/IRestoreTask.h>
#include <Backups/RestoreSettings.h>
#include <Backups/formatTableNameOrTemporaryTableName.h>
#include <Common/escapeForFileName.h>
#include <Databases/IDatabase.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <boost/range/adaptor/reversed.hpp>
#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_TABLE;
    extern const int CANNOT_RESTORE_DATABASE;
}

namespace
{
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
            const RestoreSettingsPtr & restore_settings_,
            bool ignore_if_database_def_differs_)
            : context(context_)
            , create_query(typeid_cast<std::shared_ptr<ASTCreateQuery>>(create_query_))
            , restore_settings(restore_settings_)
            , ignore_if_database_def_differs(ignore_if_database_def_differs_)
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
            /// We need to call clone() for `create_query` because the interpreter can decide
            /// to change a passed AST a little bit.
            InterpreterCreateQuery create_interpreter{create_query->clone(), context};
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
            if (ignore_if_database_def_differs || !restore_settings->throw_if_database_def_differs)
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
        bool ignore_if_database_def_differs = false;
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
            const RestoreSettingsPtr & restore_settings_)
            : context(context_), create_query(typeid_cast<std::shared_ptr<ASTCreateQuery>>(create_query_)),
              partitions(partitions_), backup(backup_), table_name_in_backup(table_name_in_backup_),
              restore_settings(restore_settings_)
        {
            table_name = DatabaseAndTableName{create_query->getDatabase(), create_query->getTable()};
            if (create_query->temporary)
                table_name.first = DatabaseCatalog::TEMPORARY_DATABASE;
        }

        RestoreTasks run() override
        {
            createStorage();
            getStorage();
            checkStorageCreateQuery();
            RestoreTasks tasks;
            if (auto task = insertData())
                tasks.push_back(std::move(task));
            return tasks;
        }

        bool isSequential() const override { return true; }

    private:
        void createStorage()
        {
            /// We need to call clone() for `create_query` because the interpreter can decide
            /// to change a passed AST a little bit.
            InterpreterCreateQuery create_interpreter{create_query->clone(), context};
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
            if (!restore_settings->throw_if_table_def_differs)
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

            data_path_in_backup = getDataPathInBackup(table_name_in_backup);
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
            return storage->restoreData(context, partitions, backup, data_path_in_backup, *restore_settings);
        }

        ContextMutablePtr context;
        std::shared_ptr<ASTCreateQuery> create_query;
        DatabaseAndTableName table_name;
        ASTs partitions;
        BackupPtr backup;
        DatabaseAndTableName table_name_in_backup;
        RestoreSettingsPtr restore_settings;
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
            : context(context_), backup(backup_), restore_settings(restore_settings_) {}

        /// Prepares internal structures for making tasks for restoring.
        void prepare(const ASTBackupQuery::Elements & elements)
        {
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
            /// Check that there are not `different_create_query`. (If it's set it means error.)
            for (const auto & info : databases | boost::adaptors::map_values)
            {
                if (info.different_create_query)
                    throw Exception(ErrorCodes::CANNOT_RESTORE_DATABASE,
                                    "Cannot restore a database because two different create queries were generated for it: {} and {}",
                                    serializeAST(*info.create_query), serializeAST(*info.different_create_query));
            }

            auto restore_settings_ptr = std::make_shared<const RestoreSettings>(restore_settings);

            RestoreTasks res;
            for (const auto & info : databases | boost::adaptors::map_values)
                res.push_back(std::make_unique<RestoreDatabaseTask>(context, info.create_query, restore_settings_ptr,
                                                                    /* ignore_if_database_def_differs = */ !info.is_explicit));

            /// TODO: We need to restore tables according to their dependencies.
            for (const auto & info : tables | boost::adaptors::map_values)
                res.push_back(std::make_unique<RestoreTableTask>(context, info.create_query, info.partitions, backup, info.name_in_backup, restore_settings_ptr));

            return res;
        }

    private:
        /// Prepares to restore a single table and probably its database's definition.
        void prepareToRestoreTable(const DatabaseAndTableName & table_name_, const ASTs & partitions_)
        {
            /// Check that we are not trying to restore the same table again.
            DatabaseAndTableName new_table_name = renaming_settings.getNewTableName(table_name_);
            if (tables.contains(new_table_name))
                throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "Cannot restore the {} twice", formatTableNameOrTemporaryTableName(new_table_name));

            /// Make a create query for this table.
            auto create_query = renameInCreateQuery(readCreateQueryFromBackup(table_name_));
            create_query->if_not_exists = !restore_settings.throw_if_table_exists;

            CreateTableInfo info;
            info.create_query = create_query;
            info.name_in_backup = table_name_;
            info.partitions = partitions_;
            tables[new_table_name] = std::move(info);

            /// If it's not system or temporary database then probably we need to restore the database's definition too.
            if (!isSystemOrTemporaryDatabase(new_table_name.first))
            {
                if (!databases.contains(new_table_name.first))
                {
                    /// Add a create query for restoring the database if we haven't done it yet.
                    std::shared_ptr<ASTCreateQuery> create_db_query;
                    String db_name_in_backup = table_name_.first;
                    if (hasCreateQueryInBackup(db_name_in_backup))
                    {
                        create_db_query = renameInCreateQuery(readCreateQueryFromBackup(db_name_in_backup));
                    }
                    else
                    {
                        create_db_query = std::make_shared<ASTCreateQuery>();
                        db_name_in_backup.clear();
                    }
                    create_db_query->setDatabase(new_table_name.first);
                    create_db_query->if_not_exists = true;

                    CreateDatabaseInfo info_db;
                    info_db.create_query = create_db_query;
                    info_db.name_in_backup = std::move(db_name_in_backup);
                    info_db.is_explicit = false;
                    databases[new_table_name.first] = std::move(info_db);
                }
                else
                {
                    /// We already have added a create query for restoring the database,
                    /// set `different_create_query` if it's not the same.
                    auto & info_db = databases[new_table_name.first];
                    if (!info_db.is_explicit && (info_db.name_in_backup != table_name_.first) && !info_db.different_create_query)
                    {
                        std::shared_ptr<ASTCreateQuery> create_db_query;
                        if (hasCreateQueryInBackup(table_name_.first))
                            create_db_query = renameInCreateQuery(readCreateQueryFromBackup(table_name_.first));
                        else
                            create_db_query = std::make_shared<ASTCreateQuery>();
                        create_db_query->setDatabase(new_table_name.first);
                        create_db_query->if_not_exists = true;
                        if (!areDatabaseDefinitionsSame(*info_db.create_query, *create_db_query))
                            info_db.different_create_query = create_db_query;
                    }
                }
            }
        }

        /// Prepares to restore a database and all tables in it.
        void prepareToRestoreDatabase(const String & database_name_, const std::set<String> & except_list_)
        {
            /// Check that we are not trying to restore the same database again.
            String new_database_name = renaming_settings.getNewDatabaseName(database_name_);
            if (databases.contains(new_database_name) && databases[new_database_name].is_explicit)
                throw Exception(ErrorCodes::CANNOT_RESTORE_DATABASE, "Cannot restore the database {} twice", backQuoteIfNeed(new_database_name));

            Strings table_metadata_filenames = backup->listFiles("metadata/" + escapeForFileName(database_name_) + "/", "/");

            bool throw_if_no_create_database_query = table_metadata_filenames.empty();
            if (throw_if_no_create_database_query && !hasCreateQueryInBackup(database_name_))
                throw Exception(ErrorCodes::CANNOT_RESTORE_DATABASE, "Cannot restore the database {} because there is no such database in the backup", backQuoteIfNeed(database_name_));

            /// Of course we're not going to restore the definition of the system or the temporary database.
            if (!isSystemOrTemporaryDatabase(new_database_name))
            {
                /// Make a create query for this database.
                std::shared_ptr<ASTCreateQuery> create_db_query;
                String db_name_in_backup = database_name_;
                if (hasCreateQueryInBackup(db_name_in_backup))
                {
                    create_db_query = renameInCreateQuery(readCreateQueryFromBackup(db_name_in_backup));
                }
                else
                {
                    create_db_query = std::make_shared<ASTCreateQuery>();
                    create_db_query->setDatabase(database_name_);
                    db_name_in_backup.clear();
                }

                create_db_query->if_not_exists = !restore_settings.throw_if_database_exists;

                CreateDatabaseInfo info_db;
                info_db.create_query = create_db_query;
                info_db.name_in_backup = std::move(db_name_in_backup);
                info_db.is_explicit = true;
                databases[new_database_name] = std::move(info_db);
            }

            /// Restore tables in this database.
            for (const String & table_metadata_filename : table_metadata_filenames)
            {
                String table_name = unescapeForFileName(fs::path{table_metadata_filename}.stem());
                if (except_list_.contains(table_name))
                    continue;
                prepareToRestoreTable(DatabaseAndTableName{database_name_, table_name}, ASTs{});
            }
        }

        /// Prepares to restore all the databases contained in the backup.
        void prepareToRestoreAllDatabases(const std::set<String> & except_list_)
        {
            Strings database_metadata_filenames = backup->listFiles("metadata/", "/");
            for (const String & database_metadata_filename : database_metadata_filenames)
            {
                String database_name = unescapeForFileName(fs::path{database_metadata_filename}.stem());
                if (except_list_.contains(database_name))
                    continue;
                prepareToRestoreDatabase(database_name, std::set<String>{});
            }
        }

        /// Reads a create query for creating a specified table from the backup.
        std::shared_ptr<ASTCreateQuery> readCreateQueryFromBackup(const DatabaseAndTableName & table_name) const
        {
            String create_query_path = getMetadataPathInBackup(table_name);
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
            String create_query_path = getMetadataPathInBackup(database_name);
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
            String create_query_path = getMetadataPathInBackup(database_name);
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
            String name_in_backup;

            /// Whether the creation of this database is specified explicitly, via RESTORE DATABASE or
            /// RESTORE ALL DATABASES.
            /// It's false if the creation of this database is caused by creating a table contained in it.
            bool is_explicit = false;

            /// If this is set it means the following error:
            /// it means that for implicitly created database there were two different create query
            /// generated so we cannot restore the database.
            ASTPtr different_create_query;
        };

        ContextMutablePtr context;
        BackupPtr backup;
        RestoreSettings restore_settings;
        DDLRenamingSettings renaming_settings;
        std::map<String, CreateDatabaseInfo> databases;
        std::map<DatabaseAndTableName, CreateTableInfo> tables;
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

}
