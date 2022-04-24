#include <Backups/BackupUtils.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupSettings.h>
#include <Backups/DDLCompareUtils.h>
#include <Backups/DDLRenamingVisitor.h>
#include <Backups/IBackup.h>
#include <Backups/formatTableNameOrTemporaryTableName.h>
#include <Common/escapeForFileName.h>
#include <Access/Common/AccessFlags.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
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
                        prepareToBackupTable(DatabaseAndTableName{database_name, table_name}, element.partitions);
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
            /// Check that there are not `different_create_query`. (If it's set it means error.)
            for (const auto & info : databases | boost::adaptors::map_values)
            {
                if (info.different_create_query)
                    throw Exception(ErrorCodes::CANNOT_BACKUP_DATABASE,
                                    "Cannot backup a database because two different create queries were generated for it: {} and {}",
                                    serializeAST(*info.create_query), serializeAST(*info.different_create_query));
            }

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
                        String data_path = getDataPathInBackup(*info.create_query);
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
            DatabaseAndTableName new_table_name = renaming_settings.getNewTableName(table_name_);
            if (tables.contains(new_table_name))
                throw Exception(ErrorCodes::CANNOT_BACKUP_TABLE, "Cannot backup the {} twice", formatTableNameOrTemporaryTableName(new_table_name));

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
            info.name_in_backup = new_table_name;
            info.partitions = partitions_;
            info.has_data = has_data;
            tables[new_table_name] = std::move(info);

            /// If it's not system or temporary database then probably we need to backup the database's definition too.
            if (!isSystemOrTemporaryDatabase(table_name_.first))
            {
                if (!databases.contains(new_table_name.first))
                {
                    /// Add a create query to backup the database if we haven't done it yet.
                    auto create_db_query = prepareCreateQueryForBackup(database->getCreateDatabaseQuery());
                    create_db_query->setDatabase(new_table_name.first);

                    CreateDatabaseInfo info_db;
                    info_db.create_query = create_db_query;
                    info_db.original_name = table_name_.first;
                    info_db.is_explicit = false;
                    databases[new_table_name.first] = std::move(info_db);
                }
                else
                {
                    /// We already have added a create query to backup the database,
                    /// set `different_create_query` if it's not the same.
                    auto & info_db = databases[new_table_name.first];
                    if (!info_db.is_explicit && (info_db.original_name != table_name_.first) && !info_db.different_create_query)
                    {
                        auto create_db_query = prepareCreateQueryForBackup(table_.first->getCreateDatabaseQuery());
                        create_db_query->setDatabase(new_table_name.first);
                        if (!areDatabaseDefinitionsSame(*info_db.create_query, *create_db_query))
                            info_db.different_create_query = create_db_query;
                    }
                }
            }
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
            String new_database_name = renaming_settings.getNewDatabaseName(database_name_);
            if (databases.contains(new_database_name) && databases[new_database_name].is_explicit)
                throw Exception(ErrorCodes::CANNOT_BACKUP_DATABASE, "Cannot backup the database {} twice", backQuoteIfNeed(new_database_name));

            /// Of course we're not going to backup the definition of the system or the temporary database.
            if (!isSystemOrTemporaryDatabase(database_name_))
            {
                /// Make a create query for this database.
                auto create_db_query = prepareCreateQueryForBackup(database_->getCreateDatabaseQuery());

                CreateDatabaseInfo info_db;
                info_db.create_query = create_db_query;
                info_db.original_name = database_name_;
                info_db.is_explicit = true;
                databases[new_database_name] = std::move(info_db);
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
            create_query->uuid = UUIDHelpers::Nil;
            create_query->to_inner_uuid = UUIDHelpers::Nil;
            return create_query;
        }

        static bool isSystemOrTemporaryDatabase(const String & database_name)
        {
            return (database_name == DatabaseCatalog::SYSTEM_DATABASE) || (database_name == DatabaseCatalog::TEMPORARY_DATABASE);
        }

        static std::pair<String, BackupEntryPtr> makeBackupEntryForMetadata(const IAST & create_query)
        {
            auto metadata_entry = std::make_unique<BackupEntryFromMemory>(serializeAST(create_query));
            String metadata_path = getMetadataPathInBackup(create_query);
            return {metadata_path, std::move(metadata_entry)};
        }

        /// Information which is used to make an instance of RestoreTableFromBackupTask.
        struct CreateTableInfo
        {
            ASTPtr create_query;
            StoragePtr storage;
            DatabaseAndTableName name_in_backup;
            ASTs partitions;
            bool has_data = false;
        };

        /// Information which is used to make an instance of RestoreDatabaseFromBackupTask.
        struct CreateDatabaseInfo
        {
            ASTPtr create_query;
            String original_name;

            /// Whether the creation of this database is specified explicitly, via RESTORE DATABASE or
            /// RESTORE ALL DATABASES.
            /// It's false if the creation of this database is caused by creating a table contained in it.
            bool is_explicit = false;

            /// If this is set it means the following error:
            /// it means that for implicitly created database there were two different create query
            /// generated so we cannot restore the database.
            ASTPtr different_create_query;
        };

        ContextPtr context;
        BackupSettings backup_settings;
        DDLRenamingSettings renaming_settings;
        std::map<String, CreateDatabaseInfo> databases;
        std::map<DatabaseAndTableName, CreateTableInfo> tables;
    };
}


BackupEntries makeBackupEntries(const ContextPtr & context, const Elements & elements, const BackupSettings & backup_settings)
{
    BackupEntriesBuilder builder{context, backup_settings};
    builder.prepare(elements);
    return builder.makeBackupEntries();
}


void writeBackupEntries(BackupMutablePtr backup, BackupEntries && backup_entries, size_t num_threads)
{
    if (!num_threads || !backup->supportsWritingInMultipleThreads())
        num_threads = 1;
    std::vector<ThreadFromGlobalPool> threads;
    size_t num_active_threads = 0;
    std::mutex mutex;
    std::condition_variable cond;
    std::exception_ptr exception;

    for (auto & name_and_entry : backup_entries)
    {
        auto & name = name_and_entry.first;
        auto & entry = name_and_entry.second;

        {
            std::unique_lock lock{mutex};
            if (exception)
                break;
            cond.wait(lock, [&] { return num_active_threads < num_threads; });
            if (exception)
                break;
            ++num_active_threads;
        }

        threads.emplace_back([backup, &name, &entry, &mutex, &cond, &num_active_threads, &exception]()
        {
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

            {
                std::lock_guard lock{mutex};
                --num_active_threads;
                cond.notify_all();
            }
        });
    }

    for (auto & thread : threads)
        thread.join();

    backup_entries.clear();

    if (exception)
    {
        /// We don't call finalizeWriting() if an error occurs.
        /// And IBackup's implementation should remove the backup in its destructor if finalizeWriting() hasn't called before.
        std::rethrow_exception(exception);
    }

    backup->finalizeWriting();
}


String getDataPathInBackup(const DatabaseAndTableName & table_name)
{
    if (table_name.first.empty() || table_name.second.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Database name and table name must not be empty");
    assert(!table_name.first.empty() && !table_name.second.empty());
    return String{"data/"} + escapeForFileName(table_name.first) + "/" + escapeForFileName(table_name.second) + "/";
}

String getDataPathInBackup(const IAST & create_query)
{
    const auto & create = create_query.as<const ASTCreateQuery &>();
    if (!create.table)
        return {};
    if (create.temporary)
        return getDataPathInBackup({DatabaseCatalog::TEMPORARY_DATABASE, create.getTable()});
    return getDataPathInBackup({create.getDatabase(), create.getTable()});
}

String getMetadataPathInBackup(const DatabaseAndTableName & table_name)
{
    if (table_name.first.empty() || table_name.second.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Database name and table name must not be empty");
    return String{"metadata/"} + escapeForFileName(table_name.first) + "/" + escapeForFileName(table_name.second) + ".sql";
}

String getMetadataPathInBackup(const String & database_name)
{
    if (database_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Database name must not be empty");
    return String{"metadata/"} + escapeForFileName(database_name) + ".sql";
}

String getMetadataPathInBackup(const IAST & create_query)
{
    const auto & create = create_query.as<const ASTCreateQuery &>();
    if (!create.table)
        return getMetadataPathInBackup(create.getDatabase());
    if (create.temporary)
        return getMetadataPathInBackup({DatabaseCatalog::TEMPORARY_DATABASE, create.getTable()});
    return getMetadataPathInBackup({create.getDatabase(), create.getTable()});
}

}
