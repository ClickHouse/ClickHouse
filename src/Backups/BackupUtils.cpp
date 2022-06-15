#include <Backups/BackupUtils.h>
#include <Backups/IBackup.h>
#include <Backups/RestoreSettings.h>
#include <Access/Common/AccessRightsElement.h>
#include <Databases/DDLRenamingVisitor.h>
#include <Interpreters/DatabaseCatalog.h>


namespace DB
{

DDLRenamingMap makeRenamingMapFromBackupQuery(const ASTBackupQuery::Elements & elements)
{
    DDLRenamingMap map;

    for (const auto & element : elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::TABLE:
            {
                const String & table_name = element.table_name;
                const String & database_name = element.database_name;
                const String & new_table_name = element.new_table_name;
                const String & new_database_name = element.new_database_name;
                assert(!table_name.empty());
                assert(!new_table_name.empty());
                assert(!database_name.empty());
                assert(!new_database_name.empty());
                map.setNewTableName({database_name, table_name}, {new_database_name, new_table_name});
                break;
            }

            case ASTBackupQuery::TEMPORARY_TABLE:
            {
                const String & table_name = element.table_name;
                const String & new_table_name = element.new_table_name;
                assert(!table_name.empty());
                assert(!new_table_name.empty());
                map.setNewTemporaryTableName(table_name, new_table_name);
                break;
            }

            case ASTBackupQuery::DATABASE:
            {
                const String & database_name = element.database_name;
                const String & new_database_name = element.new_database_name;
                assert(!database_name.empty());
                assert(!new_database_name.empty());
                map.setNewDatabaseName(database_name, new_database_name);
                break;
            }

            case ASTBackupQuery::ALL: break;
        }
    }
    return map;
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
}


void restoreTablesData(DataRestoreTasks && tasks, ThreadPool & thread_pool)
{
    size_t num_active_jobs = 0;
    std::mutex mutex;
    std::condition_variable event;
    std::exception_ptr exception;

    for (auto & task : tasks)
    {
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
                std::move(task)();
            }
            catch (...)
            {
                std::lock_guard lock{mutex};
                if (!exception)
                    exception = std::current_exception();
            }
        };

        if (!thread_pool.trySchedule(job))
            job();
    }

    {
        std::unique_lock lock{mutex};
        event.wait(lock, [&] { return !num_active_jobs; });
    }

    tasks.clear();

    if (exception)
    {
        /// We don't call finalizeWriting() if an error occurs.
        /// And IBackup's implementation should remove the backup in its destructor if finalizeWriting() hasn't called before.
        std::rethrow_exception(exception);
    }
}


/// Returns access required to execute BACKUP query.
AccessRightsElements getRequiredAccessToBackup(const ASTBackupQuery::Elements & elements)
{
    AccessFlags backup_and_show_tables_flags = AccessType::BACKUP | AccessType::SHOW_TABLES;
    AccessRightsElements required_access;
    for (const auto & element : elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::TABLE:
            {
                required_access.emplace_back(backup_and_show_tables_flags, element.database_name, element.table_name);

                if (element.database_name == "system")
                {
                    if (element.table_name == "users")
                        required_access.emplace_back(AccessType::SHOW_USERS);
                    else if (element.table_name == "roles")
                        required_access.emplace_back(AccessType::SHOW_ROLES);
                    else if (element.table_name == "settings_profiles")
                        required_access.emplace_back(AccessType::SHOW_SETTINGS_PROFILES);
                    else if (element.table_name == "row_policies")
                        required_access.emplace_back(AccessType::SHOW_ROW_POLICIES);
                    else if (element.table_name == "quotas")
                        required_access.emplace_back(AccessType::SHOW_QUOTAS);
                }
                break;
            }
            
            case ASTBackupQuery::TEMPORARY_TABLE:
            {
                /// It's always allowed to backup temporary tables.
                break;
            }
            
            case ASTBackupQuery::DATABASE:
            {
                /// TODO: It's better to process `element.except_tables` somehow.
                required_access.emplace_back(backup_and_show_tables_flags, element.database_name);

                if (element.database_name == "system")
                    required_access.emplace_back(AccessType::SHOW_ACCESS);

                break;
            }
            
            case ASTBackupQuery::ALL:
            {
                /// TODO: It's better to process `element.except_databases` & `element.except_tables` somehow.
                required_access.emplace_back(backup_and_show_tables_flags);
                required_access.emplace_back(AccessType::SHOW_ACCESS);
                break;
            }
        }
    }
    return required_access;
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
                AccessFlags flags = AccessType::SHOW_TABLES;
                if (restore_settings.create_table != RestoreTableCreationMode::kMustExist)
                    flags |= AccessType::CREATE_TABLE;
                if (!restore_settings.structure_only)
                    flags |= AccessType::INSERT;
                required_access.emplace_back(flags, element.new_database_name, element.new_table_name);

                if (element.database_name == "system")
                {
                    if ((element.table_name == "users") || (element.table_name == "roles"))
                    {
                        AccessRightsElement everything_with_grant_option{AccessType::ALL}; /// Users and roles can come with any grants.
                        everything_with_grant_option.grant_option = true;
                        required_access.emplace_back(everything_with_grant_option);
                    }
                    else if (element.table_name == "settings_profiles")
                        required_access.emplace_back(AccessType::CREATE_SETTINGS_PROFILE);
                    else if (element.table_name == "row_policies")
                        required_access.emplace_back(AccessType::CREATE_ROW_POLICY);
                    else if (element.table_name == "quotas")
                        required_access.emplace_back(AccessType::CREATE_QUOTA);
                }
                break;
            }
            
            case ASTBackupQuery::TEMPORARY_TABLE:
            {
                if (restore_settings.create_table != RestoreTableCreationMode::kMustExist)
                    required_access.emplace_back(AccessType::CREATE_TEMPORARY_TABLE);
                break;
            }
            
            case ASTBackupQuery::DATABASE:
            {
                AccessFlags flags = AccessType::SHOW_TABLES | AccessType::SHOW_DATABASES;
                if (restore_settings.create_table != RestoreTableCreationMode::kMustExist)
                    flags |= AccessType::CREATE_TABLE;
                if (restore_settings.create_database != RestoreDatabaseCreationMode::kMustExist)
                    flags |= AccessType::CREATE_DATABASE;
                if (!restore_settings.structure_only)
                    flags |= AccessType::INSERT;
                /// TODO: It's better to process `element.except_tables` somehow.
                required_access.emplace_back(flags, element.new_database_name);

                if (element.database_name == "system")
                {
                    AccessRightsElement everything_with_grant_option{AccessType::ALL}; /// Users and roles can come with any grants.
                    everything_with_grant_option.grant_option = true;
                    required_access.emplace_back(everything_with_grant_option);
                }

                break;
            }
            
            case ASTBackupQuery::ALL:
            {
                AccessFlags flags = AccessType::SHOW_TABLES | AccessType::SHOW_DATABASES;
                if (restore_settings.create_table != RestoreTableCreationMode::kMustExist)
                    flags |= AccessType::CREATE_TABLE;
                if (restore_settings.create_database != RestoreDatabaseCreationMode::kMustExist)
                    flags |= AccessType::CREATE_DATABASE;
                if (!restore_settings.structure_only)
                    flags |= AccessType::INSERT;
                /// TODO: It's better to process `element.except_databases` & `element.except_tables` somehow.
                required_access.emplace_back(flags);

                {
                    AccessRightsElement everything_with_grant_option{AccessType::ALL}; /// Users and roles can come with any grants.
                    everything_with_grant_option.grant_option = true;
                    required_access.emplace_back(everything_with_grant_option);
                }

                break;
            }
        }
    }
    return required_access;
}

}
