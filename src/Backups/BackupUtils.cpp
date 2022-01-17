#include <Backups/BackupUtils.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupRenamingConfig.h>
#include <Backups/IBackup.h>
#include <Backups/renameInCreateQuery.h>
#include <Common/escapeForFileName.h>
#include <Access/Common/AccessFlags.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Storages/IStorage.h>
#include <base/insertAtEnd.h>
#include <base/sort.h>
#include <boost/range/adaptor/reversed.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int BACKUP_ELEMENT_DUPLICATE;
    extern const int BACKUP_IS_EMPTY;
    extern const int LOGICAL_ERROR;
}

namespace
{
    using Kind = ASTBackupQuery::Kind;
    using Element = ASTBackupQuery::Element;
    using Elements = ASTBackupQuery::Elements;
    using ElementType = ASTBackupQuery::ElementType;

    /// Replace elements of types DICTIONARY or EVERYTHING with elements of other types.
    void replaceElementTypesWithBaseElementTypes(Elements & elements)
    {
        for (size_t i = 0; i != elements.size(); ++i)
        {
            auto & element = elements[i];
            switch (element.type)
            {
                case ElementType::DICTIONARY:
                {
                    element.type = ElementType::TABLE;
                    break;
                }

                case ElementType::EVERYTHING:
                {
                    element.type = ElementType::ALL_DATABASES;
                    auto & new_element = elements.emplace_back();
                    new_element.type = ElementType::ALL_TEMPORARY_TABLES;
                    break;
                }

                default:
                    break;
            }
        }
    }

    /// Replaces an empty database with the current database.
    void replaceEmptyDatabaseWithCurrentDatabase(Elements & elements, const String & current_database)
    {
        for (auto & element : elements)
        {
            if (element.type == ElementType::TABLE)
            {
                if (element.name.first.empty() && !element.name.second.empty())
                    element.name.first = current_database;
                if (element.new_name.first.empty() && !element.new_name.second.empty())
                    element.new_name.first = current_database;
            }
        }
    }

    /// Replaces elements of types TEMPORARY_TABLE or ALL_TEMPORARY_TABLES with elements of type TABLE or DATABASE.
    void replaceTemporaryTablesWithTemporaryDatabase(Elements & elements)
    {
        for (auto & element : elements)
        {
            switch (element.type)
            {
                case ElementType::TEMPORARY_TABLE:
                {
                    element.type = ElementType::TABLE;
                    element.name.first = DatabaseCatalog::TEMPORARY_DATABASE;
                    if (element.new_name.first.empty() && !element.new_name.second.empty())
                        element.new_name.first = DatabaseCatalog::TEMPORARY_DATABASE;
                    break;
                }

                case ElementType::ALL_TEMPORARY_TABLES:
                {
                    element.type = ElementType::DATABASE;
                    element.name.first = DatabaseCatalog::TEMPORARY_DATABASE;
                    break;
                }

                default:
                    break;
            }
        }
    }

    /// Set new names if they are not specified.
    void setNewNamesIfNotSet(Elements & elements)
    {
        for (auto & element : elements)
        {
            switch (element.type)
            {
                case ElementType::TABLE:
                {
                    if (element.new_name.second.empty())
                        element.new_name = element.name;
                    break;
                }

                case ElementType::DATABASE:
                {
                    if (element.new_name.first.empty())
                        element.new_name = element.name;
                    break;
                }

                default:
                    break;
            }
        }
    }

    /// Removes duplications in the elements of a backup query by removing some excessive elements and by updating except_lists.
    /// This function helps deduplicate elements in queries like "BACKUP ALL DATABASES, DATABASE xxx USING NAME yyy"
    /// (we need a deduplication for that query because `ALL DATABASES` includes `xxx` however we don't want
    /// to backup/restore the same database twice while executing the same query).
    /// Also this function slightly reorders elements: it puts databases before tables and dictionaries they contain.
    void deduplicateAndReorderElements(Elements & elements)
    {
        std::set<size_t> skip_indices; /// Indices of elements which should be removed in the end of this function.
        size_t index_all_databases = static_cast<size_t>(-1); /// Index of the first element of type ALL_DATABASES or -1 if not found.

        struct DatabaseInfo
        {
            size_t index = static_cast<size_t>(-1);
            std::unordered_map<std::string_view, size_t> tables;
        };
        std::unordered_map<std::string_view, DatabaseInfo> databases; /// Found databases and tables.

        for (size_t i = 0; i != elements.size(); ++i)
        {
            auto & element = elements[i];
            switch (element.type)
            {
                case ElementType::TABLE:
                {
                    auto & tables = databases.emplace(element.name.first, DatabaseInfo{}).first->second.tables;
                    auto it = tables.find(element.name.second);
                    if (it == tables.end())
                    {
                        tables.emplace(element.name.second, i);
                    }
                    else
                    {
                        size_t prev_index = it->second;
                        if ((elements[i].new_name == elements[prev_index].new_name)
                            && (elements[i].partitions.empty() == elements[prev_index].partitions.empty()))
                        {
                            insertAtEnd(elements[prev_index].partitions, elements[i].partitions);
                            skip_indices.emplace(i);
                        }
                        else
                        {
                            throw Exception(
                                "Table " + backQuote(element.name.first) + "." + backQuote(element.name.second) + " was specified twice",
                                ErrorCodes::BACKUP_ELEMENT_DUPLICATE);
                        }
                    }
                    break;
                }

                case ElementType::DATABASE:
                {
                    auto it = databases.find(element.name.first);
                    if (it == databases.end())
                    {
                        DatabaseInfo new_db_info;
                        new_db_info.index = i;
                        databases.emplace(element.name.first, new_db_info);
                    }
                    else if (it->second.index == static_cast<size_t>(-1))
                    {
                        it->second.index = i;
                    }
                    else
                    {
                        size_t prev_index = it->second.index;
                        if ((elements[i].new_name == elements[prev_index].new_name)
                            && (elements[i].except_list == elements[prev_index].except_list))
                        {
                            skip_indices.emplace(i);
                        }
                        else
                        {
                            throw Exception("Database " + backQuote(element.name.first) + " was specified twice", ErrorCodes::BACKUP_ELEMENT_DUPLICATE);
                        }

                    }
                    break;
                }

                case ElementType::ALL_DATABASES:
                {
                    if (index_all_databases == static_cast<size_t>(-1))
                    {
                        index_all_databases = i;
                    }
                    else
                    {
                        size_t prev_index = index_all_databases;
                        if (elements[i].except_list == elements[prev_index].except_list)
                            skip_indices.emplace(i);
                        else
                            throw Exception("The tag ALL DATABASES was specified twice", ErrorCodes::BACKUP_ELEMENT_DUPLICATE);
                    }
                    break;
                }

                default:
                    /// replaceElementTypesWithBaseElementTypes() and replaceTemporaryTablesWithTemporaryDatabase() should have removed all other element types.
                    throw Exception("Unexpected element type: " + std::to_string(static_cast<int>(element.type)), ErrorCodes::LOGICAL_ERROR);
            }
        }

        if (index_all_databases != static_cast<size_t>(-1))
        {
            for (auto & [database_name, database] : databases)
            {
                elements[index_all_databases].except_list.emplace(database_name);
                if (database.index == static_cast<size_t>(-1))
                {
                    auto & new_element = elements.emplace_back();
                    new_element.type = ElementType::DATABASE;
                    new_element.name.first = database_name;
                    new_element.new_name = new_element.name;
                    database.index = elements.size() - 1;
                }
            }
        }

        for (auto & [database_name, database] : databases)
        {
            if (database.index == static_cast<size_t>(-1))
                continue;
            for (const auto & [table_name, table_index] : database.tables)
                elements[database.index].except_list.emplace(table_name);
        }

        for (auto skip_index : skip_indices | boost::adaptors::reversed)
            elements.erase(elements.begin() + skip_index);
    }

    Elements adjustElements(const Elements & elements, const String & current_database)
    {
        auto res = elements;
        replaceElementTypesWithBaseElementTypes(res);
        replaceEmptyDatabaseWithCurrentDatabase(res, current_database);
        replaceTemporaryTablesWithTemporaryDatabase(res);
        setNewNamesIfNotSet(res);
        deduplicateAndReorderElements(res);
        return res;
    }

    void backupCreateQuery(const IAST & create_query, BackupEntries & backup_entries)
    {
        auto metadata_entry = std::make_unique<BackupEntryFromMemory>(serializeAST(create_query));
        String metadata_path = getMetadataPathInBackup(create_query);
        backup_entries.emplace_back(metadata_path, std::move(metadata_entry));
    }

    void backupTable(
        const DatabaseAndTable & database_and_table,
        const String & table_name,
        const ASTs & partitions,
        const ContextPtr & context,
        const BackupRenamingConfigPtr & renaming_config,
        BackupEntries & backup_entries)
    {
        const auto & database = database_and_table.first;
        const auto & storage = database_and_table.second;
        context->checkAccess(AccessType::SELECT, database->getDatabaseName(), table_name);

        auto create_query = database->getCreateTableQuery(table_name, context);
        ASTPtr new_create_query = renameInCreateQuery(create_query, renaming_config, context);
        backupCreateQuery(*new_create_query, backup_entries);

        auto data_backup = storage->backup(partitions, context);
        if (!data_backup.empty())
        {
            String data_path = getDataPathInBackup(*new_create_query);
            for (auto & [path_in_backup, backup_entry] : data_backup)
                backup_entries.emplace_back(data_path + path_in_backup, std::move(backup_entry));
        }
    }

    void backupDatabase(
        const DatabasePtr & database,
        const std::set<String> & except_list,
        const ContextPtr & context,
        const BackupRenamingConfigPtr & renaming_config,
        BackupEntries & backup_entries)
    {
        context->checkAccess(AccessType::SHOW_TABLES, database->getDatabaseName());

        auto create_query = database->getCreateDatabaseQuery();
        ASTPtr new_create_query = renameInCreateQuery(create_query, renaming_config, context);
        backupCreateQuery(*new_create_query, backup_entries);

        for (auto it = database->getTablesIteratorForBackup(context); it->isValid(); it->next())
        {
            if (except_list.contains(it->name()))
                continue;
            backupTable({database, it->table()}, it->name(), {}, context, renaming_config, backup_entries);
        }
    }

    void backupAllDatabases(
        const std::set<String> & except_list,
        const ContextPtr & context,
        const BackupRenamingConfigPtr & renaming_config,
        BackupEntries & backup_entries)
    {
        for (const auto & [database_name, database] : DatabaseCatalog::instance().getDatabases())
        {
            if (except_list.contains(database_name))
                continue;
            if (database_name == DatabaseCatalog::SYSTEM_DATABASE || database_name == DatabaseCatalog::TEMPORARY_DATABASE)
                continue;
            backupDatabase(database, {}, context, renaming_config, backup_entries);
        }
    }
}


BackupEntries makeBackupEntries(const Elements & elements, const ContextPtr & context)
{
    BackupEntries backup_entries;

    auto elements2 = adjustElements(elements, context->getCurrentDatabase());
    auto renaming_config = std::make_shared<BackupRenamingConfig>();
    renaming_config->setFromBackupQueryElements(elements2);

    for (const auto & element : elements2)
    {
        switch (element.type)
        {
            case ElementType::TABLE:
            {
                const String & database_name = element.name.first;
                const String & table_name = element.name.second;
                auto [database, storage] = DatabaseCatalog::instance().getDatabaseAndTable({database_name, table_name}, context);
                backupTable({database, storage}, table_name, element.partitions, context, renaming_config, backup_entries);
                break;
            }

            case ElementType::DATABASE:
            {
                const String & database_name = element.name.first;
                auto database = DatabaseCatalog::instance().getDatabase(database_name, context);
                backupDatabase(database, element.except_list, context, renaming_config, backup_entries);
                break;
            }

            case ElementType::ALL_DATABASES:
            {
                backupAllDatabases(element.except_list, context, renaming_config, backup_entries);
                break;
            }

            default:
                throw Exception("Unexpected element type", ErrorCodes::LOGICAL_ERROR); /// other element types have been removed in deduplicateElements()
        }
    }

    /// A backup cannot be empty.
    if (backup_entries.empty())
        throw Exception("Backup must not be empty", ErrorCodes::BACKUP_IS_EMPTY);

    /// Check that all backup entries are unique.
    ::sort(
        backup_entries.begin(),
        backup_entries.end(),
        [](const std::pair<String, std::unique_ptr<IBackupEntry>> & lhs, const std::pair<String, std::unique_ptr<IBackupEntry>> & rhs)
        {
            return lhs.first < rhs.first;
        });
    auto adjacent = std::adjacent_find(backup_entries.begin(), backup_entries.end());
    if (adjacent != backup_entries.end())
        throw Exception("Cannot write multiple entries with the same name " + quoteString(adjacent->first), ErrorCodes::BACKUP_ELEMENT_DUPLICATE);

    return backup_entries;
}

UInt64 estimateBackupSize(const BackupEntries & backup_entries, const BackupPtr & base_backup)
{
    UInt64 total_size = 0;
    for (const auto & [name, entry] : backup_entries)
    {
        UInt64 data_size = entry->getSize();
        if (base_backup)
        {
            if (base_backup->fileExists(name) && (data_size == base_backup->getFileSize(name)))
            {
                auto checksum = entry->getChecksum();
                if (checksum && (*checksum == base_backup->getFileChecksum(name)))
                    continue;
            }
        }
        total_size += data_size;
    }
    return total_size;
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
                backup->addFile(name, std::move(entry));
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
