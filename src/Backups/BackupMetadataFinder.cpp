#include <Backups/BackupMetadataFinder.h>
#include <Backups/BackupSettings.h>
#include <Backups/BackupUtils.h>
#include <Backups/IBackup.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <base/insertAtEnd.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/map.hpp>

#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
extern const int BACKUP_ENTRY_NOT_FOUND;
extern const int CANNOT_RESTORE_TABLE;
extern const int CANNOT_RESTORE_DATABASE;
}

BackupMetadataFinder::BackupMetadataFinder(
    const RestoreSettings & restore_settings_,
    const BackupPtr & backup_,
    const ContextMutablePtr & context_,
    const ContextPtr & query_context_,
    ThreadPool & thread_pool_)
    : restore_settings(restore_settings_)
    , backup(backup_)
    , context(context_)
    , query_context(query_context_)
    , process_list_element(context->getProcessListElement())
    , log(getLogger("BackupMetadataFinder"))
    , tables_dependencies("BackupMetadataFinder")
    , thread_pool(thread_pool_)
{
}

String BackupMetadataFinder::tableNameWithTypeToString(const String & database_name, const String & table_name, bool first_upper)
{
    String str;
    if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
        str = fmt::format("temporary table {}", backQuoteIfNeed(table_name));
    else
        str = fmt::format("table {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));
    if (first_upper)
        str[0] = static_cast<char>(std::toupper(str[0]));
    return str;
}

void BackupMetadataFinder::waitFutures(bool throw_if_error)
{
    std::exception_ptr error;

    for (;;)
    {
        std::vector<std::future<void>> futures_to_wait;
        {
            std::lock_guard lock{mutex};
            std::swap(futures_to_wait, futures);
        }

        if (futures_to_wait.empty())
            break;

        /// Wait for all tasks to finish.
        for (auto & future : futures_to_wait)
        {
            try
            {
                future.get();
            }
            catch (...)
            {
                if (!error)
                    error = std::current_exception();
                exception_caught = true;
            }
        }
    }

    if (error)
    {
        if (throw_if_error)
            std::rethrow_exception(error);
        else
            tryLogException(error, log);
    }
}

size_t BackupMetadataFinder::getNumFutures() const
{
    std::lock_guard lock{mutex};
    return futures.size();
}

void BackupMetadataFinder::schedule(std::function<void()> && task_, ThreadName thread_name_)
{
    if (exception_caught)
        return;

    checkIsQueryCancelled();

    auto future = scheduleFromThreadPoolUnsafe<void>(
        [this, task = std::move(task_)]() mutable
        {
            if (exception_caught)
                return;
            checkIsQueryCancelled();

            std::move(task)();
        },
        thread_pool,
        thread_name_);

    std::lock_guard lock{mutex};
    futures.push_back(std::move(future));
}

void BackupMetadataFinder::checkIsQueryCancelled() const
{
    if (process_list_element)
        process_list_element->checkTimeLimit();
}

void BackupMetadataFinder::applyCustomStoragePolicy(ASTPtr)
{
    /// Default implementation does nothing. RestorerFromBackup overrides this.
}

void BackupMetadataFinder::findRootPathsInBackup()
{
    size_t shard_num = 1;
    size_t replica_num = 1;
    if (!restore_settings.host_id.empty())
    {
        std::tie(shard_num, replica_num)
            = BackupSettings::Util::findShardNumAndReplicaNum(restore_settings.cluster_host_ids, restore_settings.host_id);
    }

    root_paths_in_backup.clear();

    /// Start with "" as the root path and then we will add shard- and replica-related part to it.
    fs::path root_path = "/";
    root_paths_in_backup.push_back(root_path);

    /// Add shard-related part to the root path.
    Strings shards_in_backup = backup->listFiles(root_path / "shards", /*recursive*/ false);
    if (shards_in_backup.empty())
    {
        if (restore_settings.shard_num_in_backup > 1)
            throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "No shard #{} in backup", restore_settings.shard_num_in_backup);
    }
    else
    {
        String shard_name;
        if (restore_settings.shard_num_in_backup)
            shard_name = std::to_string(restore_settings.shard_num_in_backup);
        else if (shards_in_backup.size() == 1)
            shard_name = shards_in_backup.front();
        else
            shard_name = std::to_string(shard_num);
        if (std::find(shards_in_backup.begin(), shards_in_backup.end(), shard_name) == shards_in_backup.end())
            throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "No shard #{} in backup", shard_name);
        root_path = root_path / "shards" / shard_name;
        root_paths_in_backup.push_back(root_path);
    }

    /// Add replica-related part to the root path.
    Strings replicas_in_backup = backup->listFiles(root_path / "replicas", /*recursive*/ false);
    if (replicas_in_backup.empty())
    {
        if (restore_settings.replica_num_in_backup > 1)
            throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "No replica #{} in backup", restore_settings.replica_num_in_backup);
    }
    else
    {
        String replica_name;
        if (restore_settings.replica_num_in_backup)
        {
            replica_name = std::to_string(restore_settings.replica_num_in_backup);
            if (std::find(replicas_in_backup.begin(), replicas_in_backup.end(), replica_name) == replicas_in_backup.end())
                throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "No replica #{} in backup", replica_name);
        }
        else
        {
            replica_name = std::to_string(replica_num);
            if (std::find(replicas_in_backup.begin(), replicas_in_backup.end(), replica_name) == replicas_in_backup.end())
                replica_name = replicas_in_backup.front();
        }
        root_path = root_path / "replicas" / replica_name;
        root_paths_in_backup.push_back(root_path);
    }

    /// Revert the list of root paths, because we need it in the following order:
    /// "/shards/<shard_num>/replicas/<replica_num>/" (first we search tables here)
    /// "/shards/<shard_num>/" (then here)
    /// "/" (and finally here)
    std::reverse(root_paths_in_backup.begin(), root_paths_in_backup.end());

    LOG_TRACE(
        log,
        "Will use paths in backup: {}",
        boost::algorithm::join(
            root_paths_in_backup
                | boost::adaptors::transformed(
                    [](const std::filesystem::path & path) -> String { return doubleQuoteString(String{path}); }),
            ", "));
}

void BackupMetadataFinder::findTableInBackup(
    const QualifiedTableName & table_name_in_backup, bool skip_if_inner_table, const std::optional<ASTs> & partitions)
{
    schedule(
        [this, table_name_in_backup, skip_if_inner_table, partitions]()
        { findTableInBackupImpl(table_name_in_backup, skip_if_inner_table, partitions); },
        ThreadName::RESTORE_FIND_TABLE);
}

void BackupMetadataFinder::findTableInBackupImpl(
    const QualifiedTableName & table_name_in_backup, bool skip_if_inner_table, const std::optional<ASTs> & partitions)
{
    bool is_temporary_table = (table_name_in_backup.database == DatabaseCatalog::TEMPORARY_DATABASE);

    std::optional<fs::path> metadata_path;
    std::optional<fs::path> root_path_in_use;
    for (const auto & root_path_in_backup : root_paths_in_backup)
    {
        fs::path try_metadata_path;
        if (is_temporary_table)
        {
            try_metadata_path
                = root_path_in_backup / "temporary_tables" / "metadata" / (escapeForFileName(table_name_in_backup.table) + ".sql");
        }
        else
        {
            try_metadata_path = root_path_in_backup / "metadata" / escapeForFileName(table_name_in_backup.database)
                / (escapeForFileName(table_name_in_backup.table) + ".sql");
        }

        if (backup->fileExists(try_metadata_path))
        {
            metadata_path = try_metadata_path;
            root_path_in_use = root_path_in_backup;
            break;
        }
    }

    if (!metadata_path)
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND,
            "{} not found in backup",
            tableNameWithTypeToString(table_name_in_backup.database, table_name_in_backup.table, true));

    fs::path data_path_in_backup;
    if (is_temporary_table)
    {
        data_path_in_backup = *root_path_in_use / "temporary_tables" / "data" / escapeForFileName(table_name_in_backup.table);
    }
    else
    {
        data_path_in_backup
            = *root_path_in_use / "data" / escapeForFileName(table_name_in_backup.database) / escapeForFileName(table_name_in_backup.table);
    }

    QualifiedTableName table_name = renaming_map.getNewTableName(table_name_in_backup);
    if (skip_if_inner_table && BackupUtils::isInnerTable(table_name))
        return;

    auto read_buffer = backup->readFile(*metadata_path);
    String create_query_str;
    readStringUntilEOF(create_query_str, *read_buffer);
    read_buffer.reset();
    ParserCreateQuery create_parser;
    ASTPtr create_table_query
        = parseQuery(create_parser, create_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    applyCustomStoragePolicy(create_table_query);
    renameDatabaseAndTableNameInCreateQuery(create_table_query, renaming_map, context->getGlobalContext());
    String create_table_query_str = create_table_query->formatWithSecretsOneLine();

    bool is_predefined_table = DatabaseCatalog::instance().isPredefinedTable(StorageID{table_name.database, table_name.table});
    auto table_dependencies = getDependenciesFromCreateQuery(context, table_name, create_table_query, context->getCurrentDatabase(), /*can_throw*/ false, /*validate_current_database*/ false);
    bool table_has_data = backup->hasFiles(data_path_in_backup);

    std::lock_guard lock{mutex};

    if (auto it = table_infos.find(table_name); it != table_infos.end())
    {
        const TableInfo & table_info = it->second;
        if (table_info.create_table_query && (table_info.create_table_query_str != create_table_query_str))
        {
            throw Exception(
                ErrorCodes::CANNOT_RESTORE_TABLE,
                "Extracted two different create queries for the same {}: {} and {}",
                tableNameWithTypeToString(table_name.database, table_name.table, false),
                table_info.create_table_query_str,
                create_table_query_str);
        }
    }

    TableInfo & res_table_info = table_infos[table_name];
    res_table_info.create_table_query = create_table_query;
    res_table_info.create_table_query_str = create_table_query_str;
    res_table_info.is_predefined_table = is_predefined_table;
    res_table_info.has_data = table_has_data;
    res_table_info.data_path_in_backup = data_path_in_backup;
    res_table_info.metadata_path_in_backup = *metadata_path;

    tables_dependencies.addDependencies(table_name, table_dependencies.dependencies);

    if (partitions)
    {
        if (!res_table_info.partitions)
            res_table_info.partitions.emplace();
        insertAtEnd(*res_table_info.partitions, *partitions);
    }
}

void BackupMetadataFinder::findDatabaseInBackup(
    const String & database_name_in_backup, const std::set<DatabaseAndTableName> & except_table_names)
{
    schedule(
        [this, database_name_in_backup, except_table_names]() { findDatabaseInBackupImpl(database_name_in_backup, except_table_names); },
        ThreadName::RESTORE_FIND_TABLE);
}

void BackupMetadataFinder::findDatabaseInBackupImpl(
    const String & database_name_in_backup, const std::set<DatabaseAndTableName> & except_table_names)
{
    std::optional<fs::path> metadata_path;
    std::unordered_set<String> table_names_in_backup;
    for (const auto & root_path_in_backup : root_paths_in_backup)
    {
        fs::path try_metadata_path;
        fs::path try_tables_metadata_path;
        if (database_name_in_backup == DatabaseCatalog::TEMPORARY_DATABASE)
        {
            try_tables_metadata_path = root_path_in_backup / "temporary_tables" / "metadata";
        }
        else
        {
            try_metadata_path = root_path_in_backup / "metadata" / (escapeForFileName(database_name_in_backup) + ".sql");
            try_tables_metadata_path = root_path_in_backup / "metadata" / escapeForFileName(database_name_in_backup);
        }

        if (!metadata_path && !try_metadata_path.empty() && backup->fileExists(try_metadata_path))
            metadata_path = try_metadata_path;

        Strings file_names = backup->listFiles(try_tables_metadata_path, /*recursive*/ false);
        for (const String & file_name : file_names)
        {
            if (!file_name.ends_with(".sql"))
                continue;
            String file_name_without_ext = file_name.substr(0, file_name.length() - strlen(".sql"));
            table_names_in_backup.insert(unescapeForFileName(file_name_without_ext));
        }
    }

    if (!metadata_path && table_names_in_backup.empty())
        throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Database {} not found in backup", backQuoteIfNeed(database_name_in_backup));

    if (metadata_path)
    {
        auto read_buffer = backup->readFile(*metadata_path);
        String create_query_str;
        readStringUntilEOF(create_query_str, *read_buffer);
        read_buffer.reset();
        ParserCreateQuery create_parser;
        ASTPtr create_database_query
            = parseQuery(create_parser, create_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        renameDatabaseAndTableNameInCreateQuery(create_database_query, renaming_map, context->getGlobalContext());
        String create_database_query_str = create_database_query->formatWithSecretsOneLine();

        String database_name = renaming_map.getNewDatabaseName(database_name_in_backup);
        bool is_predefined_database = DatabaseCatalog::isPredefinedDatabase(database_name);

        std::lock_guard lock{mutex};

        DatabaseInfo & database_info = database_infos[database_name];

        if (database_info.create_database_query && (database_info.create_database_query_str != create_database_query_str))
        {
            throw Exception(
                ErrorCodes::CANNOT_RESTORE_DATABASE,
                "Extracted two different create queries for the same database {}: {} and {}",
                backQuoteIfNeed(database_name),
                database_info.create_database_query_str,
                create_database_query_str);
        }

        database_info.create_database_query = create_database_query;
        database_info.create_database_query_str = create_database_query_str;
        database_info.is_predefined_database = is_predefined_database;
        database_info.metadata_path_in_backup = *metadata_path;
    }

    for (const String & table_name_in_backup : table_names_in_backup)
    {
        if (except_table_names.contains({database_name_in_backup, table_name_in_backup}))
            continue;

        findTableInBackup({database_name_in_backup, table_name_in_backup}, /* skip_if_inner_table= */ true, /* partitions= */ {});
    }
}

void BackupMetadataFinder::findEverythingInBackup(
    const std::set<String> & except_database_names, const std::set<DatabaseAndTableName> & except_table_names)
{
    std::unordered_set<String> database_names_in_backup;

    for (const auto & root_path_in_backup : root_paths_in_backup)
    {
        Strings file_names = backup->listFiles(root_path_in_backup / "metadata", /*recursive*/ false);
        for (String & file_name : file_names)
        {
            if (file_name.ends_with(".sql"))
                file_name.resize(file_name.length() - strlen(".sql"));
            database_names_in_backup.emplace(unescapeForFileName(file_name));
        }

        if (backup->hasFiles(root_path_in_backup / "temporary_tables" / "metadata"))
            database_names_in_backup.emplace(DatabaseCatalog::TEMPORARY_DATABASE);
    }

    for (const String & database_name_in_backup : database_names_in_backup)
    {
        if (except_database_names.contains(database_name_in_backup))
            continue;

        findDatabaseInBackup(database_name_in_backup, except_table_names);
    }
}

size_t BackupMetadataFinder::getNumDatabases() const
{
    std::lock_guard lock{mutex};
    return database_infos.size();
}

size_t BackupMetadataFinder::getNumTables() const
{
    std::lock_guard lock{mutex};
    return table_infos.size();
}

}
