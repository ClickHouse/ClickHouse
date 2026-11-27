#include <Functions/UserDefined/UserDefinedSQLObjectsBackup.h>

#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupCoordination.h>
#include <Backups/IRestoreCoordination.h>
#include <Backups/RestorerFromBackup.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Interpreters/Context.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Parsers/ParserCreateTypeQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <base/sort.h>
#include <Core/Settings.h>
#include <IO/ReadHelpers.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_TABLE;
}


void backupUserDefinedSQLObjects(
    BackupEntriesCollector & backup_entries_collector,
    const String & data_path_in_backup,
    UserDefinedSQLObjectType object_type,
    const VectorWithMemoryTracking<std::pair<String, ASTPtr>> & objects)
{
    VectorWithMemoryTracking<std::pair<String, BackupEntryPtr>> backup_entries;
    backup_entries.reserve(objects.size());
    for (const auto & [object_name, create_object_query] : objects)
        backup_entries.emplace_back(
            escapeForFileName(object_name) + ".sql", std::make_shared<BackupEntryFromMemory>(create_object_query->formatWithSecretsOneLine()));

    auto context = backup_entries_collector.getContext();
    const auto & storage = (object_type == UserDefinedSQLObjectType::Type)
        ? context->getUserDefinedTypesStorage()
        : context->getUserDefinedSQLObjectsStorage();

    if (!storage.isReplicated())
    {
        fs::path data_path_in_backup_fs{data_path_in_backup};
        for (const auto & [file_name, entry] : backup_entries)
            backup_entries_collector.addBackupEntry(data_path_in_backup_fs / file_name, entry);
        return;
    }

    String replication_id = storage.getReplicationID();

    auto backup_coordination = backup_entries_collector.getBackupCoordination();
    backup_coordination->addReplicatedSQLObjectsDir(replication_id, object_type, data_path_in_backup);

    // On the stage of running post tasks, all directories will already be added to the backup coordination object.
    // They will only be returned for one of the hosts below, for the rest an empty list.
    // See also BackupCoordinationReplicatedSQLObjects class.
    backup_entries_collector.addPostTask(
        [my_backup_entries = std::move(backup_entries),
         my_replication_id = std::move(replication_id),
         object_type,
         &backup_entries_collector,
         backup_coordination]
        {
            auto dirs = backup_coordination->getReplicatedSQLObjectsDirs(my_replication_id, object_type);

            for (const auto & dir : dirs)
            {
                fs::path dir_fs{dir};
                for (const auto & [file_name, entry] : my_backup_entries)
                {
                    backup_entries_collector.addBackupEntry(dir_fs / file_name, entry);
                }
            }
        });
}


namespace
{
    const IUserDefinedSQLObjectsStorage & getStorage(const ContextPtr & context, UserDefinedSQLObjectType object_type)
    {
        return (object_type == UserDefinedSQLObjectType::Type)
            ? context->getUserDefinedTypesStorage()
            : context->getUserDefinedSQLObjectsStorage();
    }

    /// Reads the objects from the directories of the backup. The same object can be in several directories (the backup
    /// of a shared storage has an identical directory for every host sharing it), so identical definitions collapse
    /// into one. The same name with different definitions means the directories hold objects which could not have
    /// coexisted in one storage; picking one of them would silently restore a wrong definition, so it is rejected.
    VectorWithMemoryTracking<std::pair<String, ASTPtr>>
    readUserDefinedSQLObjects(RestorerFromBackup & restorer, const Strings & data_paths_in_backup, UserDefinedSQLObjectType object_type)
    {
        auto context = restorer.getContext();
        auto backup = restorer.getBackup();

        VectorWithMemoryTracking<std::pair<String, ASTPtr>> res;
        UnorderedMapWithMemoryTracking<String /* object name */, String /* formatted statement */> statements_by_name;

        for (const auto & data_path_in_backup : data_paths_in_backup)
        {
            fs::path data_path_in_backup_fs{data_path_in_backup};

            Strings filenames = backup->listFiles(data_path_in_backup, /*recursive*/ false);
            ::sort(filenames.begin(), filenames.end());

            for (const auto & filename : filenames)
            {
                if (!filename.ends_with(".sql"))
                {
                    throw Exception(
                        ErrorCodes::CANNOT_RESTORE_TABLE,
                        "Cannot restore user-defined SQL objects: File name {} doesn't have the extension .sql",
                        String{data_path_in_backup_fs / filename});
                }
            }

            for (const auto & filename : filenames)
            {
                String escaped_object_name = filename.substr(0, filename.length() - strlen(".sql"));
                String object_name = unescapeForFileName(escaped_object_name);

                String filepath = data_path_in_backup_fs / filename;
                auto in = backup->readFile(filepath);
                String statement_def;
                readStringUntilEOF(statement_def, *in);

                auto parse = [&](IParser & parser)
                {
                    return parseQuery(
                        parser,
                        statement_def.data(),
                        statement_def.data() + statement_def.size(),
                        "in file " + filepath + " from backup " + backup->getNameForLogging(),
                        0,
                        context->getSettingsRef()[Setting::max_parser_depth],
                        context->getSettingsRef()[Setting::max_parser_backtracks]);
                };

                ASTPtr ast;
                switch (object_type)
                {
                    case UserDefinedSQLObjectType::Function:
                    {
                        ParserCreateFunctionQuery parser;
                        ast = parse(parser);
                        break;
                    }
                    case UserDefinedSQLObjectType::Type:
                    {
                        ParserCreateTypeQuery parser;
                        ast = parse(parser);
                        break;
                    }
                }

                String statement = ast->formatWithSecretsOneLine();
                auto [it, inserted] = statements_by_name.emplace(object_name, statement);
                if (!inserted)
                {
                    if (it->second != statement)
                        throw Exception(
                            ErrorCodes::CANNOT_RESTORE_TABLE,
                            "Cannot restore user-defined SQL objects from backup {}: the object {} has different "
                            "definitions on the hosts of the backup which are restored into one replicated storage: "
                            "{} and {}",
                            backup->getNameForLogging(), backQuote(object_name), it->second, statement);
                    continue;
                }

                res.emplace_back(std::move(object_name), ast);
            }
        }

        return res;
    }
}


VectorWithMemoryTracking<std::pair<String, ASTPtr>>
restoreUserDefinedSQLObjects(RestorerFromBackup & restorer, const String & data_path_in_backup, UserDefinedSQLObjectType object_type)
{
    const auto & storage = getStorage(restorer.getContext(), object_type);

    if (storage.isReplicated() && !restorer.getRestoreCoordination()->acquireReplicatedSQLObjects(storage.getReplicationID(), object_type))
        return {}; /// Other replica is already restoring user-defined SQL objects.

    return readUserDefinedSQLObjects(restorer, {data_path_in_backup}, object_type);
}


void registerUserDefinedSQLObjectsDirForRestore(
    RestorerFromBackup & restorer, const String & data_path_in_backup, UserDefinedSQLObjectType object_type)
{
    const auto & storage = getStorage(restorer.getContext(), object_type);
    if (!storage.isReplicated())
        return;
    restorer.getRestoreCoordination()->addReplicatedSQLObjectsDir(storage.getReplicationID(), object_type, data_path_in_backup);
}


bool restoreUserDefinedSQLObjectsOfAllHosts(
    RestorerFromBackup & restorer,
    const String & data_path_in_backup,
    UserDefinedSQLObjectType object_type,
    VectorWithMemoryTracking<std::pair<String, ASTPtr>> & objects)
{
    objects.clear();
    const auto & storage = getStorage(restorer.getContext(), object_type);

    if (!storage.isReplicated())
    {
        if (data_path_in_backup.empty())
            return false;
        objects = readUserDefinedSQLObjects(restorer, {data_path_in_backup}, object_type);
        return true;
    }

    auto restore_coordination = restorer.getRestoreCoordination();
    String replication_id = storage.getReplicationID();

    /// The directories registered by every host sharing this storage. They hold identical objects when the backup was
    /// taken from hosts sharing a storage the same way, but a backup taken from hosts with per-host storages has the
    /// objects of each host in its own directory, and restoring just the directory of this host would silently lose
    /// the objects of the others.
    Strings data_paths = restore_coordination->getReplicatedSQLObjectsDirs(replication_id, object_type);
    if (!data_path_in_backup.empty() && std::find(data_paths.begin(), data_paths.end(), data_path_in_backup) == data_paths.end())
        data_paths.push_back(data_path_in_backup);

    /// This host's part of the backup may have no such objects while a host sharing the storage restores the ones of
    /// its own part; then this host only has to reload them.
    if (data_paths.empty())
        return false;

    if (!restore_coordination->acquireReplicatedSQLObjects(replication_id, object_type))
        return true; /// Other replica is already restoring the objects into the shared storage.

    ::sort(data_paths.begin(), data_paths.end());
    objects = readUserDefinedSQLObjects(restorer, data_paths, object_type);
    return true;
}

}
