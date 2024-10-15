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
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Common/escapeForFileName.h>
#include <Core/Settings.h>


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
    const std::vector<std::pair<String, ASTPtr>> & objects)
{
    std::vector<std::pair<String, BackupEntryPtr>> backup_entries;
    backup_entries.reserve(objects.size());
    for (const auto & [object_name, create_object_query] : objects)
        backup_entries.emplace_back(
            escapeForFileName(object_name) + ".sql", std::make_shared<BackupEntryFromMemory>(queryToString(create_object_query)));

    auto context = backup_entries_collector.getContext();
    const auto & storage = context->getUserDefinedSQLObjectsStorage();

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


std::vector<std::pair<String, ASTPtr>>
restoreUserDefinedSQLObjects(RestorerFromBackup & restorer, const String & data_path_in_backup, UserDefinedSQLObjectType object_type)
{
    auto context = restorer.getContext();
    const auto & storage = context->getUserDefinedSQLObjectsStorage();

    if (storage.isReplicated() && !restorer.getRestoreCoordination()->acquireReplicatedSQLObjects(storage.getReplicationID(), object_type))
        return {}; /// Other replica is already restoring user-defined SQL objects.

    auto backup = restorer.getBackup();
    fs::path data_path_in_backup_fs{data_path_in_backup};

    Strings filenames = backup->listFiles(data_path_in_backup, /*recursive*/ false);
    if (filenames.empty())
        return {}; /// Nothing to restore.

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

    std::vector<std::pair<String, ASTPtr>> res;

    for (const auto & filename : filenames)
    {
        String escaped_object_name = filename.substr(0, filename.length() - strlen(".sql"));
        String object_name = unescapeForFileName(escaped_object_name);

        String filepath = data_path_in_backup_fs / filename;
        auto in = backup->readFile(filepath);
        String statement_def;
        readStringUntilEOF(statement_def, *in);

        ASTPtr ast;

        switch (object_type)
        {
            case UserDefinedSQLObjectType::Function:
            {
                ParserCreateFunctionQuery parser;
                ast = parseQuery(
                    parser,
                    statement_def.data(),
                    statement_def.data() + statement_def.size(),
                    "in file " + filepath + " from backup " + backup->getNameForLogging(),
                    0,
                    context->getSettingsRef()[Setting::max_parser_depth],
                    context->getSettingsRef()[Setting::max_parser_backtracks]);
                break;
            }
        }

        res.emplace_back(std::move(object_name), ast);
    }

    return res;
}

}
