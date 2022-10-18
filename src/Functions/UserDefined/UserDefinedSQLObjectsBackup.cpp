#include <Functions/UserDefined/UserDefinedSQLObjectsBackup.h>

#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupCoordination.h>
#include <Backups/IRestoreCoordination.h>
#include <Backups/RestorerFromBackup.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsLoader.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Interpreters/Context.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Common/escapeForFileName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_TABLE;
}

void backupUserDefinedSQLObjects(
    BackupEntriesCollector & backup_entries_collector,
    const String & data_path_in_backup,
    UserDefinedSQLObjectType /* object_type */,
    const std::vector<std::pair<String, ASTPtr>> & objects)
{
    std::vector<std::pair<String, BackupEntryPtr>> backup_entries;
    backup_entries.reserve(objects.size());
    for (const auto & [function_name, create_function_query] : objects)
        backup_entries.emplace_back(
            escapeForFileName(function_name) + ".sql", std::make_shared<BackupEntryFromMemory>(queryToString(create_function_query)));

    fs::path data_path_in_backup_fs{data_path_in_backup};
    for (const auto & entry : backup_entries)
        backup_entries_collector.addBackupEntry(data_path_in_backup_fs / entry.first, entry.second);
}


std::vector<std::pair<String, ASTPtr>>
restoreUserDefinedSQLObjects(RestorerFromBackup & restorer, const String & data_path_in_backup, UserDefinedSQLObjectType object_type)
{
    auto context = restorer.getContext();
    auto backup = restorer.getBackup();
    fs::path data_path_in_backup_fs{data_path_in_backup};

    Strings filenames = backup->listFiles(data_path_in_backup);
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
        String escaped_function_name = filename.substr(0, filename.length() - strlen(".sql"));
        String function_name = unescapeForFileName(escaped_function_name);

        String filepath = data_path_in_backup_fs / filename;
        auto backup_entry = backup->readFile(filepath);
        auto in = backup_entry->getReadBuffer();
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
                    "in file " + filepath + " from backup " + backup->getName(),
                    0,
                    context->getSettingsRef().max_parser_depth);
                break;
            }
        }

        res.emplace_back(std::move(function_name), ast);
    }

    return res;
}

}
