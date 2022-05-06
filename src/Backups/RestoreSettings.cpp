#include <Backups/RestoreSettings.h>
#include <Backups/BackupInfo.h>
#include <Core/SettingsFields.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTSetQuery.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

RestoreSettings RestoreSettings::fromRestoreQuery(const ASTBackupQuery & query)
{
    RestoreSettings res;

    if (query.base_backup_name)
        res.base_backup_info = BackupInfo::fromAST(*query.base_backup_name);

    if (query.settings)
    {
        const auto & settings = query.settings->as<const ASTSetQuery &>().changes;
        for (const auto & setting : settings)
        {
            if (setting.name == "password")
                res.password = SettingFieldString{setting.value};
            else if (setting.name == "structure_only")
                res.structure_only = SettingFieldBool{setting.value};
            else if (setting.name == "throw_if_database_exists")
                res.throw_if_database_exists = SettingFieldBool{setting.value};
            else if (setting.name == "throw_if_table_exists")
                res.throw_if_table_exists = SettingFieldBool{setting.value};
            else if (setting.name == "throw_if_database_def_differs")
                res.throw_if_database_def_differs = SettingFieldBool{setting.value};
            else if (setting.name == "throw_if_table_def_differs")
                res.throw_if_table_def_differs = SettingFieldBool{setting.value};
            else
                throw Exception(ErrorCodes::UNKNOWN_SETTING, "Unknown setting {}", setting.name);
        }
    }

    return res;
}

}
