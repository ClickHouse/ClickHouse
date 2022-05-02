#include <Backups/BackupInfo.h>
#include <Backups/BackupSettings.h>
#include <Core/SettingsFields.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTSetQuery.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_BACKUP_SETTINGS;
}

/// List of backup settings except base_backup_name.
#define LIST_OF_BACKUP_SETTINGS(M) \
    M(String, compression_method) \
    M(Int64, compression_level) \
    M(String, password) \
    M(Bool, structure_only) \
    M(Bool, async) \
    M(UInt64, shard_num) \
    M(UInt64, replica_num) \
    M(Bool, allow_storing_multiple_replicas) \
    M(Bool, internal) \
    M(String, coordination_zk_path)


BackupSettings BackupSettings::fromBackupQuery(const ASTBackupQuery & query)
{
    BackupSettings res;

    if (query.base_backup_name)
        res.base_backup_info = BackupInfo::fromAST(*query.base_backup_name);

    if (query.settings)
    {
        const auto & settings = query.settings->as<const ASTSetQuery &>().changes;
        for (const auto & setting : settings)
        {
#define GET_SETTINGS_FROM_BACKUP_QUERY_HELPER(TYPE, NAME) \
            if (setting.name == #NAME) \
                res.NAME = SettingField##TYPE{setting.value}.value; \
            else

            LIST_OF_BACKUP_SETTINGS(GET_SETTINGS_FROM_BACKUP_QUERY_HELPER)
            throw Exception(ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS, "Unknown setting {}", setting.name);
        }
    }

    return res;
}

void BackupSettings::copySettingsToBackupQuery(ASTBackupQuery & query) const
{
    query.base_backup_name = base_backup_info ? base_backup_info->toAST() : nullptr;

    auto query_settings = std::make_shared<ASTSetQuery>();
    query_settings->is_standalone = false;

    static const BackupSettings default_settings;

#define SET_SETTINGS_IN_BACKUP_QUERY_HELPER(TYPE, NAME) \
    if ((NAME) != default_settings.NAME) \
        query_settings->changes.emplace_back(#NAME, static_cast<Field>(SettingField##TYPE{NAME}));

    LIST_OF_BACKUP_SETTINGS(SET_SETTINGS_IN_BACKUP_QUERY_HELPER)

    query.settings = query_settings;
}

}
