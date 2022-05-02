#include <Backups/BackupInfo.h>
#include <Backups/RestoreSettings.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Core/SettingsFields.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <boost/algorithm/string/predicate.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_BACKUP_SETTINGS;
    extern const int LOGICAL_ERROR;
}

namespace
{
    struct SettingFieldRestoreTableCreationMode
    {
        RestoreTableCreationMode value;

        explicit SettingFieldRestoreTableCreationMode(const Field & field)
        {
            if (field.getType() == Field::Types::String)
            {
                const String & str = field.get<const String &>();
                if (str == "1" || boost::iequals(str, "true"))
                    value = RestoreTableCreationMode::kCreate;
                else if (str == "0" || boost::iequals(str, "false"))
                    value = RestoreTableCreationMode::kMustExist;
                else if (boost::iequals(str, "if not exists"))
                    value = RestoreTableCreationMode::kCreateIfNotExists;
                else throw Exception("Cannot parse creation mode from string '" + str + "'",
                                     ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS);
            }
            else
            {
                if (applyVisitor(FieldVisitorConvertToNumber<bool>(), field))
                    value = RestoreTableCreationMode::kCreate;
                else
                    value = RestoreTableCreationMode::kMustExist;
            }
        }

        explicit operator Field() const
        {
            switch (value)
            {
                case RestoreTableCreationMode::kCreate: return Field{true};
                case RestoreTableCreationMode::kMustExist: return Field{false};
                case RestoreTableCreationMode::kCreateIfNotExists: return Field{"if not exists"};
            }
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected value of enum RestoreTableCreationMode: {}", static_cast<int>(value));
        }
    };

    using SettingFieldRestoreDatabaseCreationMode = SettingFieldRestoreTableCreationMode;
}

/// List of restore settings except base_backup_name.
#define LIST_OF_RESTORE_SETTINGS(M) \
    M(String, password) \
    M(Bool, structure_only) \
    M(RestoreTableCreationMode, create_table) \
    M(RestoreDatabaseCreationMode, create_database) \
    M(Bool, allow_different_table_def) \
    M(Bool, allow_different_database_def) \
    M(Bool, async) \
    M(UInt64, shard_num) \
    M(UInt64, replica_num) \
    M(UInt64, shard_num_in_backup) \
    M(UInt64, replica_num_in_backup) \
    M(Bool, internal) \
    M(String, coordination_zk_path)

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
#define GET_SETTINGS_FROM_RESTORE_QUERY_HELPER(TYPE, NAME) \
            if (setting.name == #NAME) \
                res.NAME = SettingField##TYPE{setting.value}.value; \
            else

            LIST_OF_RESTORE_SETTINGS(GET_SETTINGS_FROM_RESTORE_QUERY_HELPER)
            throw Exception(ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS, "Unknown setting {}", setting.name);
        }
    }

    return res;
}

void RestoreSettings::copySettingsToRestoreQuery(ASTBackupQuery & query) const
{
    query.base_backup_name = base_backup_info ? base_backup_info->toAST() : nullptr;

    auto query_settings = std::make_shared<ASTSetQuery>();
    query_settings->is_standalone = false;

    static const RestoreSettings default_settings;

#define SET_SETTINGS_IN_RESTORE_QUERY_HELPER(TYPE, NAME) \
    if ((NAME) != default_settings.NAME) \
        query_settings->changes.emplace_back(#NAME, static_cast<Field>(SettingField##TYPE{NAME}));

    LIST_OF_RESTORE_SETTINGS(SET_SETTINGS_IN_RESTORE_QUERY_HELPER)

    query.settings = query_settings;
}

}
