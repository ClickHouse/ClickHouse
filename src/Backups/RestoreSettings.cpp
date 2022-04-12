#include <Backups/RestoreSettings.h>
#include <Backups/BackupInfo.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Core/SettingsFields.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <boost/algorithm/string/predicate.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int CANNOT_PARSE_RESTORE_TABLE_CREATION_MODE;
}

namespace
{
    RestoreTableCreationMode parseRestoreTableCreationMode(const Field & field)
    {
        if (field.getType() == Field::Types::String)
        {
            String str = field.get<String>();
            if (str == "1" || boost::iequals(str, "true"))
                return RestoreTableCreationMode::kCreate;
            if (str == "0" || boost::iequals(str, "false"))
                return RestoreTableCreationMode::kMustExist;
            if (boost::iequals(str, "if not exists"))
                return RestoreTableCreationMode::kCreateIfNotExists;
            throw Exception("Cannot parse creation mode from string '" + str + "'",
                            ErrorCodes::CANNOT_PARSE_RESTORE_TABLE_CREATION_MODE);
        }
        if (applyVisitor(FieldVisitorConvertToNumber<bool>(), field))
            return RestoreTableCreationMode::kCreate;
        else
            return RestoreTableCreationMode::kMustExist;
    }
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
            else if (setting.name == "create_table")
                res.create_table = parseRestoreTableCreationMode(setting.value);
            else if (setting.name == "create_database")
                res.create_database = parseRestoreTableCreationMode(setting.value);
            else if (setting.name == "allow_different_table_def")
                res.allow_different_table_def = SettingFieldBool{setting.value};
            else if (setting.name == "allow_different_database_def")
                res.allow_different_database_def = SettingFieldBool{setting.value};
            else
                throw Exception(ErrorCodes::UNKNOWN_SETTING, "Unknown setting {}", setting.name);
        }
    }

    return res;
}

}
