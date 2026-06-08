#include <Backups/BackupInfo.h>
#include <Backups/BackupSettings.h>
#include <Backups/RestoreSettings.h>
#include <Core/SettingsFields.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <boost/algorithm/string/predicate.hpp>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Backups/SettingsFieldOptionalUUID.h>
#include <Backups/SettingsFieldOptionalString.h>
#include <Backups/SettingsFieldOptionalUInt64.h>


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

        explicit SettingFieldRestoreTableCreationMode(RestoreTableCreationMode value_) : value(value_) {}

        explicit SettingFieldRestoreTableCreationMode(const Field & field)
        {
            if (field.getType() == Field::Types::String)
            {
                const String & str = field.safeGet<String>();
                if (str == "1" || boost::iequals(str, "true") || boost::iequals(str, "create"))
                {
                    value = RestoreTableCreationMode::kCreate;
                    return;
                }

                if (str == "0" || boost::iequals(str, "false") || boost::iequals(str, "must exist") || boost::iequals(str, "must-exist"))
                {
                    value = RestoreTableCreationMode::kMustExist;
                    return;
                }

                if (boost::iequals(str, "if not exists") || boost::iequals(str, "if-not-exists")
                    || boost::iequals(str, "create if not exists") || boost::iequals(str, "create-if-not-exists"))
                {
                    value = RestoreTableCreationMode::kCreateIfNotExists;
                    return;
                }
            }

            if (field.getType() == Field::Types::UInt64)
            {
                UInt64 number = field.safeGet<UInt64>();
                if (number == 1)
                {
                    value = RestoreTableCreationMode::kCreate;
                    return;
                }

                if (number == 0)
                {
                    value = RestoreTableCreationMode::kMustExist;
                    return;
                }
            }

            throw Exception(ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS, "Cannot parse creation mode from {}", field);
        }

        explicit operator Field() const
        {
            switch (value)
            {
                case RestoreTableCreationMode::kCreate: return Field{true};
                case RestoreTableCreationMode::kMustExist: return Field{false};
                case RestoreTableCreationMode::kCreateIfNotExists: return Field{"if-not-exists"};
            }
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected value of enum RestoreTableCreationMode: {}", static_cast<int>(value));
        }
    };

    using SettingFieldRestoreDatabaseCreationMode = SettingFieldRestoreTableCreationMode;

    struct SettingFieldRestoreAccessCreationMode
    {
        RestoreAccessCreationMode value;

        explicit SettingFieldRestoreAccessCreationMode(RestoreAccessCreationMode value_) : value(value_) {}

        explicit SettingFieldRestoreAccessCreationMode(const Field & field)
        {
            if (field.getType() == Field::Types::String)
            {
                const String & str = field.safeGet<String>();
                if (str == "1" || boost::iequals(str, "true") || boost::iequals(str, "create"))
                {
                    value = RestoreAccessCreationMode::kCreate;
                    return;
                }

                if (boost::iequals(str, "if not exists") || boost::iequals(str, "if-not-exists")
                    || boost::iequals(str, "create if not exists") || boost::iequals(str, "create-if-not-exists"))
                {
                    value = RestoreAccessCreationMode::kCreateIfNotExists;
                    return;
                }

                if (boost::iequals(str, "replace") || boost::iequals(str, "create or replace") || boost::iequals(str, "create-or-replace"))
                {
                    value = RestoreAccessCreationMode::kReplace;
                    return;
                }
            }

            if (field.getType() == Field::Types::UInt64)
            {
                UInt64 number = field.safeGet<UInt64>();
                if (number == 1)
                {
                    value = RestoreAccessCreationMode::kCreate;
                    return;
                }
            }

            throw Exception(ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS, "Cannot parse creation mode from {}", field);
        }

        explicit operator Field() const
        {
            switch (value)
            {
                case RestoreAccessCreationMode::kCreate: return Field{true};
                case RestoreAccessCreationMode::kCreateIfNotExists: return Field{"if-not-exists"};
                case RestoreAccessCreationMode::kReplace: return Field{"replace"};
            }
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected value of enum RestoreAccessCreationMode: {}", static_cast<int>(value));
        }
    };

    using SettingFieldRestoreUDFCreationMode = SettingFieldRestoreAccessCreationMode;
}

/// List of restore settings except base_backup_name and cluster_host_ids.
#define LIST_OF_RESTORE_SETTINGS(M) \
    M(String, id) \
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
    M(Bool, allow_non_empty_tables) \
    M(RestoreAccessCreationMode, create_access) \
    M(Bool, skip_unresolved_access_dependencies) \
    M(Bool, update_access_entities_dependents) \
    M(RestoreUDFCreationMode, create_function) \
    M(Bool, allow_azure_native_copy) \
    M(Bool, allow_s3_native_copy) \
    M(Bool, use_same_s3_credentials_for_base_backup) \
    M(Bool, use_same_password_for_base_backup) \
    M(Bool, restore_broken_parts_as_detached) \
    M(Bool, internal) \
    M(String, host_id) \
    M(OptionalString, storage_policy) \
    M(OptionalUUID, restore_uuid)


RestoreSettings RestoreSettings::fromRestoreQuery(const ASTBackupQuery & query)
{
    RestoreSettings res;

    if (query.settings)
    {
        const auto & settings = query.settings->as<const ASTSetQuery &>().changes;
        for (const auto & setting : settings)
        {
#define GET_RESTORE_SETTINGS_FROM_QUERY(TYPE, NAME) \
            if (setting.name == #NAME) \
                res.NAME = SettingField##TYPE{setting.value}.value; \
            else

            LIST_OF_RESTORE_SETTINGS(GET_RESTORE_SETTINGS_FROM_QUERY)
            /// else
            /// `allow_unresolved_access_dependencies` is an obsolete name.
            if (setting.name == "allow_unresolved_access_dependencies")
            {
                res.skip_unresolved_access_dependencies = SettingFieldBool{setting.value}.value;
            }
            else
            {
                /// (if setting.name is not the name of a field of BackupSettings)
                res.core_settings.emplace_back(setting);
            }
        }
    }

    if (query.base_backup_name)
        res.base_backup_info = BackupInfo::fromAST(*query.base_backup_name);

    if (query.cluster_host_ids)
        res.cluster_host_ids = BackupSettings::Util::clusterHostIDsFromAST(*query.cluster_host_ids);

    return res;
}

void RestoreSettings::copySettingsToQuery(ASTBackupQuery & query) const
{
    auto query_settings = std::make_shared<ASTSetQuery>();
    query_settings->is_standalone = false;

    /// Copy the fields of the RestoreSettings to the query.
    static const RestoreSettings default_settings;

#define COPY_RESTORE_SETTINGS_TO_QUERY(TYPE, NAME) \
    if ((NAME) != default_settings.NAME) \
        query_settings->changes.emplace_back(#NAME, static_cast<Field>(SettingField##TYPE{NAME})); \

    LIST_OF_RESTORE_SETTINGS(COPY_RESTORE_SETTINGS_TO_QUERY)

    /// Copy the core settings to the query too.
    query_settings->changes.insert(query_settings->changes.end(), core_settings.begin(), core_settings.end());

    if (query_settings->changes.empty())
        query_settings = nullptr;

    query.settings = query_settings;

    auto base_backup_name = base_backup_info ? base_backup_info->toAST() : nullptr;
    if (base_backup_name)
        query.setOrReplace(query.base_backup_name, base_backup_name);
    else
        query.reset(query.base_backup_name);

    query.cluster_host_ids = !cluster_host_ids.empty() ? BackupSettings::Util::clusterHostIDsToAST(cluster_host_ids) : nullptr;
}

}
