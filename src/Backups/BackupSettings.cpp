#include "config.h"

#include <Backups/BackupInfo.h>
#include <Backups/BackupSettings.h>
#include <Core/SettingsFields.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Backups/SettingsFieldOptionalUUID.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_BACKUP_SETTINGS;
    extern const int WRONG_BACKUP_SETTINGS;
}

#if CLICKHOUSE_CLOUD
#define LIST_OF_CLOUD_BACKUP_SETTINGS(M) \
    M(UInt64, resumable_backup_batch_size) \
    M(UInt64, resumable_backup_batch_size_bytes)
#else
#define LIST_OF_CLOUD_BACKUP_SETTINGS(M)
#endif

/// List of backup settings except base_backup_name and cluster_host_ids.
#define LIST_OF_BACKUP_SETTINGS(M) \
    M(String, id) \
    M(String, compression_method) \
    M(String, password) \
    M(String, s3_storage_class) \
    M(Bool, structure_only) \
    M(Bool, async) \
    M(Bool, decrypt_files_from_encrypted_disks) \
    M(Bool, deduplicate_files) \
    M(Bool, allow_s3_native_copy) \
    M(Bool, allow_azure_native_copy) \
    M(Bool, use_same_s3_credentials_for_base_backup) \
    M(Bool, use_same_password_for_base_backup) \
    M(Bool, azure_attempt_to_create_container) \
    M(Bool, read_from_filesystem_cache) \
    M(UInt64, shard_num) \
    M(UInt64, replica_num) \
    M(Bool, check_parts) \
    M(Bool, check_projection_parts) \
    M(Bool, allow_backup_broken_projections) \
    M(Bool, write_access_entities_dependents) \
    M(Bool, allow_checksums_from_remote_paths) \
    M(BackupDataFileNameGeneratorType, data_file_name_generator) \
    M(Bool, backup_data_from_refreshable_materialized_view_targets) \
    LIST_OF_CLOUD_BACKUP_SETTINGS(M) \
    M(Bool, internal) \
    M(Bool, experimental_lightweight_snapshot) \
    M(String, host_id) \
    M(OptionalUUID, backup_uuid) \
    /// M(Int64, compression_level)

namespace
{
    /// `s3_storage_class_name` addresses the `s3_storage_class` field (see `fromBackupQuery`), so the two
    /// spellings must resolve as one setting: defaulting either has to drop a change written as the other.
    std::string_view canonicalBackupSettingName(std::string_view name)
    {
        if (name == "s3_storage_class_name")
            return "s3_storage_class";
        return name;
    }

    /// The canonical names of the backup-specific settings. The two extra names are `BackupSettings` fields
    /// handled by their own branches in `fromBackupQuery` and kept out of the macro, so a macro-only
    /// classifier would call them core settings and try to reset them on the query context instead. The
    /// `s3_storage_class_name` alias needs no entry: `canonicalBackupSettingName` maps it onto its target.
    constexpr std::string_view BACKUP_SPECIFIC_SETTING_NAMES[] = {
#define BACKUP_SETTING_NAME(TYPE, NAME) #NAME,
        LIST_OF_BACKUP_SETTINGS(BACKUP_SETTING_NAME)
#undef BACKUP_SETTING_NAME
        "compression_level",
        "data_file_name_prefix_length",
    };

    SettingsWithDefaultsResolved resolveBackupSettings(const ASTBackupQuery & query)
    {
        return resolveDefaultedSettings(query, BACKUP_SPECIFIC_SETTING_NAMES, canonicalBackupSettingName);
    }
}

BackupSettings BackupSettings::fromBackupQuery(const ASTBackupQuery & query)
{
    BackupSettings res;

    {
        const auto & settings = resolveBackupSettings(query).changes;
        for (const auto & setting : settings)
        {
            if (setting.name == "compression_level")
                res.compression_level = static_cast<int>(SettingFieldInt64{setting.value}.value);
            else if (setting.name == "data_file_name_prefix_length")
                res.data_file_name_prefix_length = setting.value.safeGet<UInt64>();
            /// `s3_storage_class_name` is an alias for `s3_storage_class`: the disk configuration uses the
            /// former (the canonical request setting name) while the BACKUP command uses the latter. Accept
            /// both spellings in both places so they are interchangeable. See issue #68551.
            else if (setting.name == "s3_storage_class_name")
                res.s3_storage_class = SettingFieldString{setting.value}.value;
            else
#define GET_BACKUP_SETTINGS_FROM_QUERY(TYPE, NAME) \
            if (setting.name == #NAME) \
                res.NAME = SettingField##TYPE{setting.value}.value; \
            else

            LIST_OF_BACKUP_SETTINGS(GET_BACKUP_SETTINGS_FROM_QUERY)
            /// else
            {
                /// (if setting.name is not the name of a field of BackupSettings)
                res.core_settings.emplace_back(setting);
            }
        }
    }

    if (query.base_backup_name)
        res.base_backup_info = BackupInfo::fromAST(*query.base_backup_name);

    if (query.cluster_host_ids)
        res.cluster_host_ids = Util::clusterHostIDsFromAST(*query.cluster_host_ids);

#if CLICKHOUSE_CLOUD
    if (res.resumable_backup_batch_size == 0)
        throw Exception(ErrorCodes::WRONG_BACKUP_SETTINGS, "Setting `resumable_backup_batch_size` must be greater than 0");
    if (res.resumable_backup_batch_size_bytes == 0)
        throw Exception(ErrorCodes::WRONG_BACKUP_SETTINGS, "Setting `resumable_backup_batch_size_bytes` must be greater than 0");
#endif

    return res;
}

bool BackupSettings::isAsync(const ASTBackupQuery & query)
{
    /// This runs before `fromBackupQuery` (BackupsWorker decides where to run the operation first), so it
    /// resolves `async = DEFAULT` on its own. It must reach the same value `fromBackupQuery` will: hence the
    /// last of several `async` changes, and the same field conversion. One name, so no classification.
    if (query.settings)
    {
        const auto & settings = query.settings->as<const ASTSetQuery &>();
        if (std::ranges::find(settings.default_settings, "async") == settings.default_settings.end())
        {
            auto it = std::find_if(
                settings.changes.rbegin(),
                settings.changes.rend(),
                [](const SettingChange & change) { return change.name == "async"; });
            if (it != settings.changes.rend())
                return SettingFieldBool{it->value}.value;
        }
    }
    return false; /// `async` is false by default.
}

CoreSettingsFromQuery BackupSettings::extractCoreSettingsFromQuery(const ASTBackupQuery & query)
{
    return extractCoreSettings(query, BACKUP_SPECIFIC_SETTING_NAMES, canonicalBackupSettingName);
}

void BackupSettings::copySettingsToQuery(ASTBackupQuery & query) const
{
    auto query_settings = make_intrusive<ASTSetQuery>();
    query_settings->is_standalone = false;

    /// Copy the fields of the BackupSettings to the query.
    static const BackupSettings default_settings;

#define COPY_BACKUP_SETTINGS_TO_QUERY(TYPE, NAME) \
    if ((NAME) != default_settings.NAME) \
        query_settings->changes.emplace_back(#NAME, static_cast<Field>(SettingField##TYPE{NAME})); \

    LIST_OF_BACKUP_SETTINGS(COPY_BACKUP_SETTINGS_TO_QUERY)

    /// Copy the core settings to the query too.
    query_settings->changes.insert(query_settings->changes.end(), core_settings.begin(), core_settings.end());

    /// Carry over only the CORE `name = DEFAULT` items: they describe a reset on the receiving host's query
    /// context, which this rebuild cannot otherwise express. A backup-specific one must NOT ride along -
    /// the rebuild emits resolved effective state, so re-resolving a defaulted name would discard state
    /// generated since parsing (`backup_uuid` is the concrete case).
    query_settings->default_settings = extractCoreSettingsFromQuery(query).default_names;

    if (query_settings->changes.empty() && query_settings->default_settings.empty())
        query_settings = nullptr;

    query.settings = query_settings;

    auto base_backup_name = base_backup_info ? base_backup_info->toAST() : nullptr;
    if (base_backup_name)
        query.setOrReplace(query.base_backup_name, base_backup_name);
    else
        query.reset(query.base_backup_name);

    query.cluster_host_ids = !cluster_host_ids.empty() ? Util::clusterHostIDsToAST(cluster_host_ids) : nullptr;
}

std::map<String, String> BackupSettings::getSerializedSettings() const
{
    std::map<String, String> res;

    /// Serialize via the setting field's own `toString` (the canonical representation, consistent with
    /// `system.query_log.Settings` and `engine_settings`) rather than going through `FieldVisitorToString`.
#define SERIALIZE_BACKUP_SETTING(TYPE, NAME) \
    res[#NAME] = SettingField##TYPE{NAME}.toString();

    LIST_OF_BACKUP_SETTINGS(SERIALIZE_BACKUP_SETTING)
#undef SERIALIZE_BACKUP_SETTING

    /// Settings handled specially in `fromBackupQuery` and not part of `LIST_OF_BACKUP_SETTINGS`.
    res["compression_level"] = std::to_string(compression_level);
    if (data_file_name_prefix_length)
        res["data_file_name_prefix_length"] = std::to_string(*data_file_name_prefix_length);

    /// Never expose the password; drop purely internal fields that are not user-facing settings
    /// (`id` has its own column, the rest are internal plumbing for BACKUP ON CLUSTER).
    for (const auto * key : {"password", "id", "internal", "host_id", "backup_uuid"})
        res.erase(key);

    return res;
}

std::vector<Strings> BackupSettings::Util::clusterHostIDsFromAST(const IAST & ast)
{
    std::vector<Strings> res;

    auto extract_replicas = [](const Array & replicas) -> Strings
    {
        Strings result(replicas.size());
        for (size_t j = 0; j != replicas.size(); ++j)
        {
            if (replicas[j].getType() != Field::Types::String)
                throw Exception(
                    ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS,
                    "Setting cluster_host_ids has wrong format, must be array of arrays of string literals");
            result[j] = replicas[j].safeGet<String>();
        }
        return result;
    };

    /// The parser may produce either ASTLiteral(Array{Array{...}, ...}) when
    /// all elements are plain literals, or ASTFunction("array", [ASTLiteral(Array), ...])
    /// when the slow path was taken. Handle both representations.
    if (const auto * literal = typeid_cast<const ASTLiteral *>(&ast))
    {
        if (literal->value.getType() != Field::Types::Array)
            throw Exception(
                ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS,
                "Setting cluster_host_ids has wrong format, must be array of arrays of string literals");

        const auto & shards = literal->value.safeGet<Array>();
        res.resize(shards.size());
        for (size_t i = 0; i != shards.size(); ++i)
        {
            if (shards[i].getType() != Field::Types::Array)
                throw Exception(
                    ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS,
                    "Setting cluster_host_ids has wrong format, must be array of arrays of string literals");
            res[i] = extract_replicas(shards[i].safeGet<Array>());
        }
        return res;
    }

    const auto * array_of_shards = typeid_cast<const ASTFunction *>(&ast);
    if (!array_of_shards || (array_of_shards->name != "array"))
        throw Exception(
            ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS,
            "Setting cluster_host_ids has wrong format, must be array of arrays of string literals");

    if (array_of_shards->arguments)
    {
        const ASTs shards = array_of_shards->arguments->children;
        res.resize(shards.size());

        for (size_t i = 0; i != shards.size(); ++i)
        {
            const auto * array_of_replicas = typeid_cast<const ASTLiteral *>(shards[i].get());
            if (!array_of_replicas || (array_of_replicas->value.getType() != Field::Types::Array))
                throw Exception(
                    ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS,
                    "Setting cluster_host_ids has wrong format, must be array of arrays of string literals");
            res[i] = extract_replicas(array_of_replicas->value.safeGet<Array>());
        }
    }

    return res;
}

ASTPtr BackupSettings::Util::clusterHostIDsToAST(const std::vector<Strings> & cluster_host_ids)
{
    if (cluster_host_ids.empty())
        return nullptr;

    /// Build as ASTLiteral(Array{Array{String, ...}, ...}) so that FieldVisitorToString
    /// always formats it with [...] syntax, which is compatible with all ClickHouse versions.
    /// Using ASTFunction("array") with operator syntax would trigger the all-literals formatting
    /// path and produce array(...) syntax that older versions cannot parse.
    Array shards_array;
    shards_array.resize(cluster_host_ids.size());

    for (size_t i = 0; i != cluster_host_ids.size(); ++i)
    {
        const auto & shard = cluster_host_ids[i];

        Array res_shard;
        res_shard.resize(shard.size());
        for (size_t j = 0; j != shard.size(); ++j)
            res_shard[j] = Field{shard[j]};

        shards_array[i] = Field{std::move(res_shard)};
    }

    return make_intrusive<ASTLiteral>(Field{std::move(shards_array)});
}

std::pair<size_t, size_t> BackupSettings::Util::findShardNumAndReplicaNum(const std::vector<Strings> & cluster_host_ids, const String & host_id)
{
    for (size_t i = 0; i != cluster_host_ids.size(); ++i)
    {
        for (size_t j = 0; j != cluster_host_ids[i].size(); ++j)
            if (cluster_host_ids[i][j] == host_id)
                return {i + 1, j + 1};
    }
    throw Exception(ErrorCodes::WRONG_BACKUP_SETTINGS,
                    "Cannot determine shard number or replica number, the current host {} is not found "
                    "in the cluster's hosts", host_id);
}

Strings BackupSettings::Util::filterHostIDs(const std::vector<Strings> & cluster_host_ids, size_t only_shard_num, size_t only_replica_num)
{
    Strings collected_host_ids;

    auto collect_replicas = [&](size_t shard_index)
    {
        const auto & shard = cluster_host_ids[shard_index - 1];
        if (only_replica_num)
        {
            if (only_replica_num <= shard.size())
                collected_host_ids.push_back(shard[only_replica_num - 1]);
        }
        else
        {
            for (size_t replica_index = 1; replica_index <= shard.size(); ++replica_index)
                collected_host_ids.push_back(shard[replica_index - 1]);
        }
    };

    if (only_shard_num)
    {
        if (only_shard_num <= cluster_host_ids.size())
            collect_replicas(only_shard_num);
    }
    else
    {
        for (size_t shard_index = 1; shard_index <= cluster_host_ids.size(); ++shard_index)
            collect_replicas(shard_index);
    }

    return collected_host_ids;
}

}
