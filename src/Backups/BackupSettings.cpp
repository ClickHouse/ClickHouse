#include <Backups/BackupInfo.h>
#include <Backups/BackupSettings.h>
#include <Core/SettingsFields.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <IO/ReadHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_BACKUP_SETTINGS;
    extern const int WRONG_BACKUP_SETTINGS;
}


namespace
{
    struct SettingFieldOptionalUUID
    {
        std::optional<UUID> value;

        explicit SettingFieldOptionalUUID(const std::optional<UUID> & value_) : value(value_) {}

        explicit SettingFieldOptionalUUID(const Field & field)
        {
            if (field.getType() == Field::Types::Null)
            {
                value = std::nullopt;
                return;
            }

            if (field.getType() == Field::Types::String)
            {
                const String & str = field.get<const String &>();
                if (str.empty())
                {
                    value = std::nullopt;
                    return;
                }

                UUID id;
                if (tryParse(id, str))
                {
                    value = id;
                    return;
                }
            }

            throw Exception(ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS, "Cannot parse uuid from {}", field);
        }

        explicit operator Field() const { return Field(value ? toString(*value) : ""); }
    };
}


/// List of backup settings except base_backup_name and cluster_host_ids.
#define LIST_OF_BACKUP_SETTINGS(M) \
    M(String, id) \
    M(String, compression_method) \
    M(Int64, compression_level) \
    M(String, password) \
    M(Bool, structure_only) \
    M(Bool, async) \
    M(UInt64, shard_num) \
    M(UInt64, replica_num) \
    M(Bool, internal) \
    M(String, host_id) \
    M(String, coordination_zk_path) \
    M(OptionalUUID, backup_uuid)

BackupSettings BackupSettings::fromBackupQuery(const ASTBackupQuery & query)
{
    BackupSettings res;

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

    if (query.base_backup_name)
        res.base_backup_info = BackupInfo::fromAST(*query.base_backup_name);

    if (query.cluster_host_ids)
        res.cluster_host_ids = Util::clusterHostIDsFromAST(*query.cluster_host_ids);

    return res;
}

void BackupSettings::copySettingsToQuery(ASTBackupQuery & query) const
{
    auto query_settings = std::make_shared<ASTSetQuery>();
    query_settings->is_standalone = false;

    static const BackupSettings default_settings;
    bool all_settings_are_default = true;

#define SET_SETTINGS_IN_BACKUP_QUERY_HELPER(TYPE, NAME) \
    if ((NAME) != default_settings.NAME) \
    { \
        query_settings->changes.emplace_back(#NAME, static_cast<Field>(SettingField##TYPE{NAME})); \
        all_settings_are_default = false; \
    }

    LIST_OF_BACKUP_SETTINGS(SET_SETTINGS_IN_BACKUP_QUERY_HELPER)

    if (all_settings_are_default)
        query_settings = nullptr;

    query.settings = query_settings;

    query.base_backup_name = base_backup_info ? base_backup_info->toAST() : nullptr;
    query.cluster_host_ids = !cluster_host_ids.empty() ? Util::clusterHostIDsToAST(cluster_host_ids) : nullptr;
}

std::vector<Strings> BackupSettings::Util::clusterHostIDsFromAST(const IAST & ast)
{
    std::vector<Strings> res;

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
            const auto & replicas = array_of_replicas->value.get<const Array &>();
            res[i].resize(replicas.size());
            for (size_t j = 0; j != replicas.size(); ++j)
            {
                const auto & replica = replicas[j];
                if (replica.getType() != Field::Types::String)
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_BACKUP_SETTINGS,
                        "Setting cluster_host_ids has wrong format, must be array of arrays of string literals");
                res[i][j] = replica.get<const String &>();
            }
        }
    }

    return res;
}

ASTPtr BackupSettings::Util::clusterHostIDsToAST(const std::vector<Strings> & cluster_host_ids)
{
    if (cluster_host_ids.empty())
        return nullptr;

    auto res = std::make_shared<ASTFunction>();
    res->name = "array";
    auto res_replicas = std::make_shared<ASTExpressionList>();
    res->arguments = res_replicas;
    res->children.push_back(res_replicas);
    res_replicas->children.resize(cluster_host_ids.size());

    for (size_t i = 0; i != cluster_host_ids.size(); ++i)
    {
        const auto & shard = cluster_host_ids[i];

        Array res_shard;
        res_shard.resize(shard.size());
        for (size_t j = 0; j != shard.size(); ++j)
            res_shard[j] = Field{shard[j]};

        res_replicas->children[i] = std::make_shared<ASTLiteral>(Field{std::move(res_shard)});
    }

    return res;
}

std::pair<size_t, size_t> BackupSettings::Util::findShardNumAndReplicaNum(const std::vector<Strings> & cluster_host_ids, const String & host_id)
{
    for (size_t i = 0; i != cluster_host_ids.size(); ++i)
    {
        for (size_t j = 0; j != cluster_host_ids[i].size(); ++j)
            if (cluster_host_ids[i][j] == host_id)
                return {i + 1, j + 1};
    }
    throw Exception(ErrorCodes::WRONG_BACKUP_SETTINGS, "Cannot determine shard number or replica number, the current host {} is not found in the cluster's hosts", host_id);
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
