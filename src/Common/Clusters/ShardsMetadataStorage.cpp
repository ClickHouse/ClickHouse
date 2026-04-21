#include <Common/Clusters/ShardsMetadataStorage.h>

#include <Common/Clusters/SQLClusterCatalogPropertyValidation.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateClusterCatalogQuery.h>
#include <Parsers/ParserCreateClusterCatalogQuery.h>
#include <Parsers/parseQuery.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

using namespace SQLClusterCatalog;

namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int BAD_CLUSTER_DEFINITION;
    extern const int SHARD_DOESNT_EXIST;
}

namespace
{

constexpr auto SHARDS_CATALOG_STORAGE_CONFIG_PREFIX = "shards_catalog_storage";

ShardCatalogDefinition parseShardCreateStatement(const String & query_text, const String & path_for_error, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    ParserCreateClusterCatalogQuery parser;
    auto ast = parseQuery(
        parser,
        query_text,
        path_for_error,
        0,
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);
    const auto & q = ast->as<const ASTCreateClusterCatalogQuery &>();
    ShardCatalogDefinition row;
    row.replica_collections = q.members;
    PropertyValidation::Shard::validateAndExtract(q.properties, row.weight, row.internal_replication);
    return row;
}

}

String ShardsMetadataStorage::fileNameForShard(const String & shard_name)
{
    return escapeForFileName(shard_name) + ".sql";
}

std::unique_ptr<ShardsMetadataStorage> ShardsMetadataStorage::create(const ContextPtr & context_, const String & default_local_directory_path)
{
    auto catalog_metadata_storage
        = ClusterCatalogMetadataStorage::create(context_, SHARDS_CATALOG_STORAGE_CONFIG_PREFIX, default_local_directory_path);
    return std::unique_ptr<ShardsMetadataStorage>(
        new ShardsMetadataStorage(context_, std::move(catalog_metadata_storage)));
}

ShardsMetadataStorage::ShardsMetadataStorage(
    ContextPtr context_, std::unique_ptr<ClusterCatalogMetadataStorage> catalog_metadata_storage_)
    : WithContext(context_)
    , catalog_metadata_storage(std::move(catalog_metadata_storage_))
{
}

void ShardsMetadataStorage::shutdown()
{
    if (catalog_metadata_storage)
        catalog_metadata_storage->shutdown();
    catalog_metadata_storage.reset();
}

bool ShardsMetadataStorage::isReplicated() const
{
    return catalog_metadata_storage && catalog_metadata_storage->isReplicated();
}

bool ShardsMetadataStorage::waitCatalogUpdate()
{
    if (!catalog_metadata_storage || !catalog_metadata_storage->isReplicated())
        return false;
    return catalog_metadata_storage->waitUpdate();
}

std::unordered_map<String, ShardCatalogDefinition> ShardsMetadataStorage::getAll() const
{
    std::unordered_map<String, ShardCatalogDefinition> result;

    for (const auto & path_str : catalog_metadata_storage->list())
    {
        const fs::path p(path_str);
        if (p.extension() != ".sql")
            continue;

        const String name = unescapeForFileName(p.stem().string());
        if (result.contains(name))
            throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Duplicate SQL SHARD definition name `{}`", name);

        try
        {
            const String query_text = catalog_metadata_storage->read(fileNameForShard(name));
            result.emplace(name, parseShardCreateStatement(query_text, name, getContext()));
        }
        catch (const Coordination::Exception & e)
        {
            if (e.code == Coordination::Error::ZNONODE)
            {
                LOG_DEBUG(logger, "SQL SHARD '{}' was removed while reading, skipping", name);
                continue;
            }
            throw;
        }
    }
    return result;
}

void ShardsMetadataStorage::writeCreateStatement(const String & shard_name, const String & create_statement_sql, bool replace)
{
    catalog_metadata_storage->write(fileNameForShard(shard_name), create_statement_sql, replace);
}

void ShardsMetadataStorage::remove(const String & shard_name)
{
    if (!removeIfExists(shard_name))
        throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "SQL SHARD `{}` metadata file does not exist", shard_name);
}

bool ShardsMetadataStorage::removeIfExists(const String & shard_name)
{
    return catalog_metadata_storage->removeIfExists(fileNameForShard(shard_name));
}

}
