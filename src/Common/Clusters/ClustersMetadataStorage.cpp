#include <Common/Clusters/ClustersMetadataStorage.h>

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

namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int BAD_CLUSTER_DEFINITION;
    extern const int CLUSTER_DEFINITION_DOESNT_EXIST;
}

namespace
{

constexpr auto CLUSTERS_CATALOG_STORAGE_CONFIG_PREFIX = "clusters_catalog_storage";

ClusterCatalogDefinition parseClusterCreateStatement(const String & query_text, const String & path_for_error, const ContextPtr & context)
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
    ClusterCatalogDefinition row;
    row.members = q.members;
    validateAndExtractClusterLevelProperties(q.properties, row.secret, row.allow_distributed_ddl_queries);
    return row;
}

}

String ClustersMetadataStorage::fileNameForCluster(const String & cluster_name)
{
    return escapeForFileName(cluster_name) + ".sql";
}

std::unique_ptr<ClustersMetadataStorage> ClustersMetadataStorage::create(const ContextPtr & context_, const String & default_local_directory_path)
{
    auto catalog_metadata_storage
        = ClusterCatalogMetadataStorage::create(context_, CLUSTERS_CATALOG_STORAGE_CONFIG_PREFIX, default_local_directory_path);
    return std::unique_ptr<ClustersMetadataStorage>(
        new ClustersMetadataStorage(context_, std::move(catalog_metadata_storage)));
}

ClustersMetadataStorage::ClustersMetadataStorage(
    ContextPtr context_, std::unique_ptr<ClusterCatalogMetadataStorage> catalog_metadata_storage_)
    : WithContext(context_)
    , catalog_metadata_storage(std::move(catalog_metadata_storage_))
{
}

void ClustersMetadataStorage::shutdown()
{
    if (catalog_metadata_storage)
        catalog_metadata_storage->shutdown();
    catalog_metadata_storage.reset();
}

bool ClustersMetadataStorage::isReplicated() const
{
    return catalog_metadata_storage && catalog_metadata_storage->isReplicated();
}

bool ClustersMetadataStorage::waitCatalogUpdate()
{
    if (!catalog_metadata_storage || !catalog_metadata_storage->isReplicated())
        return false;
    return catalog_metadata_storage->waitUpdate();
}

std::unordered_map<String, ClusterCatalogDefinition> ClustersMetadataStorage::getAll() const
{
    std::unordered_map<String, ClusterCatalogDefinition> result;

    for (const auto & path_str : catalog_metadata_storage->list())
    {
        const fs::path p(path_str);
        if (p.extension() != ".sql")
            continue;

        const String name = unescapeForFileName(p.stem().string());
        if (result.contains(name))
            throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Duplicate SQL CLUSTER definition name `{}`", name);

        try
        {
            const String query_text = catalog_metadata_storage->read(fileNameForCluster(name));
            result.emplace(name, parseClusterCreateStatement(query_text, name, getContext()));
        }
        catch (const Coordination::Exception & e)
        {
            if (e.code == Coordination::Error::ZNONODE)
            {
                LOG_DEBUG(logger, "SQL CLUSTER '{}' was removed while reading, skipping", name);
                continue;
            }
            throw;
        }
    }
    return result;
}

void ClustersMetadataStorage::writeCreateStatement(const String & cluster_name, const String & create_statement_sql, bool replace)
{
    catalog_metadata_storage->write(fileNameForCluster(cluster_name), create_statement_sql, replace);
}

void ClustersMetadataStorage::remove(const String & cluster_name)
{
    if (!removeIfExists(cluster_name))
        throw Exception(ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "SQL CLUSTER `{}` metadata file does not exist", cluster_name);
}

bool ClustersMetadataStorage::removeIfExists(const String & cluster_name)
{
    return catalog_metadata_storage->removeIfExists(fileNameForCluster(cluster_name));
}

}
