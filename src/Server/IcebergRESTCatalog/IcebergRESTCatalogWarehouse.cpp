#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouse.h>

#include "config.h"

#include <Common/Exception.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>

#if USE_AWS_S3
#include <Storages/ObjectStorage/S3/Configuration.h>
#endif

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

constexpr std::string_view S3_SCHEME = "s3://";

using S3Location = IcebergRESTCatalogWarehouse::S3Location;

/// Splits `s3://<bucket>/<key>` into the bucket and the key. The key may be empty.
S3Location parseS3Location(const String & location)
{
    /// TODO: support s3a:// and s3n://
    if (!location.starts_with(S3_SCHEME))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Location {} is not an s3:// URI", location);
    const auto slash = location.find('/', S3_SCHEME.size());
    if (slash == String::npos)
        return S3Location{.bucket = location.substr(S3_SCHEME.size()), .key = ""};
    return S3Location{.bucket = location.substr(S3_SCHEME.size(), slash - S3_SCHEME.size()), .key = location.substr(slash + 1)};
}

/// S3 client of the warehouse. Same arguments as `s3(url, extra_credentials(role_arn = ...))`.
/// The endpoint comes from `url_scheme_mappers` in the server config, AWS by default.
ObjectStoragePtr createObjectStorage([[maybe_unused]] ContextPtr context, [[maybe_unused]] const IcebergRESTCatalogWarehouseDefinition & definition)
{
#if USE_AWS_S3
    const auto & credential = definition.credential;
    auto credentials_ast = makeASTFunction("extra_credentials");
    for (const auto & [key, value] : std::initializer_list<std::pair<String, String>>{
             {"role_arn", credential.role_arn}, {"external_id", credential.external_id}, {"role_session_name", credential.role_session_name}})
    {
        if (!value.empty())
            credentials_ast->arguments->children.push_back(
                makeASTFunction("equals", make_intrusive<ASTLiteral>(key), make_intrusive<ASTLiteral>(value)));
    }

    ASTs args{make_intrusive<ASTLiteral>(definition.base_location), credentials_ast};
    auto configuration = std::make_shared<StorageS3Configuration>();
    StorageObjectStorageConfiguration::initialize(*configuration, args, context, /*with_table_structure*/ false);
    return configuration->createObjectStorage(context, /*is_readonly*/ false, std::nullopt);
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The Iceberg REST catalog needs S3 support, but this build has none");
#endif
}

}

IcebergRESTCatalogWarehouse::IcebergRESTCatalogWarehouse(
    String name_, String base_location_, KeeperIcebergRESTCatalogStorePtr store_, ObjectStoragePtr object_storage_)
    : name(std::move(name_)), base_location(std::move(base_location_)), store(std::move(store_)), object_storage(std::move(object_storage_))
{
    /// The warehouse has one S3 client, bound to one bucket. Every location must be in that bucket.
    validateLocation(base_location);
}

void IcebergRESTCatalogWarehouse::validateLocation(const String & location) const
{
    validateLocation(parseS3Location(location));
}

void IcebergRESTCatalogWarehouse::validateLocation(const S3Location & location) const
{
    if (location.bucket != object_storage->getObjectsNamespace())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Location s3://{}/{} is outside bucket '{}' of warehouse {}",
            location.bucket,
            location.key,
            object_storage->getObjectsNamespace(),
            name);
    /// `S3ObjectStorage` re-parses a key of the form `<scheme>://<bucket>/...` and writes to the embedded bucket.
    if (location.key.contains("://"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Location s3://{}/{} contains an embedded URI", location.bucket, location.key);
}

String IcebergRESTCatalogWarehouse::objectKey(const String & location) const
{
    auto parsed = parseS3Location(location);
    validateLocation(parsed);
    return std::move(parsed.key);
}

String stripTrailingSlashes(String location)
{
    while (location.ends_with('/'))
        location.pop_back();
    return location;
}

IcebergRESTCatalogWarehousePtr createIcebergRESTCatalogWarehouse(
    ContextPtr context, const String & name, const IcebergRESTCatalogWarehouseDefinition & definition, const IcebergRESTCatalogKeeperConfig & keeper_config)
{
    auto store = std::make_shared<KeeperIcebergRESTCatalogStore>(
        [context, keeper_config] { return keeper_config.getZooKeeper(*context); }, keeper_config.warehousePath(name));
    auto object_storage = createObjectStorage(context, definition);
    return std::make_shared<const IcebergRESTCatalogWarehouse>(name, definition.base_location, std::move(store), std::move(object_storage));
}

}
