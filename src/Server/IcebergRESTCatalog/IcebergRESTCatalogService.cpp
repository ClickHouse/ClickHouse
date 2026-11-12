#include <Server/IcebergRESTCatalog/IcebergRESTCatalogService.h>

#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogCommitTable.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogCreateTable.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogStorage.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>

#include <Common/ZooKeeper/KeeperException.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/String.h>

#include <boost/algorithm/string/split.hpp>
#include <fmt/ranges.h>

#include <algorithm>
#include <set>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_ddl;
    extern const SettingsUInt64 output_format_compression_level;
    extern const SettingsUInt64 readonly;
}

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int METADATA_MISMATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int QUERY_IS_PROHIBITED;
    extern const int READONLY;
    extern const int ICEBERG_CATALOG_CONCURRENT_UPDATE;
    extern const int KEEPER_EXCEPTION;
    extern const int NO_ZOOKEEPER;
    extern const int REQUIRED_PASSWORD;
    extern const int SUPPORT_IS_DISABLED;
}

using namespace IcebergRESTCatalog;

namespace
{

/// The Iceberg table property that names the codec of metadata files.
constexpr auto METADATA_COMPRESSION_CODEC_PROPERTY = "write.metadata.compression-codec";

CompressionMethod getMetadataCompressionMethod(const Poco::JSON::Object & metadata)
{
    const auto properties = getObject(metadata, Iceberg::f_properties);
    const auto codec = properties->optValue<String>(METADATA_COMPRESSION_CODEC_PROPERTY, "none");
    const auto lower = Poco::toLower(codec);
    if (lower == "none")
        return CompressionMethod::None;
    if (lower == "gzip")
        return CompressionMethod::Gzip;
    /// The spec defines only none and gzip. Other codecs would leave files that other engines cannot open.
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' must be 'none' or 'gzip', got '{}'", METADATA_COMPRESSION_CODEC_PROPERTY, codec);
}

/// Tables of the namespace go under `location`. `execute` maps `BAD_ARGUMENTS` to a 400 response.
void validateNamespaceLocation(const IcebergRESTCatalogWarehouse & warehouse, const std::map<String, String> & properties)
{
    if (const auto it = properties.find("location"); it != properties.end())
        warehouse.validateLocation(it->second);
}

/// Codec suffix inside a metadata file name: `v1-<uuid>.gz.metadata.json`. The spec spells gzip as `gz`.
String getMetadataCompressionSuffix(CompressionMethod compression_method)
{
    return compression_method == CompressionMethod::Gzip ? ".gz" : "";
}

IcebergNamespaceName splitNamespace(const String & value)
{
    IcebergNamespaceName result;
    boost::split(result, value, [](char c) { return c == NAMESPACE_LEVEL_SEPARATOR; });
    return result;
}

String joinNamespace(const IcebergNamespaceName & name)
{
    return fmt::format("{}", fmt::join(name, "."));
}

IcebergNamespaceName parseNamespaceArray(const Poco::JSON::Object & json)
{
    const auto namespace_array = json.getArray("namespace");
    if (!namespace_array)
        throw Poco::Exception("'namespace' must be an array");

    IcebergNamespaceName result;
    for (const auto & level : *namespace_array)
        result.push_back(level.extract<String>());
    return result;
}

struct TableIdentifier
{
    IcebergNamespaceName ns;
    String name;
};

/// Parses a `TableIdentifier`: `namespace` is an array of levels, `name` is a string.
TableIdentifier parseTableIdentifier(const Poco::JSON::Object::Ptr & json)
{
    if (!json)
        throw Poco::Exception("table identifier must be an object");
    if (!json->has("name") || !json->get("name").isString())
        throw Poco::Exception("'name' must be a string");
    return {parseNamespaceArray(*json), json->getValue<String>("name")};
}

Poco::JSON::Array stringsToJSONArray(const Strings & strings)
{
    Poco::JSON::Array result;
    for (const auto & value : strings)
        result.add(value);
    return result;
}

std::optional<String> getQueryParameter(const Poco::URI & uri, const String & name)
{
    for (const auto & [key, value] : uri.getQueryParameters())
    {
        if (key == name)
            return value;
    }
    return std::nullopt;
}

/// Boolean query parameters arrive as `true`, `True` or `TRUE` depending on the client, so compare case-insensitively.
/// Throws for a value that is not a boolean.
bool getBooleanQueryParameter(const Poco::URI & uri, const String & name, bool default_value)
{
    const auto value = getQueryParameter(uri, name);
    if (!value)
        return default_value;
    if (Poco::icompare(*value, "true") == 0)
        return true;
    if (Poco::icompare(*value, "false") == 0)
        return false;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Query parameter '{}' must be 'true' or 'false', got '{}'", name, *value);
}

Poco::JSON::Object tableIdentifierToJSON(const IcebergNamespaceName & ns, const String & table)
{
    Poco::JSON::Object result;
    result.set("namespace", stringsToJSONArray(ns));
    result.set("name", table);
    return result;
}

}

IcebergRESTCatalogService::IcebergRESTCatalogService(ContextPtr global_context_, IcebergRESTCatalogWarehouseRegistryPtr warehouses_)
    : WithContext(global_context_)
    , log(getLogger("IcebergRESTCatalogService"))
    , warehouses(std::move(warehouses_))
{
}

IcebergRESTResponse IcebergRESTCatalogService::makeJSON(const Poco::JSON::Object & json, Poco::Net::HTTPResponse::HTTPStatus status)
{
    return {.status = status, .body = toJSONString(json)};
}

IcebergRESTResponse IcebergRESTCatalogService::makeNoContent()
{
    return {.status = Poco::Net::HTTPResponse::HTTP_NO_CONTENT, .body = {}};
}

IcebergRESTResponse IcebergRESTCatalogService::makeError(Poco::Net::HTTPResponse::HTTPStatus status, const String & type, const String & message)
{
    LOG_DEBUG(getLogger("IcebergRESTCatalogService"), "Request rejected with {} {}: {}", static_cast<int>(status), type, message);

    /// ErrorModel from the Iceberg REST specification.
    Poco::JSON::Object error;
    error.set("message", message);
    error.set("type", type);
    error.set("code", static_cast<int>(status));

    Poco::JSON::Object result;
    result.set("error", error);
    return makeJSON(result, status);
}

IcebergRESTResponse IcebergRESTCatalogService::makeNoSuchNamespace(const IcebergNamespaceName & ns)
{
    return makeError(
        Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
        "NoSuchNamespaceException",
        fmt::format("Namespace does not exist: {}", joinNamespace(ns)));
}

IcebergRESTResponse IcebergRESTCatalogService::makeNoSuchTable(const IcebergNamespaceName & ns, const String & table)
{
    return makeError(
        Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
        "NoSuchTableException",
        fmt::format("Table does not exist: {}.{}", joinNamespace(ns), table));
}

IcebergRESTResponse IcebergRESTCatalogService::execute(const String & method, const String & request_uri, const String & body, const Context * query_context) const
{
    try
    {
        Poco::URI uri;
        std::vector<std::string> segments;
        try
        {
            /// Split before decoding, so that an encoded `/` (`%2F`) stays inside its segment.
            uri = Poco::URI(request_uri, /* enable_url_encoding */ false);
            uri.getPathSegments(segments);
            for (auto & segment : segments)
            {
                String decoded;
                Poco::URI::decode(segment, decoded);
                segment = std::move(decoded);
            }
        }
        catch (const Poco::Exception &)
        {
            return makeError(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "BadRequestException", "Malformed request URI");
        }

        auto match = matchIcebergRESTRoute(method, segments);
        if (!match)
        {
            return makeError(
                Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
                "NotFoundException",
                fmt::format("No route for {} {}", method, uri.getPath()));
        }

        /// The spec reserves 406 UnsupportedOperationResponse for operations the server does not support.
        if (!match->route->implemented)
        {
            return makeError(
                Poco::Net::HTTPResponse::HTTP_NOT_ACCEPTABLE,
                "UnsupportedOperationException",
                fmt::format("Not implemented: {}", toString(match->route->operation)));
        }

        /// `GET /v1/config` has no prefix. It gets the warehouse from the query string instead.
        if (match->route->operation == IcebergRESTOperation::GetConfig)
            return handleGetConfig(uri);

        const auto & prefix = match->path_params.at("prefix");
        const auto warehouse = warehouses->tryGet(prefix);
        if (!warehouse)
        {
            return makeError(
                Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
                "NotFoundException",
                fmt::format("Unknown prefix: {}", prefix));
        }

        switch (match->route->operation)
        {
            case IcebergRESTOperation::ListNamespaces:
                return handleListNamespaces(*warehouse, uri);
            case IcebergRESTOperation::CreateNamespace:
                return handleCreateNamespace(*warehouse, body, query_context);
            case IcebergRESTOperation::LoadNamespace:
                return handleLoadNamespace(*warehouse, *match);
            case IcebergRESTOperation::NamespaceExists:
                return handleNamespaceExists(*warehouse, *match);
            case IcebergRESTOperation::UpdateNamespaceProperties:
                return handleUpdateNamespaceProperties(*warehouse, *match, body, query_context);
            case IcebergRESTOperation::ListTables:
                return handleListTables(*warehouse, *match);
            case IcebergRESTOperation::CreateTable:
                return handleCreateTable(*warehouse, *match, body, query_context);
            case IcebergRESTOperation::LoadTable:
                return handleLoadTable(*warehouse, *match);
            case IcebergRESTOperation::TableExists:
                return handleTableExists(*warehouse, *match);
            case IcebergRESTOperation::DropTable:
                return handleDropTable(*warehouse, *match, uri, query_context);
            case IcebergRESTOperation::UpdateTable:
                return handleUpdateTable(*warehouse, *match, body, query_context);
            case IcebergRESTOperation::RenameTable:
                return handleRenameTable(*warehouse, body, query_context);
            case IcebergRESTOperation::ReportMetrics:
                return handleReportMetrics(*warehouse, *match);
            default:
                return makeError(
                    Poco::Net::HTTPResponse::HTTP_NOT_ACCEPTABLE,
                    "UnsupportedOperationException",
                    fmt::format("Not implemented: {}", toString(match->route->operation)));
        }
    }
    catch (...)
    {
        return makeErrorFromCurrentException();
    }
}

IcebergRESTResponse IcebergRESTCatalogService::makeErrorFromCurrentException() const
{
    auto status = Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
    String type = "InternalServerError";
    String message = "Internal server error";

    const int code = getCurrentExceptionCode();
    if (code == ErrorCodes::BAD_ARGUMENTS)
    {
        status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
        type = "BadRequestException";
        message = getCurrentExceptionMessage(false);
    }
    /// `AccessControl` reports a wrong password for the `default` user as `REQUIRED_PASSWORD`.
    else if (code == ErrorCodes::AUTHENTICATION_FAILED || code == ErrorCodes::REQUIRED_PASSWORD)
    {
        status = Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED;
        type = "NotAuthorizedException";
        message = getCurrentExceptionMessage(false);
    }
    else if (code == ErrorCodes::ACCESS_DENIED || code == ErrorCodes::READONLY || code == ErrorCodes::QUERY_IS_PROHIBITED)
    {
        status = Poco::Net::HTTPResponse::HTTP_FORBIDDEN;
        type = "ForbiddenException";
        message = getCurrentExceptionMessage(false);
    }
    /// A valid request that this build cannot serve, for example a partition spec on a build without Avro.
    else if (code == ErrorCodes::SUPPORT_IS_DISABLED)
    {
        status = Poco::Net::HTTPResponse::HTTP_NOT_IMPLEMENTED;
        type = "UnsupportedOperationException";
        message = getCurrentExceptionMessage(false);
    }
    /// The store handles expected Keeper errors. Anything else means the store is unavailable.
    else if (code == ErrorCodes::KEEPER_EXCEPTION || code == ErrorCodes::NO_ZOOKEEPER)
    {
        status = Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
        type = "ServiceUnavailableException";
        message = "Catalog storage is unavailable";
    }
    /// The catalog is healthy, the client should retry the request.
    else if (code == ErrorCodes::ICEBERG_CATALOG_CONCURRENT_UPDATE)
    {
        status = Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
        type = "ServiceUnavailableException";
        message = getCurrentExceptionMessage(false);
    }

    tryLogCurrentException(log, "Failed to process Iceberg REST catalog request");
    return makeError(status, type, message);
}

IcebergRESTResponse IcebergRESTCatalogService::handleGetConfig(const Poco::URI & uri) const
{
    auto requested_warehouse = getQueryParameter(uri, "warehouse");
    if (!requested_warehouse)
        return makeError(
            Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "BadRequestException", "This server requires the warehouse query parameter");

    const auto warehouse = warehouses->tryGet(*requested_warehouse);
    if (!warehouse)
    {
        return makeError(
            Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
            "NoSuchWarehouseException",
            fmt::format("Unknown warehouse: {}", *requested_warehouse));
    }

    Poco::JSON::Object defaults;
    Poco::JSON::Object overrides;
    Poco::JSON::Array endpoints;


    overrides.set("prefix", warehouse->name);
    for (const auto & route : getIcebergRESTRoutes())
    {
        if (!route.implemented)
            continue;
        endpoints.add(fmt::format("{} /{}", route.method, fmt::join(route.pattern, "/")));
    }

    Poco::JSON::Object result;
    result.set("defaults", defaults);
    result.set("overrides", overrides);
    result.set("endpoints", endpoints);
    return makeJSON(result, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::handleListNamespaces(const IcebergRESTCatalogWarehouse & warehouse, const Poco::URI & uri) const
{
    IcebergNamespaceName parent;
    if (auto parent_param = getQueryParameter(uri, "parent"); parent_param && !parent_param->empty())
    {
        parent = splitNamespace(*parent_param);
        if (!warehouse.namespaceExists(parent))
            return makeNoSuchNamespace(parent);
    }

    Poco::JSON::Array namespaces;
    for (const auto & name : warehouse.listNamespaces(parent))
        namespaces.add(stringsToJSONArray(name));

    Poco::JSON::Object result;
    result.set("namespaces", namespaces);
    return makeJSON(result, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::handleLoadNamespace(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match) const
{
    const auto ns = splitNamespace(match.path_params.at("namespace"));
    /// The properties double as the existence check.
    const auto properties = warehouse.getNamespaceProperties(ns);
    if (!properties)
        return makeNoSuchNamespace(ns);

    Poco::JSON::Object properties_json;
    for (const auto & [key, value] : *properties)
        properties_json.set(key, value);

    Poco::JSON::Object result;
    result.set("namespace", stringsToJSONArray(ns));
    result.set("properties", properties_json);
    return makeJSON(result, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::handleNamespaceExists(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match) const
{
    const auto ns = splitNamespace(match.path_params.at("namespace"));
    if (!warehouse.namespaceExists(ns))
        return makeNoSuchNamespace(ns);

    return makeNoContent();
}

void IcebergRESTCatalogService::checkNotReadonly(const Context * query_context, const String & action)
{
    /// A call from inside the server has no user. ClickHouse has already checked the query that caused it.
    if (!query_context)
        return;
    if (query_context->getSettingsRef()[Setting::readonly])
        throw Exception(ErrorCodes::READONLY, "Cannot {} in readonly mode", action);
}

void IcebergRESTCatalogService::checkDDLAllowed(const Context * query_context, const String & action)
{
    checkNotReadonly(query_context, action);
    if (query_context && !query_context->getSettingsRef()[Setting::allow_ddl])
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Cannot {}. DDL queries are prohibited for the user", action);
}

IcebergRESTResponse IcebergRESTCatalogService::handleCreateNamespace(const IcebergRESTCatalogWarehouse & warehouse, const String & body, const Context * query_context) const
{
    checkDDLAllowed(query_context, "create namespace");

    IcebergNamespaceName name;
    std::map<String, String> properties;
    try
    {
        Poco::JSON::Parser parser;
        const auto json = parser.parse(body).extract<Poco::JSON::Object::Ptr>();

        name = parseNamespaceArray(*json);

        if (json->has("properties"))
        {
            const auto properties_object = json->getObject("properties");
            if (!properties_object)
                throw Poco::Exception("'properties' must be an object");
            for (const auto & [key, value] : *properties_object)
                properties[key] = value.extract<String>();
        }
    }
    catch (const Poco::Exception & e)
    {
        return makeError(
            Poco::Net::HTTPResponse::HTTP_BAD_REQUEST,
            "BadRequestException",
            fmt::format("Malformed create namespace request: {}", e.displayText()));
    }

    validateNamespaceLocation(warehouse, properties);

    switch (warehouse.createNamespace(name, properties))
    {
        case IcebergRESTCatalogWarehouse::CreateNamespaceResult::Created:
            break;
        case IcebergRESTCatalogWarehouse::CreateNamespaceResult::NamespaceExists:
            return makeError(
                Poco::Net::HTTPResponse::HTTP_CONFLICT,
                "AlreadyExistsException",
                fmt::format("Namespace already exists: {}", joinNamespace(name)));
        case IcebergRESTCatalogWarehouse::CreateNamespaceResult::ParentMissing:
            return makeError(
                Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
                "NotFoundException",
                fmt::format("Parent of namespace {} does not exist", joinNamespace(name)));
    }
    LOG_INFO(log, "Created namespace {}", joinNamespace(name));

    Poco::JSON::Object properties_json;
    for (const auto & [key, value] : properties)
        properties_json.set(key, value);

    Poco::JSON::Object result;
    result.set("namespace", stringsToJSONArray(name));
    result.set("properties", properties_json);
    return makeJSON(result, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::handleUpdateNamespaceProperties(
    const IcebergRESTCatalogWarehouse & warehouse,
    const IcebergRESTRouteMatch & match,
    const String & body,
    const Context * query_context) const
{
    checkDDLAllowed(query_context, "update namespace properties");

    const auto ns = splitNamespace(match.path_params.at("namespace"));

    std::map<String, String> updates;
    std::set<String> removals;
    try
    {
        Poco::JSON::Parser parser;
        const auto json = parser.parse(body).extract<Poco::JSON::Object::Ptr>();

        if (json->has("updates"))
        {
            const auto updates_object = json->getObject("updates");
            if (!updates_object)
                throw Poco::Exception("'updates' must be an object");
            for (const auto & [key, value] : *updates_object)
                updates[key] = value.extract<String>();
        }

        if (json->has("removals"))
        {
            const auto removals_array = json->getArray("removals");
            if (!removals_array)
                throw Poco::Exception("'removals' must be an array");
            for (const auto & key : *removals_array)
                removals.insert(key.extract<String>());
        }
    }
    catch (const Poco::Exception & e)
    {
        return makeError(
            Poco::Net::HTTPResponse::HTTP_BAD_REQUEST,
            "BadRequestException",
            fmt::format("Malformed update namespace properties request: {}", e.displayText()));
    }

    validateNamespaceLocation(warehouse, updates);

    const auto conflict = std::ranges::find_if(removals, [&](const auto & key) { return updates.contains(key); });
    if (conflict != removals.end())
    {
        return makeError(
            Poco::Net::HTTPResponse::HTTP_UNPROCESSABLE_ENTITY,
            "UnprocessableEntityException",
            fmt::format("Property key '{}' is in both 'updates' and 'removals'", *conflict));
    }

    const auto result = warehouse.updateNamespaceProperties(ns, updates, removals);
    if (!result)
        return makeNoSuchNamespace(ns);

    Poco::JSON::Object result_json;
    result_json.set("updated", stringsToJSONArray(result->updated));
    result_json.set("removed", stringsToJSONArray(result->removed));
    result_json.set("missing", stringsToJSONArray(result->missing));
    return makeJSON(result_json, Poco::Net::HTTPResponse::HTTP_OK);
}

Poco::JSON::Object::Ptr IcebergRESTCatalogService::readTableMetadata(
    const IcebergRESTCatalogWarehouse & warehouse, const IcebergTablePointer & pointer) const
{
    const auto key = warehouse.objectKey(pointer.metadata_location);
    const auto content = readObjectToString(*warehouse.object_storage, key, getContext()->getReadSettings(), getMetadataFileCompressionMethod(key));
    return parseJSONObject(content, fmt::format("Metadata file {}", pointer.metadata_location));
}

IcebergRESTResponse IcebergRESTCatalogService::makeLoadTableResult(
    const String & metadata_location, const Poco::JSON::Object::Ptr & metadata)
{
    Poco::JSON::Object result;
    result.set("metadata-location", metadata_location);
    result.set("metadata", metadata);
    /// TODO: Add credential vending yet. For now, clients use their own storage credentials.
    result.set("config", Poco::JSON::Object());
    return makeJSON(result, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::handleListTables(
    const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match) const
{
    const auto ns = splitNamespace(match.path_params.at("namespace"));
    if (!warehouse.namespaceExists(ns))
        return makeNoSuchNamespace(ns);

    /// TODO: `pageToken` and `pageSize` are ignored: the whole list is returned at once.
    Poco::JSON::Array identifiers;
    if (const auto tables = warehouse.listTables(ns))
    {
        for (const auto & table : *tables)
            identifiers.add(tableIdentifierToJSON(ns, table));
    }

    Poco::JSON::Object result;
    result.set("identifiers", identifiers);
    return makeJSON(result, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::handleTableExists(
    const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match) const
{
    const auto ns = splitNamespace(match.path_params.at("namespace"));
    if (!warehouse.namespaceExists(ns))
        return makeNoSuchNamespace(ns);

    const auto & table = match.path_params.at("table");
    if (!warehouse.tableExists(ns, table))
        return makeNoSuchTable(ns, table);

    return makeNoContent();
}

IcebergRESTResponse IcebergRESTCatalogService::handleLoadTable(
    const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match) const
{
    const auto ns = splitNamespace(match.path_params.at("namespace"));
    if (!warehouse.namespaceExists(ns))
        return makeNoSuchNamespace(ns);

    const auto & table = match.path_params.at("table");
    const auto pointer = warehouse.getTable(ns, table);
    if (!pointer)
        return makeNoSuchTable(ns, table);

    return makeLoadTableResult(pointer->metadata_location, readTableMetadata(warehouse, *pointer));
}

IcebergRESTResponse IcebergRESTCatalogService::handleCreateTable(
    const IcebergRESTCatalogWarehouse & warehouse,
    const IcebergRESTRouteMatch & match,
    const String & body,
    const Context * query_context) const
{
    checkDDLAllowed(query_context, "create table");

    /// The properties double as the existence check.
    const auto ns = splitNamespace(match.path_params.at("namespace"));
    const auto namespace_properties = warehouse.getNamespaceProperties(ns);
    if (!namespace_properties)
        return makeNoSuchNamespace(ns);

    const auto uuid = toString(UUIDHelpers::generateV4());

    String name;
    IcebergTablePointer pointer{.uuid = uuid, .metadata_location = {}};
    CompressionMethod compression_method = CompressionMethod::None;
    String location;
    String object_key;
    Poco::JSON::Object::Ptr metadata;
    try
    {
        const auto json = Poco::JSON::Parser().parse(body).extract<Poco::JSON::Object::Ptr>();

        if (!json->has("name") || !json->get("name").isString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'name' must be a string");
        name = json->getValue<String>("name");
        /// The store checks the name too, but only after the metadata file is written. Reject it before that.
        validateTableName(name);

        /// TODO: Stage-create means "write the metadata, do not register it". Needed for CTAS.
        if (json->optValue<bool>("stage-create", false))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "stage-create is not supported");

        /// The catalog owns every table path, so two tables can never share a directory.
        /// TODO: Allow the ClickHouse client to send no location, so that the catalog can assign it.
        /// Currently the ClickHouse client always sends `location`. This needs a refactor of the client:
        /// create through the catalog first, then load the table from the answer.
        /// TODO: External tables need a per-table no-purge flag and an atomic location guard in Keeper.
        if (json->has("location") && !json->isNull("location"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'location' is not supported, the catalog chooses the table location");

        if (const auto it = namespace_properties->find("location"); it != namespace_properties->end())
        {
            /// The uuid suffix keeps a dropped and recreated table away from the old files.
            location = fmt::format("{}/{}-{}", stripTrailingSlashes(it->second), name, uuid);
        }
        else
        {
            location = fmt::format("{}/{}/{}-{}", warehouse.base_location, fmt::join(ns, "/"), name, uuid);
        }

        std::map<String, String> properties;
        if (const auto properties_object = getOptionalObject(*json, "properties"))
        {
            for (const auto & [key, value] : *properties_object)
                properties[key] = value.convert<String>();
        }

        metadata = buildInitialTableMetadata(
            uuid,
            location,
            getOptionalObject(*json, "schema"),
            getOptionalObject(*json, "partition-spec"),
            getOptionalObject(*json, "write-order"),
            std::move(properties));
        compression_method = getMetadataCompressionMethod(*metadata);

        /// Same naming as the ClickHouse Iceberg writer: `<location>/metadata/v<version>-<uuid>.metadata.json`, starting at 1.
        pointer.metadata_location
            = fmt::format("{}/metadata/v1-{}{}.metadata.json", location, uuid, getMetadataCompressionSuffix(compression_method));
        object_key = warehouse.objectKey(pointer.metadata_location);
    }
    /// `Exception` derives from `Poco::Exception`, so it goes first.
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::BAD_ARGUMENTS)
            throw;
        return makeError(
            Poco::Net::HTTPResponse::HTTP_BAD_REQUEST,
            "BadRequestException",
            fmt::format("Malformed create table request: {}", e.message()));
    }
    catch (const Poco::Exception & e)
    {
        return makeError(
            Poco::Net::HTTPResponse::HTTP_BAD_REQUEST,
            "BadRequestException",
            fmt::format("Malformed create table request: {}", e.displayText()));
    }

    /// Object storage first, then the Keeper pointer. A crash in between leaves an orphan file, not a dangling pointer.
    writeNewObject(
        *warehouse.object_storage,
        object_key,
        toJSONString(*metadata, 4),
        getContext()->getWriteSettings(),
        compression_method,
        static_cast<int>(getContext()->getSettingsRef()[Setting::output_format_compression_level]));

    /// A Keeper exception (timeout, session loss) leaves the file. The node may exist, so deleting could leave a dangling pointer.
    using CreateTableResult = IcebergRESTCatalogWarehouse::CreateTableResult;
    const auto created = warehouse.createTable(ns, name, pointer);
    if (created != CreateTableResult::Created)
    {
        LOG_INFO(log, "Removing metadata file {} because table {}.{} was not registered", object_key, joinNamespace(ns), name);
        warehouse.object_storage->removeObjectIfExists(StoredObject(object_key));
    }

    if (created == CreateTableResult::TableExists)
    {
        return makeError(
            Poco::Net::HTTPResponse::HTTP_CONFLICT,
            "TableAlreadyExistsException",
            fmt::format("Table already exists: {}.{}", joinNamespace(ns), name));
    }
    if (created == CreateTableResult::NamespaceMissing)
        return makeNoSuchNamespace(ns);

    LOG_INFO(log, "Created table {}.{} at {}", joinNamespace(ns), name, pointer.metadata_location);
    /// Answer from memory. The table is registered, so a failed read-back must not turn into a 500.
    return makeLoadTableResult(pointer.metadata_location, metadata);
}

/// Checks requirements and applies updates in memory, then calls `commitTableMetadata`.
IcebergRESTResponse IcebergRESTCatalogService::handleUpdateTable(
    const IcebergRESTCatalogWarehouse & warehouse,
    const IcebergRESTRouteMatch & match,
    const String & body,
    const Context * query_context) const
{
    checkNotReadonly(query_context, "commit to table");

    const auto ns = splitNamespace(match.path_params.at("namespace"));
    if (!warehouse.namespaceExists(ns))
        return makeNoSuchNamespace(ns);
    const auto & table = match.path_params.at("table");

    const auto pointer = warehouse.getTable(ns, table);
    if (!pointer)
        return makeNoSuchTable(ns, table);
    auto metadata = readTableMetadata(warehouse, *pointer);

    CompressionMethod compression_method = CompressionMethod::None;
    try
    {
        const auto json = Poco::JSON::Parser().parse(body).extract<Poco::JSON::Object::Ptr>();
        const auto requirements = getArray(*json, "requirements");
        const auto updates = getArray(*json, "updates");

        if (const auto result = checkTableRequirements(*metadata, *requirements); !result)
            return makeError(Poco::Net::HTTPResponse::HTTP_CONFLICT, "CommitFailedException", result.error());

        /// Do not write a new metadata version for an empty commit.
        if (updates->size() == 0)
            return makeCommitTableResult(pointer->metadata_location, metadata);

        applyTableUpdatesToJSON(*metadata, *updates, pointer->metadata_location);
        /// A `set-properties` update may change the codec. The new file follows the updated properties.
        compression_method = getMetadataCompressionMethod(*metadata);
    }
    catch (const Exception & e)
    {
        /// The request was based on metadata that another commit has since replaced. The client retries after reloading.
        if (e.code() == ErrorCodes::METADATA_MISMATCH)
            return makeError(Poco::Net::HTTPResponse::HTTP_CONFLICT, "CommitFailedException", e.message());
        /// Other errors are not caused by the client. `execute` reports them as internal errors.
        if (e.code() != ErrorCodes::NOT_IMPLEMENTED && e.code() != ErrorCodes::BAD_ARGUMENTS)
            throw;
        return makeCommitRequestError(e.code(), e.message());
    }
    catch (const Poco::Exception & e)
    {
        return makeCommitRequestError(ErrorCodes::BAD_ARGUMENTS, e.displayText());
    }

    return commitTableMetadata(warehouse, ns, table, *pointer, metadata, compression_method);
}

IcebergRESTResponse IcebergRESTCatalogService::commitTableMetadata(
    const IcebergRESTCatalogWarehouse & warehouse,
    const IcebergNamespaceName & ns,
    const String & table,
    const IcebergTablePointer & pointer,
    const Poco::JSON::Object::Ptr & metadata,
    CompressionMethod compression_method) const
{
    /// Get the name for the next metadata.json file.
    const String new_location
        = nextMetadataLocation(*metadata, pointer.metadata_location, getMetadataCompressionSuffix(compression_method));

    /// Write the metadata.json to object storage.
    const auto object_key = warehouse.objectKey(new_location);
    writeNewObject(
        *warehouse.object_storage,
        object_key,
        toJSONString(*metadata, 4),
        getContext()->getWriteSettings(),
        compression_method,
        static_cast<int>(getContext()->getSettingsRef()[Setting::output_format_compression_level]));

    /// Update Keeper.
    using UpdateTableResult = IcebergRESTCatalogWarehouse::UpdateTableResult;
    std::optional<UpdateTableResult> result;
    try
    {
        result = warehouse.updateTable(ns, table, pointer, {.uuid = pointer.uuid, .metadata_location = new_location});
    }
    catch (const zkutil::KeeperException & e)
    {
        /// The set may have been applied, so the new file must stay. The client reloads the table to find out.
        LOG_WARNING(log, "Commit of {}.{} to {} has an unknown outcome: {}", joinNamespace(ns), table, new_location, e.message());
        return makeError(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, "CommitStateUnknownException", "Commit state is unknown");
    }

    if (*result != UpdateTableResult::Updated)
        warehouse.object_storage->removeObjectIfExists(StoredObject(object_key));

    switch (*result)
    {
        case UpdateTableResult::Updated:
            LOG_INFO(log, "Committed table {}.{}: {} -> {}", joinNamespace(ns), table, pointer.metadata_location, new_location);
            return makeCommitTableResult(new_location, metadata);
        case UpdateTableResult::VersionMismatch:
            return makeError(
                Poco::Net::HTTPResponse::HTTP_CONFLICT,
                "CommitFailedException",
                fmt::format("Table {}.{} was updated concurrently, retry the commit", joinNamespace(ns), table));
        case UpdateTableResult::UuidMismatch:
            return makeError(
                Poco::Net::HTTPResponse::HTTP_CONFLICT,
                "CommitFailedException",
                fmt::format("Table {}.{} was dropped and re-created, reload the table", joinNamespace(ns), table));
        case UpdateTableResult::TableMissing:
            return makeNoSuchTable(ns, table);
    }
}

IcebergRESTResponse IcebergRESTCatalogService::makeCommitTableResult(
    const String & metadata_location, const Poco::JSON::Object::Ptr & metadata)
{
    Poco::JSON::Object result;
    result.set("metadata-location", metadata_location);
    result.set("metadata", metadata);
    return makeJSON(result, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::makeCommitRequestError(int code, const String & message)
{
    if (code == ErrorCodes::NOT_IMPLEMENTED)
        return makeError(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "UnsupportedOperationException", message);
    return makeError(
        Poco::Net::HTTPResponse::HTTP_BAD_REQUEST,
        "BadRequestException",
        fmt::format("Malformed commit request: {}", message));
}

IcebergRESTResponse IcebergRESTCatalogService::handleRenameTable(
    const IcebergRESTCatalogWarehouse & warehouse,
    const String & body,
    const Context * query_context) const
{
    checkDDLAllowed(query_context, "rename table");

    TableIdentifier source;
    TableIdentifier dest;
    try
    {
        const auto json = Poco::JSON::Parser().parse(body).extract<Poco::JSON::Object::Ptr>();
        source = parseTableIdentifier(json->getObject("source"));
        dest = parseTableIdentifier(json->getObject("destination"));
    }
    catch (const Poco::Exception & e)
    {
        return makeError(
            Poco::Net::HTTPResponse::HTTP_BAD_REQUEST,
            "BadRequestException",
            fmt::format("Malformed rename table request: {}", e.displayText()));
    }

    if (source.ns == dest.ns && source.name == dest.name)
        return makeError(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "BadRequestException", "Source and destination are the same table");

    switch (warehouse.renameTable(source.ns, source.name, dest.ns, dest.name))
    {
        case IcebergRESTCatalogWarehouse::RenameTableResult::Renamed:
            LOG_INFO(log, "Renamed table {}.{} to {}.{}", joinNamespace(source.ns), source.name, joinNamespace(dest.ns), dest.name);
            return makeNoContent();
        case IcebergRESTCatalogWarehouse::RenameTableResult::TableMissing:
            return makeNoSuchTable(source.ns, source.name);
        case IcebergRESTCatalogWarehouse::RenameTableResult::NamespaceMissing:
            return makeNoSuchNamespace(dest.ns);
        case IcebergRESTCatalogWarehouse::RenameTableResult::TargetExists:
            return makeError(
                Poco::Net::HTTPResponse::HTTP_CONFLICT,
                "AlreadyExistsException",
                fmt::format("Table already exists: {}.{}", joinNamespace(dest.ns), dest.name));
    }
}

/// Clients send scan and commit reports after each operation. The server has nothing to do with them yet.
IcebergRESTResponse IcebergRESTCatalogService::handleReportMetrics(
    const IcebergRESTCatalogWarehouse & warehouse,
    const IcebergRESTRouteMatch & match) const
{
    const auto ns = splitNamespace(match.path_params.at("namespace"));
    if (!warehouse.namespaceExists(ns))
        return makeNoSuchNamespace(ns);

    const auto & table = match.path_params.at("table");
    if (!warehouse.tableExists(ns, table))
        return makeNoSuchTable(ns, table);

    return makeNoContent();
}

IcebergRESTResponse IcebergRESTCatalogService::handleDropTable(
    const IcebergRESTCatalogWarehouse & warehouse,
    const IcebergRESTRouteMatch & match,
    const Poco::URI & uri,
    const Context * query_context) const
{
    checkDDLAllowed(query_context, "drop table");

    const auto ns = splitNamespace(match.path_params.at("namespace"));
    if (!warehouse.namespaceExists(ns))
        return makeNoSuchNamespace(ns);

    const auto & table = match.path_params.at("table");

    /// TODO: purge deletes every file the table owns: data files, manifests, manifest lists and metadata files.
    if (getBooleanQueryParameter(uri, "purgeRequested", /*default_value=*/ false))
        return makeError(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "BadRequestException", "purgeRequested is not supported yet");

    if (!warehouse.dropTable(ns, table))
        return makeNoSuchTable(ns, table);

    LOG_INFO(log, "Dropped table {}.{}", joinNamespace(ns), table);
    return makeNoContent();
}

}
