#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseDefinition.h>

#include <Common/Exception.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouse.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

namespace
{

/// Bump on any layout change so old data is refused, not misread.
constexpr auto FORMAT = "IcebergRESTCatalog\nformat_version: 2";

String getRequiredString(const Poco::JSON::Object & json, const String & key)
{
    IcebergRESTCatalogJSON::validateString(json, key);
    return json.getValue<String>(key);
}

String getOptionalString(const Poco::JSON::Object & json, const String & key)
{
    if (!json.has(key))
        return "";
    return getRequiredString(json, key);
}

}

IcebergRESTCatalogWarehouseCredential IcebergRESTCatalogWarehouseCredential::fromJSON(const Poco::JSON::Object & json)
{
    const auto type = getRequiredString(json, "type");
    if (type != TYPE_AWS_ROLE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported credential type '{}'. Supported types: {}", type, TYPE_AWS_ROLE);

    IcebergRESTCatalogWarehouseCredential credential;
    credential.role_arn = getRequiredString(json, "role_arn");
    credential.external_id = getOptionalString(json, "external_id");
    credential.role_session_name = getOptionalString(json, "role_session_name");
    return credential;
}

Poco::JSON::Object::Ptr IcebergRESTCatalogWarehouseCredential::toJSON() const
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    json->set("type", TYPE_AWS_ROLE);
    json->set("role_arn", role_arn);
    if (!external_id.empty())
        json->set("external_id", external_id);
    if (!role_session_name.empty())
        json->set("role_session_name", role_session_name);
    return json;
}

IcebergRESTCatalogWarehouseDefinition IcebergRESTCatalogWarehouseDefinition::fromJSON(const Poco::JSON::Object & json)
{
    IcebergRESTCatalogWarehouseDefinition definition;
    definition.base_location = stripTrailingSlashes(getRequiredString(json, "base_location"));
    if (!definition.base_location.starts_with("s3://"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'base_location' must be an s3:// URI");

    const auto credential = json.getObject("credential");
    if (!credential)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'credential' must be an object");
    definition.credential = IcebergRESTCatalogWarehouseCredential::fromJSON(*credential);
    return definition;
}

String IcebergRESTCatalogWarehouseDefinition::toKeeperData() const
{
    Poco::JSON::Object json(Poco::JSON_PRESERVE_KEY_ORDER);
    json.set("format", FORMAT);
    json.set("base_location", base_location);
    json.set("credential", credential.toJSON());
    return toJSONString(json);
}

IcebergRESTCatalogWarehouseDefinition IcebergRESTCatalogWarehouseDefinition::fromKeeperData(const String & data, const String & path)
{
    const auto what = fmt::format("Warehouse definition at {}", path);
    const auto json = parseJSONObject(data, what);

    const auto format = json->optValue<String>("format", "");
    if (format != FORMAT)
        throw Exception(ErrorCodes::INCORRECT_DATA, "{} has an unsupported format: expected {}, found {}", what, FORMAT, format);

    return fromJSON(*json);
}

}
