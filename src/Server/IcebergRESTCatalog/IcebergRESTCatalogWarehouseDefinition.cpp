#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseDefinition.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
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

/// Bump on any layout change so data of another layout is refused, not misread.
constexpr Int64 FORMAT_VERSION = 2;

constexpr size_t MAX_WAREHOUSE_NAME_LENGTH = 64;
constexpr auto HIDDEN = "[HIDDEN]";

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

Poco::JSON::Object::Ptr IcebergRESTCatalogWarehouseCredential::toJSON(bool hide_secrets) const
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object;
    json->set("type", TYPE_AWS_ROLE);
    json->set("role_arn", role_arn);
    if (!external_id.empty())
        json->set("external_id", hide_secrets ? HIDDEN : external_id);
    if (!role_session_name.empty())
        json->set("role_session_name", hide_secrets ? HIDDEN : role_session_name);
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

Poco::JSON::Object::Ptr IcebergRESTCatalogWarehouseDefinition::toJSON(const String & name) const
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object;
    json->set("name", name);
    json->set("base_location", base_location);
    json->set("credential", credential.toJSON(/*hide_secrets*/ true));
    return json;
}

String IcebergRESTCatalogWarehouseDefinition::toKeeperData() const
{
    Poco::JSON::Object json;
    json.set("format_version", FORMAT_VERSION);
    json.set("base_location", base_location);
    json.set("credential", credential.toJSON(/*hide_secrets*/ false));
    return toJSONString(json);
}

IcebergRESTCatalogWarehouseDefinition IcebergRESTCatalogWarehouseDefinition::fromKeeperData(const String & data, const String & path)
{
    const auto what = fmt::format("Warehouse definition at {}", path);
    const auto json = parseJSONObject(data, what);

    const auto format_version = json->optValue<Int64>("format_version", 0);
    if (format_version != FORMAT_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA, "{} has an unsupported format version: expected {}, found {}", what, FORMAT_VERSION, format_version);

    return fromJSON(*json);
}

void validateIcebergRESTCatalogWarehouseName(const String & name)
{
    if (name.empty() || name.size() > MAX_WAREHOUSE_NAME_LENGTH)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Warehouse name must be 1 to {} bytes long", MAX_WAREHOUSE_NAME_LENGTH);
    for (const char c : name)
    {
        if (!isAlphaNumericASCII(c) && c != '_' && c != '-')
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Warehouse name '{}' must contain only letters, digits, '_' and '-'", name);
    }
}

}
