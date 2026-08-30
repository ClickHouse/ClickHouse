#include <Databases/DataLake/IcebergCatalog/Models/IcebergRestCatalogConfig.h>

#if USE_AVRO

#include <Common/Exception.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <sstream>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DataLake::IcebergRestModels
{

void CatalogConfigSettings::mergeFrom(const CatalogConfigSettings & overrides)
{
    if (!overrides.prefix.empty())
        prefix = overrides.prefix;
    if (!overrides.default_base_location.empty())
        default_base_location = overrides.default_base_location;
}

CatalogConfigSettings CatalogConfigResponse::merged() const
{
    CatalogConfigSettings result = defaults;
    result.mergeFrom(overrides);
    return result;
}

void applyCatalogConfigSettings(const Poco::JSON::Object::Ptr & object, CatalogConfigSettings & result)
{
    if (!object)
        return;

    if (object->has("prefix"))
        result.prefix = object->get("prefix").extract<std::string>();

    if (object->has("default-base-location"))
        result.default_base_location = object->get("default-base-location").extract<std::string>();
}

CatalogConfigResponse parseCatalogConfigResponse(const std::string & json)
{
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var parsed = parser.parse(json);
    const auto & object = parsed.extract<Poco::JSON::Object::Ptr>();

    CatalogConfigResponse response;
    applyCatalogConfigSettings(object->get("defaults").extract<Poco::JSON::Object::Ptr>(), response.defaults);
    applyCatalogConfigSettings(object->get("overrides").extract<Poco::JSON::Object::Ptr>(), response.overrides);
    return response;
}

std::string serializeCatalogConfigResponse(const CatalogConfigResponse & response)
{
    auto make_settings_object = [](const CatalogConfigSettings & settings)
    {
        Poco::JSON::Object::Ptr object = new Poco::JSON::Object;
        if (!settings.prefix.empty())
            object->set("prefix", settings.prefix.string());
        if (!settings.default_base_location.empty())
            object->set("default-base-location", settings.default_base_location);
        return object;
    };

    Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
    root->set("defaults", make_settings_object(response.defaults));
    root->set("overrides", make_settings_object(response.overrides));

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(root, oss);
    return oss.str();
}

}

#endif
