#pragma once
#include "config.h"

#if USE_AVRO

#include <filesystem>
#include <string>

namespace Poco
{
namespace JSON
{
    class Object;
}
}

namespace DataLake::IcebergRestModels
{

struct CatalogConfigSettings
{
    std::filesystem::path prefix;
    std::string default_base_location;

    void mergeFrom(const CatalogConfigSettings & overrides);
};

struct CatalogConfigResponse
{
    CatalogConfigSettings defaults;
    CatalogConfigSettings overrides;

    CatalogConfigSettings merged() const;
};

CatalogConfigResponse parseCatalogConfigResponse(const std::string & json);
void applyCatalogConfigSettings(const Poco::JSON::Object::Ptr & object, CatalogConfigSettings & result);

std::string serializeCatalogConfigResponse(const CatalogConfigResponse & response);

}

#endif
