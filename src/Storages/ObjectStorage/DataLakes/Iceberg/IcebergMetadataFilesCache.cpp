#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Common/ProfileEvents.h>
#include <Poco/JSON/Parser.h>

namespace ProfileEvents
{
    extern const Event IcebergMetadataJSONParses;
    extern const Event IcebergMetadataJSONParsedBytes;
}

namespace DB::Iceberg
{

namespace
{

Poco::JSON::Object::Ptr parseMetadataJSON(const String & json_body)
{
    ProfileEvents::increment(ProfileEvents::IcebergMetadataJSONParses);
    ProfileEvents::increment(ProfileEvents::IcebergMetadataJSONParsedBytes, json_body.size());
    Poco::JSON::Parser parser;
    return parser.parse(json_body).extract<Poco::JSON::Object::Ptr>();
}

}

MetadataObject::MetadataObject(String json_body_)
    : json_body(std::move(json_body_))
    , parsed(parseMetadataJSON(json_body))
{
}

Poco::JSON::Object::Ptr MetadataObject::getParsedCopy() const
{
    return parseMetadataJSON(json_body);
}

}

#endif
