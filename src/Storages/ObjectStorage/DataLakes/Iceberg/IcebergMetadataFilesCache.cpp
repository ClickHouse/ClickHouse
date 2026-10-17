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

Poco::JSON::Object::Ptr parseMetadataJSON(const String & body)
{
    ProfileEvents::increment(ProfileEvents::IcebergMetadataJSONParses);
    ProfileEvents::increment(ProfileEvents::IcebergMetadataJSONParsedBytes, body.size());
    Poco::JSON::Parser parser;
    return parser.parse(body).extract<Poco::JSON::Object::Ptr>();
}

}

MetadataJSON::MetadataJSON(String body_)
    : body(std::move(body_))
    , parsed(parseMetadataJSON(body))
{
}

std::shared_ptr<const Poco::JSON::Object> MetadataJSON::getConstMetadataJSON() const
{
    return std::shared_ptr<const Poco::JSON::Object>(std::make_shared<Poco::JSON::Object::Ptr>(parsed), parsed.get());
}

Poco::JSON::Object::Ptr MetadataJSON::getMutableMetadataJSON() const
{
    return parseMetadataJSON(body);
}

}

#endif
