#include "PartMetadataJSON.h"

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int FORMAT_VERSION_TOO_OLD;
}

PartMetadataJSON::PartMetadataJSON(Poco::JSON::Object::Ptr json_): json(json_) {}

void PartMetadataJSON::readJSON(ReadBuffer & in)
{
    String json_str;
    readString(json_str, in);

    Poco::JSON::Parser parser;
    json = parser.parse(json_str).extract<Poco::JSON::Object::Ptr>();
}

void PartMetadataJSON::writeJSON(WriteBuffer & out) const
{
    if (!json)
        throw Exception(ErrorCodes::FORMAT_VERSION_TOO_OLD, "Tried to write metadata.json but it is not set in memory, logic bug");
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    writeString(oss.str(), out);
}

PartMetadataFormatVersion PartMetadataJSON::metadataFormatVersion() const
{
    if (!json)
        return PART_METADATA_FORMAT_VERSION_OLD;
    auto version = json->getValue<UInt32>("version");
    if (version > PART_METADATA_MAX_FORMAT_VERSION)
        // The part was actually read with an unknown value for metadata version
        // I don't think we can just assume a lower value, since future version of clickhouse might fundamentally shake things up
        // So instead, we must throw exception, presumably this part will get detached :(
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown part metadata format version {}. Maximum supported version is {}", version, PART_METADATA_MAX_FORMAT_VERSION);
    return PartMetadataFormatVersion{version};
}

time_t PartMetadataJSON::creationTime() const
{
    if (metadataFormatVersion() < PART_METADATA_FORMAT_VERSION_INITIAL)
        throw Exception(ErrorCodes::FORMAT_VERSION_TOO_OLD, "Part was written with format version {} which does not have creation time. Requires at least format version {}", metadataFormatVersion(), PART_METADATA_FORMAT_VERSION_INITIAL);
    auto timestamp = json->getValue<UInt64>("creation_time");
    return time_t(timestamp);
}

time_t PartMetadataJSON::creationTime(time_t fallback) const
{
    if (metadataFormatVersion() < PART_METADATA_FORMAT_VERSION_INITIAL)
        return fallback;
    auto timestamp = json->getValue<UInt64>("creation_time");
    return time_t(timestamp);
}

}
