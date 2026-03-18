#include "PinnedPartUUIDs.h"
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

String PinnedPartUUIDs::toString() const
{
    std::vector<UUID> vec(part_uuids.begin(), part_uuids.end());

    Poco::JSON::Object json;
    json.set(JSON_KEY_UUIDS, DB::toString(vec));

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    json.stringify(oss);

    return oss.str();
}

void PinnedPartUUIDs::fromString(const String & buf)
{
    Poco::JSON::Parser parser;
    auto json = parser.parse(buf).extract<Poco::JSON::Object::Ptr>();

    std::vector<UUID> vec = parseFromString<std::vector<UUID>>(json->getValue<std::string>(PinnedPartUUIDs::JSON_KEY_UUIDS));

    part_uuids.clear();
    std::copy(vec.begin(), vec.end(), std::inserter(part_uuids, part_uuids.begin()));
}

}
