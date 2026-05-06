#include <Storages/MergeTree/SelectiveReplication/Constants.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Poco/JSON/Parser.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace DB::SelectiveReplication
{

UInt64 parseSelectiveConfig(const String & json_str)
{
    if (json_str.empty())
        return 0;
    try
    {
        Poco::JSON::Parser parser;
        auto json = parser.parse(json_str).extract<Poco::JSON::Object::Ptr>();
        if (json && json->has("replication_factor"))
            return json->getValue<UInt64>("replication_factor");
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Failed to parse selective replication config from Keeper: {}. Raw data: {}",
            e.displayText(), json_str.substr(0, std::min(json_str.size(), size_t(100))));
    }
    return 0;
}

String serializeSelectiveConfig(UInt64 replication_factor)
{
    return "{\"replication_factor\":" + toString(replication_factor) + "}";
}

}
