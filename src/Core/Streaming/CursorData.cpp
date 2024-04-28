#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Core/Streaming/CursorTree.h>
#include <Core/Streaming/CursorData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace
{

const String kCursor = "cursor";
const String kKeeperKey = "keeper_key";

Poco::JSON::Object cursorDataToJson(const CursorData & data)
{
    Poco::JSON::Object json;

    if (data.keeper_key.has_value())
        json.set(kKeeperKey, data.keeper_key.value());

    json.set(kCursor, cursorTreeToJson(data.tree));

    return json;
}

CursorData parseDataFromJson(const Poco::Dynamic::Var & var)
{
    const auto & json = var.extract<Poco::JSON::Object::Ptr>();

    CursorData data {
        .tree = buildCursorTree(json->getObject(kCursor)),
    };

    if (json->has(kKeeperKey))
        data.keeper_key = json->getValue<String>(kKeeperKey);

    return data;
}

}

void readBinary(CursorDataMap & data_map, ReadBuffer & buf)
{
    String str_repr;
    readBinary(str_repr, buf);

    Poco::JSON::Parser parser;
    auto json = parser.parse(str_repr).extract<Poco::JSON::Object::Ptr>();

    for (const auto & [table, serialized_data] : *json)
        data_map[table] = parseDataFromJson(serialized_data);
}

void writeBinary(const CursorDataMap & data_map, WriteBuffer & buf)
{
    Poco::JSON::Object json;

    for (const auto & [table, data] : data_map)
        json.set(table, cursorDataToJson(data));

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    json.stringify(oss);

    String str_repr = oss.str();
    writeBinary(str_repr, buf);
}

}
