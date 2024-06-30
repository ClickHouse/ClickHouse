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

const String CURSOR = "cursor";
const String KEEPER_KEY = "keeper_key";

Poco::JSON::Object cursorDataToJSON(const CursorData & data)
{
    Poco::JSON::Object json;

    if (data.keeper_key.has_value())
        json.set(KEEPER_KEY, data.keeper_key.value());

    json.set(CURSOR, cursorTreeToJSON(data.tree));

    return json;
}

CursorData parseDataFromJSON(const Poco::Dynamic::Var & var)
{
    const auto & json = var.extract<Poco::JSON::Object::Ptr>();

    CursorData data;
    data.tree = buildCursorTree(json->getObject(CURSOR));

    if (json->has(KEEPER_KEY))
        data.keeper_key = json->getValue<String>(KEEPER_KEY);

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
        data_map[table] = parseDataFromJSON(serialized_data);
}

void writeBinary(const CursorDataMap & data_map, WriteBuffer & buf)
{
    Poco::JSON::Object json;

    for (const auto & [table, data] : data_map)
        json.set(table, cursorDataToJSON(data));

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    json.stringify(oss);

    String str_repr = oss.str();
    writeBinary(str_repr, buf);
}

}
