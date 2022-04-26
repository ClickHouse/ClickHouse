#pragma once

#include <IO/ReadHelpers.h>
#include <Common/HashTable/HashMap.h>
#include <DataTypes/Serializations/PathInData.h>

namespace DB
{

class ReadBuffer;

template <typename ParserImpl>
class JSONDataParser
{
public:
    static void readJSON(String & s, ReadBuffer & buf)
    {
        readJSONObjectPossiblyInvalid(s, buf);
    }

    std::optional<ParseResult> parse(const char * begin, size_t length);

private:
    using Element = typename ParserImpl::Element;
    using JSONObject = typename ParserImpl::Object;
    using JSONArray = typename ParserImpl::Array;

    struct ParseContext
    {
        PathInDataBuilder builder;
        std::vector<PathInData::Parts> paths;
        std::vector<Field> values;
    };

    using PathPartsWithArray = std::pair<PathInData::Parts, Array>;
    using PathToArray = HashMapWithStackMemory<UInt128, PathPartsWithArray, UInt128TrivialHash, 5>;
    using KeyToSizes = HashMapWithStackMemory<StringRef, std::vector<size_t>, StringRefHash, 5>;

    struct ParseArrayContext
    {
        size_t current_size = 0;
        size_t total_size = 0;

        PathToArray arrays_by_path;
        KeyToSizes nested_sizes_by_key;
        Arena strings_pool;
    };

    void traverse(const Element & element, ParseContext & ctx);
    void traverseObject(const JSONObject & object, ParseContext & ctx);
    void traverseArray(const JSONArray & array, ParseContext & ctx);
    void traverseArrayElement(const Element & element, ParseArrayContext & ctx);

    static void fillMissedValuesInArrays(ParseArrayContext & ctx);
    static bool tryInsertDefaultFromNested(
        ParseArrayContext & ctx, const PathInData::Parts & path, Array & array);

    static Field getValueAsField(const Element & element);
    static StringRef getNameOfNested(const PathInData::Parts & path, const Field & value);

    ParserImpl parser;
};

}
