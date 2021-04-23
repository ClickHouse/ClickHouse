#pragma once

#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

template <typename Element>
String getValueAsString(const Element & element)
{
    if (element.isBool())   return toString(element.getBool());
    if (element.isInt64())  return toString(element.getInt64());
    if (element.isUInt64()) return toString(element.getUInt64());
    if (element.isDouble()) return toString(element.getDouble());
    if (element.isString()) return String(element.getString());

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported type of JSON field");
}

}

struct ParseResult
{
    Strings paths;
    Strings values;
};

template <typename ParserImpl>
class JSONDataParser
{
public:
    using Element = typename ParserImpl::Element;
    using Path = std::vector<std::string_view>;
    using Paths = std::vector<Path>;

    void readInto(String & s, ReadBuffer & buf)
    {
        readJSONObjectPossiblyInvalid(s, buf);
    }

    std::optional<ParseResult> parse(const char * begin, size_t length)
    {
        std::string_view json{begin, length};
        Element document;
        if (!parser.parse(json, document))
            return {};

        ParseResult result;
        traverse(document, "", result);
        return result;
    }

private:
    void traverse(const Element & element, String current_path, ParseResult & result)
    {
        /// TODO: support arrays.
        if (element.isObject())
        {
            auto object = element.getObject();
            for (auto it = object.begin(); it != object.end(); ++it)
            {
                const auto & [key, value] = *it;
                String next_path = current_path;
                if (!next_path.empty())
                    next_path += ".";
                next_path += key;
                traverse(value, next_path, result);
            }
        }
        else
        {
            result.paths.push_back(current_path);
            result.values.push_back(getValueAsString(element));
        }
    }

    ParserImpl parser;
};

}
