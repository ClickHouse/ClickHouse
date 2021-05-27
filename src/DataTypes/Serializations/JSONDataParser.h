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
Field getValueAsField(const Element & element)
{
    if (element.isBool())   return element.getBool();
    if (element.isInt64())  return element.getInt64();
    if (element.isUInt64()) return element.getUInt64();
    if (element.isDouble()) return element.getDouble();
    if (element.isString()) return element.getString();
    if (element.isNull())   return Field();

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported type of JSON field");
}

String getNextPath(const String & current_path, const std::string_view & key)
{
    String next_path = current_path;
    if (!key.empty())
    {
        if (!next_path.empty())
            next_path += ".";
        next_path += key;
    }

    return next_path;
}

}

struct ParseResult
{
    Strings paths;
    std::vector<Field> values;
};

template <typename ParserImpl>
class JSONDataParser
{
public:
    using Element = typename ParserImpl::Element;

    void readJSON(String & s, ReadBuffer & buf)
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
        if (element.isObject())
        {
            auto object = element.getObject();
            result.paths.reserve(result.paths.size() + object.size());
            result.values.reserve(result.values.size() + object.size());

            for (auto it = object.begin(); it != object.end(); ++it)
            {
                const auto & [key, value] = *it;
                traverse(value, getNextPath(current_path, key), result);
            }
        }
        else if (element.isArray())
        {
            auto array = element.getArray();
            std::unordered_map<String, Array> arrays_by_path;
            size_t current_size = 0;

            for (auto it = array.begin(); it != array.end(); ++it)
            {
                ParseResult element_result;
                traverse(*it, "", element_result);

                NameSet inserted_paths;
                const auto & [paths, values] = element_result;
                for (size_t i = 0; i < paths.size(); ++i)
                {
                    inserted_paths.insert(paths[i]);

                    auto & path_array = arrays_by_path[paths[i]];
                    assert(path_array.size() == 0 || path_array.size() == current_size);

                    if (path_array.size() == 0)
                    {
                        path_array.reserve(paths.size());
                        path_array.resize(current_size);
                    }

                    path_array.push_back(values[i]);
                }

                for (auto & [path, path_array] : arrays_by_path)
                {
                    if (!inserted_paths.count(path))
                        path_array.push_back(Field());
                }

                ++current_size;
            }

            if (arrays_by_path.empty())
            {
                result.paths.push_back(current_path);
                result.values.push_back(Array());
            }
            else
            {
                result.paths.reserve(result.paths.size() + arrays_by_path.size());
                result.values.reserve(result.paths.size() + arrays_by_path.size());

                for (const auto & [path, path_array] : arrays_by_path)
                {
                    result.paths.push_back(getNextPath(current_path, path));
                    result.values.push_back(path_array);
                }
            }
        }
        else
        {
            result.paths.push_back(current_path);
            result.values.push_back(getValueAsField(element));
        }
    }

    ParserImpl parser;
};

}
