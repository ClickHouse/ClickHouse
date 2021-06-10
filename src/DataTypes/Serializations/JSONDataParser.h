#pragma once

#include <IO/ReadHelpers.h>
#include <Common/HashTable/HashMap.h>

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

            using PathToArray = HashMap<StringRef, Array, StringRefHash>;
            PathToArray arrays_by_path;
            Arena strings_pool;

            size_t current_size = 0;

            for (auto it = array.begin(); it != array.end(); ++it)
            {
                ParseResult element_result;
                traverse(*it, "", element_result);

                auto && [paths, values] = element_result;
                for (size_t i = 0; i < paths.size(); ++i)
                {
                    const auto & path = paths[i];

                    if (auto found = arrays_by_path.find(path))
                    {
                        auto & path_array = found->getMapped();
                        assert(path_array.size() == current_size);
                        path_array.push_back(std::move(values[i]));
                    }
                    else
                    {
                        StringRef ref{strings_pool.insert(path.data(), path.size()), path.size()};
                        auto & path_array = arrays_by_path[ref];

                        path_array.reserve(array.size());
                        path_array.resize(current_size);
                        path_array.push_back(std::move(values[i]));
                    }
                }

                for (auto & [path, path_array] : arrays_by_path)
                {
                    assert(path_array.size() == current_size || path_array.size() == current_size + 1);
                    if (path_array.size() == current_size)
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

                for (auto && [path, path_array] : arrays_by_path)
                {
                    result.paths.push_back(getNextPath(current_path, static_cast<std::string_view>(path)));
                    result.values.push_back(std::move(path_array));
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
