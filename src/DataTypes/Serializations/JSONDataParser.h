#pragma once

#include <IO/ReadHelpers.h>
#include <Common/HashTable/HashMap.h>
#include <Common/checkStackSize.h>
#include <DataTypes/Serializations/DataPath.h>
#include <boost/dynamic_bitset.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_DIMENSIONS_MISMATHED;
}

class ReadBuffer;
class WriteBuffer;

template <typename Element>
static Field getValueAsField(const Element & element)
{
    if (element.isBool())   return element.getBool();
    if (element.isInt64())  return element.getInt64();
    if (element.isUInt64()) return element.getUInt64();
    if (element.isDouble()) return element.getDouble();
    if (element.isString()) return element.getString();
    if (element.isNull())   return Field();

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported type of JSON field");
}

struct ParseResult
{
    std::vector<Path> paths;
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
        traverse(document, {}, result);
        return result;
    }

private:
    void traverse(const Element & element, Path current_path, ParseResult & result)
    {
        checkStackSize();

        if (element.isObject())
        {
            auto object = element.getObject();
            result.paths.reserve(result.paths.size() + object.size());
            result.values.reserve(result.values.size() + object.size());

            for (auto it = object.begin(); it != object.end(); ++it)
            {
                const auto & [key, value] = *it;
                traverse(value, Path::getNext(current_path, Path(key)), result);
            }
        }
        else if (element.isArray())
        {
            auto array = element.getArray();

            using PathWithArray = std::pair<Path, Array>;
            using PathToArray = HashMap<StringRef, PathWithArray, StringRefHash>;

            PathToArray arrays_by_path;
            Arena strings_pool;

            size_t current_size = 0;

            for (auto it = array.begin(); it != array.end(); ++it)
            {
                ParseResult element_result;
                traverse(*it, {}, element_result);

                auto & [paths, values] = element_result;
                for (size_t i = 0; i < paths.size(); ++i)
                {
                    if (auto * found = arrays_by_path.find(paths[i].getPath()))
                    {
                        auto & path_array = found->getMapped().second;

                        assert(path_array.size() == current_size);
                        path_array.push_back(std::move(values[i]));
                    }
                    else
                    {
                        Array path_array;
                        path_array.reserve(array.size());
                        path_array.resize(current_size);
                        path_array.push_back(std::move(values[i]));

                        const auto & path_str = paths[i].getPath();
                        StringRef ref{strings_pool.insert(path_str.data(), path_str.size()), path_str.size()};

                        auto & elem = arrays_by_path[ref];
                        elem.first = std::move(paths[i]);
                        elem.second = std::move(path_array);
                    }
                }

                for (auto & [_, value] : arrays_by_path)
                {
                    auto & path_array = value.second;
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

                bool is_nested = arrays_by_path.size() > 1 || arrays_by_path.begin()->getKey().size != 0;

                for (auto && [_, value] : arrays_by_path)
                {
                    auto && [path, path_array] = value;
                    result.paths.push_back(Path::getNext(current_path, path, is_nested));
                    result.values.push_back(std::move(path_array));
                }
            }
        }
        else
        {
            result.paths.push_back(std::move(current_path));
            result.values.push_back(getValueAsField(element));
        }
    }

    ParserImpl parser;
};

}
