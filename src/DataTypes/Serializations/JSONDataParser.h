#pragma once

#include <IO/ReadHelpers.h>
#include <Common/HashTable/HashMap.h>
#include <Common/checkStackSize.h>
#include <DataTypes/Serializations/PathInData.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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

template <typename ParserImpl>
class JSONDataParser
{
public:
    using Element = typename ParserImpl::Element;

    static void readJSON(String & s, ReadBuffer & buf)
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
        PathInDataBuilder builder;
        std::vector<PathInData::Parts> paths;

        traverse(document, builder, paths, result.values);

        result.paths.reserve(paths.size());
        for (auto && path : paths)
            result.paths.emplace_back(std::move(path));

        return result;
    }

private:
    void traverse(
        const Element & element,
        PathInDataBuilder & builder,
        std::vector<PathInData::Parts> & paths,
        std::vector<Field> & values)
    {
        checkStackSize();

        if (element.isObject())
        {
            auto object = element.getObject();

            paths.reserve(paths.size() + object.size());
            values.reserve(values.size() + object.size());

            for (auto it = object.begin(); it != object.end(); ++it)
            {
                const auto & [key, value] = *it;
                traverse(value, builder.append(key, false), paths, values);
                builder.popBack();
            }
        }
        else if (element.isArray())
        {
            auto array = element.getArray();

            using PathPartsWithArray = std::pair<PathInData::Parts, Array>;
            using PathToArray = HashMapWithStackMemory<UInt128, PathPartsWithArray, UInt128TrivialHash, 5>;

            /// Traverse elements of array and collect an array
            /// of fields by each path.

            PathToArray arrays_by_path;
            Arena strings_pool;

            size_t current_size = 0;
            for (auto it = array.begin(); it != array.end(); ++it)
            {
                std::vector<PathInData::Parts> element_paths;
                std::vector<Field> element_values;
                PathInDataBuilder element_builder;

                traverse(*it, element_builder, element_paths, element_values);
                size_t size = element_paths.size();
                size_t keys_to_update = arrays_by_path.size();

                for (size_t i = 0; i < size; ++i)
                {
                    UInt128 hash = PathInData::getPartsHash(element_paths[i]);
                    if (auto * found = arrays_by_path.find(hash))
                    {
                        auto & path_array = found->getMapped().second;

                        assert(path_array.size() == current_size);
                        path_array.push_back(std::move(element_values[i]));
                        --keys_to_update;
                    }
                    else
                    {
                        /// We found a new key. Add and empty array with current size.
                        Array path_array;
                        path_array.reserve(array.size());
                        path_array.resize(current_size);
                        path_array.push_back(std::move(element_values[i]));

                        auto & elem = arrays_by_path[hash];
                        elem.first = std::move(element_paths[i]);
                        elem.second = std::move(path_array);
                    }
                }

                /// If some of the keys are missed in current element,
                /// add default values for them.
                if (keys_to_update)
                {
                    for (auto & [_, value] : arrays_by_path)
                    {
                        auto & path_array = value.second;
                        assert(path_array.size() == current_size || path_array.size() == current_size + 1);
                        if (path_array.size() == current_size)
                            path_array.push_back(Field());
                    }
                }

                ++current_size;
            }

            if (arrays_by_path.empty())
            {
                paths.push_back(builder.getParts());
                values.push_back(Array());
            }
            else
            {
                paths.reserve(paths.size() + arrays_by_path.size());
                values.reserve(values.size() + arrays_by_path.size());

                for (auto && [_, value] : arrays_by_path)
                {
                    auto && [path, path_array] = value;

                    /// Merge prefix path and path of array element.
                    paths.push_back(builder.append(path, true).getParts());
                    values.push_back(std::move(path_array));

                    builder.popBack(path.size());
                }
            }
        }
        else
        {
            paths.push_back(builder.getParts());
            values.push_back(getValueAsField(element));
        }
    }

    ParserImpl parser;
};

}
