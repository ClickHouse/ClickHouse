#pragma once

#include <bitset>
#include <type_traits>
#include <IO/WriteHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/BinaryRow.h>
#include <base/types.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Common/Exception.h>


namespace DB
{
struct PaimonTableSchema;
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

namespace Paimon
{
using namespace DB;

class PathEscape
{
public:
    PathEscape() = delete;

private:
    static const std::bitset<128> CHAR_TO_ESCAPE;

    static void escapeChar(unsigned char c, std::string & output)
    {
        output += '%';
        if (c < 16)
        {
            output += '0';
        }
        const char hex_digits[] = "0123456789ABCDEF";
        output += hex_digits[(c >> 4) & 0xF];
        output += hex_digits[c & 0xF];
    }

public:
    static std::string escapePathName(const std::string & path)
    {
        if (path.empty())
        {
            return path;
        }

        std::string escaped;
        escaped.reserve(path.size() * 2);

        for (unsigned char c : path)
        {
            if (needsEscaping(c))
            {
                escapeChar(c, escaped);
            }
            else
            {
                escaped += c;
            }
        }
        return escaped;
    }

    static bool needsEscaping(unsigned char c) { return c < CHAR_TO_ESCAPE.size() && CHAR_TO_ESCAPE.test(c); }
};

String getBucketPath(String partition, Int32 bucket, const PaimonTableSchema & table_schema, const String & partition_default_name);
String concatPath(std::initializer_list<String> paths);

template <typename T>
void getValueFromJSON(T & t, const Poco::JSON::Object::Ptr & json, const String & key)
{
    if (!json->has(key) || json->isNull(key))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,"JSON object does not have key: {}", key);
    }
    t = json->getValue<std::remove_reference_t<decltype(t)>>(key);
}


template <typename T>
void getOptionalValueFromJSON(std::optional<T> & t, const Poco::JSON::Object::Ptr & json, const String & key)
{
    if (json->has(key) && !json->isNull(key))
    {
        using type = typename std::remove_reference_t<decltype(t)>::value_type;
        t = json->getValue<type>(key);
    }
}

template <typename T>
void getVecFromJSON(std::vector<T> & vec, const Poco::JSON::Object::Ptr & json, const String & key)
{
    const auto & json_array = json->getArray(key);
    if (!json_array)
    {
        return;
    }
    vec.reserve(json_array->size());
    for (uint32_t i = 0; i < json_array->size(); ++i)
    {
        vec.emplace_back(json_array->getElement<T>(i));
    }
}

template <typename T>
void getMapFromJSON(std::unordered_map<String, T> & map, const Poco::JSON::Object::Ptr & json, const String & key)
{
    const auto & json_object = json->getObject(key);
    if (!json_object)
    {
        return;
    }
    map.reserve(json_object->size());

    for (const auto & inner_key : json_object->getNames())
    {
        map.emplace(inner_key, json_object->getValue<T>(inner_key));
    }
}
}
