#pragma once

#include <optional>

#include <rapidjson/document.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Mongo
{

template <char token>
const char * findKth(const char * begin, const char * end, size_t k)
{
    const char * iter = begin;
    for (size_t i = 0; i < k; ++i)
    {
        if (i != 0 && iter != end)
        {
            iter++;
        }
        while (iter < end && iter[0] != token)
        {
            iter++;
        }
        if (iter == end)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid query: there is less than {} tokens {}", k, token);
        }
    }
    return iter;

}

std::pair<const char *, const char *> getMetadataSubstring(const char * begin, const char * end);

std::pair<const char *, const char *> getSettingsSubstring(const char * begin, const char * end);

void validateFirstMetadataArgument(const char * begin, const char * end);

template <typename T>
rapidjson::Value copyValue(const T & value)
{
    static rapidjson::Document::AllocatorType allocator;

    rapidjson::Value result;
    result.CopyFrom(value, allocator);
    return result;
}

std::optional<rapidjson::Value> findField(const rapidjson::Value & value, const std::string & key);

rapidjson::Value parseData(const char * begin, const char * end);


class MongoQueryKeyNameExtractor
{
public:
    explicit MongoQueryKeyNameExtractor(const std::string & pattern_) : pattern(pattern_) { }

    std::optional<int> extractInt(const char * begin, const char * end);

    std::optional<std::string> extractString(const char * begin, const char * end);

private:
    std::optional<size_t> findPosition(const char * begin, const char * end);

    std::string pattern;
};

}

}
