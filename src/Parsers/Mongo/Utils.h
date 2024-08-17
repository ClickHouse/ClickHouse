#pragma once

#include <optional>
#include <vector>

#include <Poco/JSON/Object.h>
#include <rapidjson/document.h>

namespace DB
{

namespace Mongo
{

const char* findKth(const char* begin, const char* end, char token, size_t k);

std::pair<const char*, const char*> getMetadataSubstring(const char* begin, const char* end);

std::pair<const char*, const char*> getSettingsSubstring(const char* begin, const char* end);

void validateFirstMetadataArgument(const char* begin, const char* end);

template <typename T>
rapidjson::Value copyValue(const T& value)
{
    static rapidjson::Document::AllocatorType allocator;

    rapidjson::Value result;
    result.CopyFrom(value, allocator);
    return result;
}

std::optional<rapidjson::Value> findField(const rapidjson::Value& value, const std::string& key);

rapidjson::Value parseData(const char* begin, const char* end);


class MongoQueryKeyNameExtractor
{
public:
    explicit MongoQueryKeyNameExtractor(const std::string& pattern_)
        : pattern(pattern_)
    {}

    std::optional<int> extractInt(const char* begin, const char* end);    

    std::optional<std::string> extractString(const char* begin, const char* end);    

private:
    std::optional<size_t> findPosition(const char* begin, const char* end);

    std::string pattern;
};

}

}
