#include "Utils.h"
#include <optional>
#include <stdexcept>
#include "Common/Exception.h"
#include <string>

#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/Dynamic/Var.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace Mongo
{

const char* findKth(const char* begin, const char* end, char token, size_t k)
{
    const char* iter = begin;
    for (size_t i = 0; i < k; ++i)
    {
        if (i != 0 && iter != end) {
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

std::pair<const char*, const char*> getMetadataSubstring(const char* begin, const char* end)
{
    const char* position = findKth(begin, end, '(', 1);
    if (position == end)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid query: can not find ) in query");
    }
    return {begin, position};
}

std::pair<const char*, const char*> getSettingsSubstring(const char* begin, const char* end)
{
    const char* position_start = findKth(begin, end, '(', 1);
    if (position_start == end)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid query: can not find ( in query");
    }
    
    const char* position_end = findKth(begin, end, ')', 1);
    if (position_end == end)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid query: : can not find ) in your query ");
    }
    return {position_start + 1, position_end};
}

void validateFirstMetadataArgument(const char* begin, const char* end)
{
    size_t size = end - begin;
    if (size < 2 || *(end - 1) != 'b' || *(end - 2) != 'd')
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid query: first argument of query should be 'db'");
    } 
}

std::optional<rapidjson::Value> findField(const rapidjson::Value& value, const std::string& key)
{
    for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it)
    {
        if (it->name.GetString() == key)
        {
            return copyValue(it->value);
        }
    }
    return std::nullopt;
}

rapidjson::Value parseData(const char* begin, const char* end)
{
    std::string input(begin, end);
    std::replace(input.begin(), input.end(), '\'', '"');

    rapidjson::Document document;

    if (document.Parse(input.data()).HasParseError()) 
    {
        throw std::runtime_error("Error while parsing json in parseData #" + input + "#");
    }
    return copyValue(document);
}

std::optional<size_t> MongoQueryKeyNameExtractor::findPosition(const char* begin, const char* end)
{
    size_t size_str = end - begin;
    for (size_t i = 0; i < size_str - pattern.size() + 1; ++i)
    {
        bool match = true;
        for (size_t j = 0; j < pattern.size(); ++j)
        {
            if (begin[i + j] != pattern[j])
            {
                match = false;
                break;
            }   
        }
        if (match)
        {
            if (begin[i + pattern.size()] != '(')
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect query : after {} should be (", pattern);
            }
            return i + pattern.size() + 1;
        }
    }
    return std::nullopt;
}

std::optional<int> MongoQueryKeyNameExtractor::extractInt(const char* begin, const char* end)
{
    auto maybe_start_position = findPosition(begin, end);
    if (!maybe_start_position)
    {
        return std::nullopt;
    }
    auto start_position = *maybe_start_position;
    std::string str_representation;
    while (begin[start_position] != ')')
    {
        if (begin[start_position] < '0' || begin[start_position] > '9')
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect query : pattern {} should contain only numbers", pattern);
        }
        str_representation.push_back(begin[start_position]);
        ++start_position;
    }
    return std::stoi(str_representation);
}

std::optional<std::string> MongoQueryKeyNameExtractor::extractString(const char* begin, const char* end)
{
    auto maybe_start_position = findPosition(begin, end);
    if (!maybe_start_position)
    {
        return std::nullopt;
    }
    auto start_position = *maybe_start_position;
    std::string result;
    while (begin[start_position] != ')')
    {
        result.push_back(begin[start_position]);
        ++start_position;
    }
    return result;
}

}

}
