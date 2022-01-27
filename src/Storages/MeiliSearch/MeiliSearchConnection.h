#pragma once

#include <string>
#include <string_view>
#include <unordered_map>
#include <base/types.h>
#include <curl/curl.h>

namespace DB
{
struct MeiliSearchConfiguration
{
    String key;
    String index;
    String connection_string;

    MeiliSearchConfiguration(const String & url_, const String & index_, const String & key_) : index{index_}
    {
        connection_string = url_ + "/indexes/" + index_ + "/";
        key = "Authorization: Bearer " + key_;
    }
};

using MeiliConfig = MeiliSearchConfiguration;

class MeiliSearchConnection
{
public:
    explicit MeiliSearchConnection(const MeiliConfig & config);

    String searchQuery(const std::unordered_map<String, String> & query_params) const;

    String updateQuery(std::string_view data) const;

private:
    CURLcode execQuery(std::string_view url, std::string_view post_fields, std::string & response_buffer) const;

    MeiliConfig config;
};

}
