#pragma once

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <base/types.h>

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
        key = "X-Meili-API-Key:" + key_;
    }
};

using MeiliConfig = MeiliSearchConfiguration;

class MeiliSearchConnection
{
public:
    explicit MeiliSearchConnection(const MeiliConfig & config);

    explicit MeiliSearchConnection(MeiliConfig && config);

    String searchQuery(const std::unordered_map<String, String> & query_params) const;

    String updateQuery(std::string_view data) const;

private:
    MeiliConfig config;
};

}
