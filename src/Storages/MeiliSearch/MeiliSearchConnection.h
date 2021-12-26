#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <base/types.h>
#include <utility>
#include <vector>
#include <iostream>

namespace DB
{

struct MeiliSearchConfiguration
{
    String host;
    UInt16 port;
    String key;
    String index;
    String connection_string;

    MeiliSearchConfiguration(
        const String& host_,
        const UInt16& port_,
        const String& index_,
        const String& key_) : 
        host{host_}, port{port_}, index{index_} {
            connection_string = "http://" + host_ + ":" + std::to_string(port_) + "/indexes/" + index_ + "/";
            key = "X-Meili-API-Key:" + key_;
        }
    
    MeiliSearchConfiguration(
        const String& url_,
        const String& index_,
        const String& key_) : 
        index{index_} {
            connection_string = url_ + "/indexes/" + index_ + "/";
            key = "X-Meili-API-Key:" + key_;
        }

};

using MeiliConfig = MeiliSearchConfiguration;

class MeiliSearchConnection {
public:

    explicit MeiliSearchConnection(const MeiliConfig& config);

    explicit MeiliSearchConnection(MeiliConfig&& config);

    String searchQuery(const std::vector<std::pair<String, String>>& query_params) const;

private:

    MeiliConfig config;

};

}
