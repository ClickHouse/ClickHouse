#pragma once

#include <string>
#include <string_view>
#include <unordered_map>
#include <base/types.h>

#include <Poco/Exception.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Path.h>
#include <Poco/URI.h>

namespace DB
{
struct MeiliSearchConfiguration
{
    String key;
    String index;
    String connection_string;

    MeiliSearchConfiguration(const String & url_, const String & index_, const String & key_) : key{key_}, index{index_}
    {
        connection_string = url_ + "/indexes/" + index_ + "/";
    }
};

using MeiliConfig = MeiliSearchConfiguration;

class MeiliSearchConnection
{
public:
    explicit MeiliSearchConnection(const MeiliConfig & config);

    String searchQuery(const std::unordered_map<String, String> & query_params) const;

    String getDocumentsQuery(const std::unordered_map<String, String> & query_params) const;

    String updateQuery(std::string_view data) const;

private:
    String execPostQuery(const String & url, std::string_view post_fields) const;

    String execGetQuery(const String & url, const std::unordered_map<String, String> & query_params) const;

    MeiliConfig config;
    mutable Poco::Net::HTTPClientSession session;
};

}
