#pragma once

#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/URI.h>
#include <Poco/Net/HTTPRequest.h>

namespace DB
{
class ElasticsearchClient
{
public:
    ElasticsearchClient(ElasticsearchConfiguration, ContextPtr);

    using IndexPage = Poco::JSON::Array::Ptr;

    IndexPage searchIndex(bool fetch_source) const;

private:

    Poco::JSON::Object::Ptr sendRequestToElastic(
        const String & method,
        const Poco::URI & uri,
        const String & request_body) const;

    ElasticsearchConfiguration config;
    Poco::Net::HTTPBasicCredentials credentials;
    ContextPtr context;
};
}
