#pragma once

#include <Storages/BigQuery/BigQueryConfiguration.h>
#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <Poco/JSON/Parser.h>
#include <Poco/URI.h>
#include <Poco/Net/HTTPRequest.h>
#include <Common/logger_useful.h>

namespace DB 
{
class ElasticsearchClient
{
public:
    explicit ElasticsearchClient(ElasticsearchConfiguration, ContextPtr);

    using IndexPage = Poco::JSON::Array::Ptr;

    IndexPage searchIndex() const;

private:

    Poco::JSON::Object::Ptr sendRequestToElastic(
        const String & method,
        const Poco::URI & uri,
        const String & request_body) const;

    ElasticsearchConfiguration config;
    ContextPtr context;
    LoggerPtr log;
};
}
