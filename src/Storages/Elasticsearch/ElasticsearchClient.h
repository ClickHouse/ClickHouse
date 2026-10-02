#pragma once

#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
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

    IndexPage searchIndex(bool fetch_source);

private:

    void setPointInTime();

    void validateResponse(Poco::JSON::Object::Ptr response) const;

    Poco::JSON::Object::Ptr sendRequestToElastic(
        const String & method,
        const Poco::URI & uri,
        const String & request_body) const;

    String pit_id;
    Poco::JSON::Array::Ptr last_document_order_no;
    ElasticsearchConfiguration config;
    Poco::Net::HTTPBasicCredentials credentials;
    ContextPtr context;
};
}
