#pragma once

#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/Elasticsearch/ElasticsearchSettings.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/URI.h>
#include <Poco/Net/HTTPRequest.h>
#include <Common/logger_useful.h>

namespace DB
{
class ElasticsearchClient
{
public:
    ElasticsearchClient(ElasticsearchConfiguration, ContextPtr, const ElasticsearchSettings &);

    ~ElasticsearchClient();

    using IndexPage = Poco::JSON::Array::Ptr;

    IndexPage searchIndex(bool fetch_source);

    size_t pageSize() const { return page_size; }

private:

    void setPointInTime();

    void deletePointInTime();

    void validateShards(Poco::JSON::Object::Ptr response) const;

    Poco::JSON::Object::Ptr sendRequestToElastic(
        const String & method,
        const Poco::URI & uri,
        const String & request_body) const;

    size_t page_size;
    String pit_id;
    String pit_keep_alive;
    Poco::JSON::Array::Ptr last_document_order_no;
    ElasticsearchConfiguration config;
    Poco::Net::HTTPBasicCredentials credentials;
    HTTPHeaderEntries auth_headers;
    LoggerPtr log;
    ContextPtr context;
};
}
