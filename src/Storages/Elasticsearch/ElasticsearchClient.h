#pragma once

#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <Storages/Elasticsearch/ElasticsearchSearchResponse.h>
#include <Interpreters/Context_fwd.h>
#include <Formats/FormatSettings.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadSettings.h>
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

    std::unique_ptr<ElasticsearchSearchResponse> searchIndex(const ElasticsearchHitColumns & hit_columns);

    void finishPage(const ElasticsearchSearchResponse & response);

    size_t pageSize() const { return page_size; }

private:

    void setPointInTime();

    void deletePointInTime();

    std::unique_ptr<ReadBuffer> sendRequest(
        const String & method,
        const Poco::URI & uri,
        const String & request_body,
        const ReadSettings & request_read_settings,
        const ConnectionTimeouts & request_timeouts) const;

    size_t page_size;
    String pit_id;
    String pit_keep_alive;
    Poco::JSON::Array::Ptr last_document_order_no;
    ElasticsearchConfiguration config;
    Poco::Net::HTTPBasicCredentials credentials;
    HTTPHeaderEntries auth_headers;
    LoggerPtr log;
    ContextPtr context;
    ReadSettings read_settings;
    ConnectionTimeouts timeouts;
    FormatSettings format_settings;
};
}
