#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>

namespace DB
{

struct HttpClientLogElement
{
    enum class HttpMethod
    {
        HTTP_GET,
        HTTP_POST,
        HTTP_DELETE,
        HTTP_PUT,
        HTTP_HEAD,
        HTTP_PATCH
    };

    enum class HttpClient
    {
        AWS
    };

    Decimal64 event_time_microseconds = 0;
    UInt64 duration_ms = 0;
    HttpClient client;

    String query_id;
    UUID trace_id;
    UInt64 span_id;

    HttpMethod method;
    String uri;
    int status_code;
    UInt64 request_size;
    UInt64 response_size;

    int exception_code = 0;
    String exception;

    static std::string name() { return "HttpClientLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};

class HttpClientLog : public SystemLog<HttpClientLogElement>
{
    using SystemLog<HttpClientLogElement>::SystemLog;

    static std::weak_ptr<HttpClientLog> httpclient_log;

public:
    static void initialize(std::shared_ptr<HttpClientLog> httpclient_log_)
    {
        httpclient_log = httpclient_log_;
    }

    static std::shared_ptr<HttpClientLog> getLog()
    {
        return httpclient_log.lock();
    }

    static bool addLogEntry(
        HttpClientLogElement::HttpClient client,
        HttpClientLogElement::HttpMethod method,
        std::string_view uri,
        UInt64 duration_ms,
        int status_code,
        UInt64 request_content_length,
        UInt64 response_content_length,
        const ExecutionStatus & exception) noexcept;
};

}
