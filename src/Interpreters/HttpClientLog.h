#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>

namespace DB
{

struct HttpClientLogEntry
{
    enum class HttpMethod
    {
        GET,
        POST,
        DELETE,
        PUT,
        HEAD,
        PATCH
    };

    enum class HttpClient
    {
        AWS
    };

    HttpClient http_client;
    HttpMethod http_method;
    std::string_view uri;
    UInt64 duration_ms;
    int status_code;
    Int64 request_size;
    Int64 response_size;
    const ExecutionStatus & exception;
};

typedef std::function<void(const HttpClientLogEntry &)> HttpClientRequestLogger;

/// The structure of httpclient_log table
struct HttpClientLogElement
{
    Decimal64 event_time_microseconds = 0;
    UInt64 duration_ms = 0;
    HttpClientLogEntry::HttpClient client;

    String query_id;
    UUID trace_id;
    UInt64 span_id;

    HttpClientLogEntry::HttpMethod method;
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

    static bool addLogEntry(const HttpClientLogEntry& log_entry) noexcept;
};

}
