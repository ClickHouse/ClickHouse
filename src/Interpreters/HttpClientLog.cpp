#include <Interpreters/HttpClientLog.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <Interpreters/Context.h>

#include <Common/CurrentThread.h>
#include <Common/OpenTelemetryTraceContext.h>

namespace DB
{

NamesAndTypesList HttpClientLogElement::getNamesAndTypes()
{
    auto http_client_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"AWS",         static_cast<Int8>(HttpClientLogEntry::HttpClient::AWS)}
        }
    );

    auto http_method_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"GET",         static_cast<Int8>(HttpClientLogEntry::HttpMethod::GET)},
            {"POST",        static_cast<Int8>(HttpClientLogEntry::HttpMethod::POST)},
            {"DELETE",      static_cast<Int8>(HttpClientLogEntry::HttpMethod::DELETE)},
            {"PUT",         static_cast<Int8>(HttpClientLogEntry::HttpMethod::PUT)},
            {"HEAD",        static_cast<Int8>(HttpClientLogEntry::HttpMethod::HEAD)},
            {"PATCH",       static_cast<Int8>(HttpClientLogEntry::HttpMethod::PATCH)},
        }
    );

    return {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime64>(6)},
        {"client", std::move(http_client_type)},
        {"query_id", std::make_shared<DataTypeString>()},
        {"trace_id", std::make_shared<DataTypeUUID>()},
        {"span_id",  std::make_shared<DataTypeUInt64>()},
        {"duration_ms", std::make_shared<DataTypeUInt64>()},
        {"method", std::move(http_method_type)},
        {"uri",    std::make_shared<DataTypeString>()},
        {"status", std::make_shared<DataTypeInt32>()},
        {"request_size", std::make_shared<DataTypeUInt64>()},
        {"response_size", std::make_shared<DataTypeUInt64>()},
        {"exception_code", std::make_shared<DataTypeInt32>()},
        {"exception", std::make_shared<DataTypeString>()}
    };
}

void HttpClientLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(DateLUT::instance().toDayNum(event_time_microseconds / 1000 / 1000).toUnderType());
    columns[i++]->insert(event_time_microseconds);

    columns[i++]->insert(client);

    columns[i++]->insert(query_id);
    columns[i++]->insert(trace_id);
    columns[i++]->insert(span_id);

    columns[i++]->insert(duration_ms);

    columns[i++]->insert(method);
    columns[i++]->insert(uri);
    columns[i++]->insert(status_code);
    columns[i++]->insert(request_size);
    columns[i++]->insert(response_size);

    columns[i++]->insert(exception_code);
    columns[i++]->insert(exception);
}

inline UInt64 time_in_microseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
}

std::weak_ptr<HttpClientLog> HttpClientLog::httpclient_log;

bool HttpClientLog::addLogEntry(const HttpClientLogEntry& log_entry) noexcept
{
    try
    {
        auto httpclient_log_owned = getLog();
        if (!httpclient_log_owned)
        {
            return false;
        }

        HttpClientLogElement elem;

        /// Log event_time as the start time of this request
        const auto time_now = std::chrono::system_clock::now();
        elem.event_time_microseconds = time_in_microseconds(time_now) - log_entry.duration_ms * 1000;

        auto query_id = CurrentThread::getQueryId();
        if (!query_id.empty())
            elem.query_id.insert(0, query_id.data(), query_id.size());

        elem.client = log_entry.http_client;
        elem.trace_id = OpenTelemetry::CurrentContext().trace_id;
        elem.span_id = OpenTelemetry::CurrentContext().span_id;
        elem.duration_ms = log_entry.duration_ms;
        elem.method = log_entry.http_method;
        elem.uri = log_entry.uri;
        elem.status_code = log_entry.status_code;
        elem.request_size = log_entry.request_size;
        elem.response_size = log_entry.response_size;
        elem.exception_code = log_entry.exception.code;
        elem.exception = log_entry.exception.message;

        httpclient_log_owned->add(elem);
    }
    catch (...)
    {
        tryLogCurrentException(&Poco::Logger::get("HttpClientLog"), __PRETTY_FUNCTION__);
        return false;
    }

    return true;
}

}
