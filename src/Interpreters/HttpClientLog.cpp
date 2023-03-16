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
            {"AWS",         static_cast<Int8>(HttpClient::AWS)}
        }
    );

    auto http_method_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"GET",         static_cast<Int8>(HttpMethod::GET)},
            {"POST",        static_cast<Int8>(HttpMethod::POST)},
            {"DELETE",      static_cast<Int8>(HttpMethod::DELETE)},
            {"PUT",         static_cast<Int8>(HttpMethod::PUT)},
            {"HEAD",        static_cast<Int8>(HttpMethod::HEAD)},
            {"PATCH",       static_cast<Int8>(HttpMethod::PATCH)},
        }
    );

    return {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime64>(6)},
        {"client", std::move(http_client_type)},
        {"query_id", std::make_shared<DataTypeString>()},

        /// In the system table opentelemetry_log, trace id is designed as type of UUID.
        /// In such case, when trace_id column is selected, its value is formatted as 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxx'.
        ///
        /// However, the OpenTelemetry standard treats trace id as a hex string without the dash character, that is 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        /// it's not convenient for users to copy the format of UUID to search it in tracing log systems that follows OpenTelemetry standard.
        ///
        /// So we define the trace id as String in storage to make it consistent with OpenTelemetry standard, which will simply the SELECT sql.
        /// This requires extra 16 bytes compared to saving it as UUID, and it's affordable.
        {"trace_id", std::make_shared<DataTypeString>()},
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
    columns[i++]->insert(trace_id == UUID() ? "" : fmt::format("{:016x}{:016x}",trace_id.toUnderType().items[0],trace_id.toUnderType().items[1]));
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

bool HttpClientLog::addLogEntry(
    HttpClientLogElement::HttpClient client,
    HttpClientLogElement::HttpMethod method,
    std::string_view uri,
    UInt64 duration_ms,
    int status_code,
    UInt64 request_content_length,
    UInt64 response_content_length,
    const ExecutionStatus & exception) noexcept
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
        elem.event_time_microseconds = time_in_microseconds(time_now) - duration_ms * 1000;

        auto query_id = CurrentThread::getQueryId();
        if (!query_id.empty())
            elem.query_id.insert(0, query_id.data(), query_id.size());

        elem.client = client;
        elem.trace_id = OpenTelemetry::CurrentContext().trace_id;
        elem.span_id = OpenTelemetry::CurrentContext().span_id;
        elem.duration_ms = duration_ms;
        elem.method = method;
        elem.uri = uri;
        elem.status_code = status_code;
        elem.request_size = request_content_length;
        elem.response_size = response_content_length;
        elem.exception_code = exception.code;
        elem.exception = exception.message;

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
