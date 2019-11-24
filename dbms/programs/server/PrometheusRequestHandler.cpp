#include "PrometheusRequestHandler.h"

#include <IO/HTTPCommon.h>

#include <Common/Exception.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>


#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>

#include <IO/WriteBufferFromHTTPServerResponse.h>


namespace
{

constexpr auto profile_events_prefix = "ClickHouse_ProfileEvents_";
constexpr auto current_metrix_prefix = "ClickHouse_Metrics_";

template<typename T>
void writeOutLine(DB::WriteBuffer & wb, T && val)
{
    writeText(std::forward<T>(val), wb);
    writeChar('\n', wb);
}

template<typename T, typename... TArgs>
void writeOutLine(DB::WriteBuffer & wb, T && val, TArgs &&... args)
{
    writeText(std::forward<T>(val), wb);
    writeChar(' ', wb);
    writeOutLine(wb, std::forward<TArgs>(args)...);
}

}

namespace DB
{

void WriteMetricsResponse(WriteBufferFromHTTPServerResponse & wb)
{
    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
    {
        const auto counter = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);

        std::string metric_name{ProfileEvents::getName(static_cast<ProfileEvents::Event>(i))};
        std::string metric_doc{ProfileEvents::getDocumentation(static_cast<ProfileEvents::Event>(i))};

        std::string key{profile_events_prefix + metric_name};

        writeOutLine(wb, "# HELP", key, metric_doc);
        writeOutLine(wb, "# TYPE", key, "counter");
        writeOutLine(wb, key, counter);
    }

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        const auto value = CurrentMetrics::values[i].load(std::memory_order_relaxed);

        std::string metric_name{CurrentMetrics::getName(static_cast<CurrentMetrics::Metric>(i))};
        std::string metric_doc{CurrentMetrics::getDocumentation(static_cast<CurrentMetrics::Metric>(i))};

        std::string key{current_metrix_prefix + metric_name};

        writeOutLine(wb, "# HELP", key, metric_doc);
        writeOutLine(wb, "# TYPE", key, "gauge");
        writeOutLine(wb, key, value);
    }
}

void PrometheusRequestHandler::handleRequest(
    Poco::Net::HTTPServerRequest & request,
    Poco::Net::HTTPServerResponse & response)
{
    try
    {
        const auto & config = server.config();
        unsigned keep_alive_timeout = config.getUInt("keep_alive_timeout", 10);

        setResponseDefaultHeaders(response, keep_alive_timeout);

        response.setContentType("text/plain; version=0.0.4; charset=UTF-8");

        auto wb = WriteBufferFromHTTPServerResponse(request, response, keep_alive_timeout);
        WriteMetricsResponse(wb);
        wb.finalize();
    }
    catch (...)
    {
        tryLogCurrentException("PrometheusRequestHandler");
    }
}

}
