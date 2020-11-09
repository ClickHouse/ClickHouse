#include "OpenTelemetrySpanLog.h"

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>

namespace DB
{

Block OpenTelemetrySpanLogElement::createBlock()
{
    return {
        {std::make_shared<DataTypeUUID>(), "trace_id"},
        {std::make_shared<DataTypeUInt64>(), "span_id"},
        {std::make_shared<DataTypeUInt64>(), "parent_span_id"},
        {std::make_shared<DataTypeString>(), "operation_name"},
        {std::make_shared<DataTypeDateTime64>(6), "start_time_us"},
        {std::make_shared<DataTypeDateTime64>(6), "finish_time_us"},
        {std::make_shared<DataTypeDate>(), "finish_date"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "attribute.names"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "attribute.values"}
    };
}

void OpenTelemetrySpanLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(UInt128(Int128(trace_id)));
    columns[i++]->insert(span_id);
    columns[i++]->insert(parent_span_id);
    columns[i++]->insert(operation_name);
    columns[i++]->insert(start_time_us);
    columns[i++]->insert(finish_time_us);
    columns[i++]->insert(DateLUT::instance().toDayNum(finish_time_us / 1000000));
    columns[i++]->insert(attribute_names);
    columns[i++]->insert(attribute_values);
}

OpenTelemetrySpanHolder::OpenTelemetrySpanHolder(const std::string & _operation_name)
{
    auto & thread = CurrentThread::get();

    trace_id = thread.opentelemetry_trace_id;
    if (!trace_id)
    {
        return;
    }

    parent_span_id = thread.opentelemetry_current_span_id;
    span_id = thread_local_rng();
    operation_name = _operation_name;
    start_time_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // *** remove this
    attribute_names.push_back("start.stacktrace");
    attribute_values.push_back(StackTrace().toString());

    thread.opentelemetry_current_span_id = span_id;
}

OpenTelemetrySpanHolder::~OpenTelemetrySpanHolder()
{
    try
    {
        fmt::print(stderr, "{}\n", StackTrace().toString());

        if (!trace_id)
        {
            return;
        }

        // First of all, return old value of current span.
        auto & thread = CurrentThread::get();
        assert(thread.opentelemetry_current_span_id = span_id);
        thread.opentelemetry_current_span_id = parent_span_id;

        // Not sure what's the best way to access the log from here.
        auto * thread_group = CurrentThread::getGroup().get();
        // Not sure whether and when this can be null.
        if (!thread_group)
        {
            return;
        }

        fmt::print(stderr, "1\n");

        auto * context = thread_group->query_context;
        if (!context)
        {
            // Both global and query contexts can be null when executing a
            // background task, and global context can be null for some
            // queries.
            return;
        }

        //******** remove this
        attribute_names.push_back("clickhouse.query_id");
        attribute_values.push_back(context->getCurrentQueryId());

        fmt::print(stderr, "2\n");

        auto log = context->getOpenTelemetrySpanLog();
        if (!log)
        {
            // The log might be disabled.
            return;
        }

        fmt::print(stderr, "3\n");

        finish_time_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        // We should use a high resolution monotonic clock for calculating
        // duration, but this way will do for now.
        duration_ns = (finish_time_us - start_time_us) * 1000;


        log->add(OpenTelemetrySpanLogElement(
                     static_cast<OpenTelemetrySpan>(*this)));

        fmt::print(stderr, "4\n");
    }
    catch (...)
    {
        tryLogCurrentException(__FUNCTION__);
    }
}

}

