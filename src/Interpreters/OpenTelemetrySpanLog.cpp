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
        // DateTime64 is really unwieldy -- there is no "normal" way to convert
        // it to an UInt64 count of microseconds, except:
        // 1) reinterpretAsUInt64(reinterpretAsFixedString(date)), which just
        // doesn't look sane;
        // 2) things like toUInt64(toDecimal64(date, 6) * 1000000) that are also
        // excessively verbose -- why do I have to write scale '6' again, and
        // write out 6 zeros? -- and also don't work because of overflow.
        // Also subtraction of two DateTime64 points doesn't work, so you can't
        // get duration.
        // It is much less hassle to just use UInt64 of microseconds.
        {std::make_shared<DataTypeUInt64>(), "start_time_us"},
        {std::make_shared<DataTypeUInt64>(), "finish_time_us"},
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
    // The user might add some ints values, and we will have Int Field, and the
    // insert will fail because the column requires Strings. Convert the fields
    // here, because it's hard to remember to convert them in all other places.
    Array string_values;
    string_values.reserve(attribute_values.size());
    for (const auto & value : attribute_values)
    {
        string_values.push_back(toString(value));
    }
    columns[i++]->insert(string_values);
}


OpenTelemetrySpanHolder::OpenTelemetrySpanHolder(const std::string & _operation_name)
{
    trace_id = 0;

    if (!CurrentThread::isInitialized())
    {
        // There may be no thread context if we're running inside the
        // clickhouse-client, e.g. reading an external table provided with the
        // `--external` option.
        return;
    }

    auto & thread = CurrentThread::get();

    trace_id = thread.thread_trace_context.trace_id;
    if (!trace_id)
    {
        return;
    }

    parent_span_id = thread.thread_trace_context.span_id;
    span_id = thread_local_rng();
    operation_name = _operation_name;
    start_time_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    thread.thread_trace_context.span_id = span_id;
}


OpenTelemetrySpanHolder::~OpenTelemetrySpanHolder()
{
    try
    {
        if (!trace_id)
        {
            return;
        }

        // First of all, return old value of current span.
        auto & thread = CurrentThread::get();
        assert(thread.thread_trace_context.span_id == span_id);
        thread.thread_trace_context.span_id = parent_span_id;

        // Not sure what's the best way to access the log from here.
        auto * thread_group = CurrentThread::getGroup().get();
        // Not sure whether and when this can be null.
        if (!thread_group)
        {
            return;
        }

        auto * context = thread_group->query_context;
        if (!context)
        {
            // Both global and query contexts can be null when executing a
            // background task, and global context can be null for some
            // queries.
            return;
        }

        auto log = context->getOpenTelemetrySpanLog();
        if (!log)
        {
            // The log might be disabled.
            return;
        }

        finish_time_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        log->add(OpenTelemetrySpanLogElement(
                     static_cast<OpenTelemetrySpan>(*this)));
    }
    catch (...)
    {
        tryLogCurrentException(__FUNCTION__);
    }
}


bool OpenTelemetryTraceContext::parseTraceparentHeader(const std::string & traceparent,
    std::string & error)
{
    trace_id = 0;

    uint8_t version = -1;
    uint64_t trace_id_high = 0;
    uint64_t trace_id_low = 0;

    // Version 00, which is the only one we can parse, is fixed width. Use this
    // fact for an additional sanity check.
    const int expected_length = 2 + 1 + 32 + 1 + 16 + 1 + 2;
    if (traceparent.length() != expected_length)
    {
        error = fmt::format("unexpected length {}, expected {}",
            traceparent.length(), expected_length);
        return false;
    }

    // clang-tidy doesn't like sscanf:
    //   error: 'sscanf' used to convert a string to an unsigned integer value,
    //   but function will not report conversion errors; consider using 'strtoul'
    //   instead [cert-err34-c,-warnings-as-errors]
    // There is no other ready solution, and hand-rolling a more complicated
    // parser for an HTTP header in C++ sounds like RCE.
    // NOLINTNEXTLINE(cert-err34-c)
    int result = sscanf(&traceparent[0],
        "%2" SCNx8 "-%16" SCNx64 "%16" SCNx64 "-%16" SCNx64 "-%2" SCNx8,
        &version, &trace_id_high, &trace_id_low, &span_id, &trace_flags);

    if (result == EOF)
    {
        error = "EOF";
        return false;
    }

    // We read uint128 as two uint64, so 5 parts and not 4.
    if (result != 5)
    {
        error = fmt::format("could only read {} parts instead of the expected 5",
            result);
        return false;
    }

    if (version != 0)
    {
        error = fmt::format("unexpected version {}, expected 00", version);
        return false;
    }

    trace_id = static_cast<__uint128_t>(trace_id_high) << 64
        | trace_id_low;
    return true;
}


std::string OpenTelemetryTraceContext::composeTraceparentHeader() const
{
    // This span is a parent for its children, so we specify this span_id as a
    // parent id.
    return fmt::format("00-{:032x}-{:016x}-{:02x}", trace_id,
        span_id,
        // This cast is needed because fmt is being weird and complaining that
        // "mixing character types is not allowed".
        static_cast<uint8_t>(trace_flags));
}


}

