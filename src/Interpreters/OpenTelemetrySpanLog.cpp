#include "OpenTelemetrySpanLog.h"

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Context.h>

#include <Common/hex.h>
#include <Common/CurrentThread.h>
#include <Core/Field.h>


namespace DB
{

NamesAndTypesList OpenTelemetrySpanLogElement::getNamesAndTypes()
{
    return {
        {"trace_id", std::make_shared<DataTypeUUID>()},
        {"span_id", std::make_shared<DataTypeUInt64>()},
        {"parent_span_id", std::make_shared<DataTypeUInt64>()},
        {"operation_name", std::make_shared<DataTypeString>()},
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
        {"start_time_us", std::make_shared<DataTypeUInt64>()},
        {"finish_time_us", std::make_shared<DataTypeUInt64>()},
        {"finish_date", std::make_shared<DataTypeDate>()},
        {"attribute", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())},
    };
}

NamesAndAliases OpenTelemetrySpanLogElement::getNamesAndAliases()
{
    return
    {
        {"attribute.names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "mapKeys(attribute)"},
        {"attribute.values", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "mapValues(attribute)"}
    };
}

void OpenTelemetrySpanLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(trace_id);
    columns[i++]->insert(span_id);
    columns[i++]->insert(parent_span_id);
    columns[i++]->insert(operation_name);
    columns[i++]->insert(start_time_us);
    columns[i++]->insert(finish_time_us);
    columns[i++]->insert(DateLUT::instance().toDayNum(finish_time_us / 1000000).toUnderType());
    // The user might add some ints values, and we will have Int Field, and the
    // insert will fail because the column requires Strings. Convert the fields
    // here, because it's hard to remember to convert them in all other places.
    columns[i++]->insert(attributes);
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
    if (trace_id == UUID())
        return;

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
        if (trace_id == UUID())
            return;

        // First of all, return old value of current span.
        auto & thread = CurrentThread::get();
        assert(thread.thread_trace_context.span_id == span_id);
        thread.thread_trace_context.span_id = parent_span_id;

        // Not sure what's the best way to access the log from here.
        auto * thread_group = CurrentThread::getGroup().get();
        // Not sure whether and when this can be null.
        if (!thread_group)
            return;

        ContextPtr context;
        {
            std::lock_guard lock(thread_group->mutex);
            context = thread_group->query_context.lock();
        }

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

void OpenTelemetrySpanHolder::addAttribute(const std::string& name, UInt64 value)
{
    if (trace_id == UUID())
        return;

    this->attributes.push_back(Tuple{name, toString(value)});
}

void OpenTelemetrySpanHolder::addAttribute(const std::string& name, const std::string& value)
{
    if (trace_id == UUID())
        return;

    this->attributes.push_back(Tuple{name, value});
}

void OpenTelemetrySpanHolder::addAttribute(const Exception & e)
{
    if (trace_id == UUID())
        return;

    this->attributes.push_back(Tuple{"clickhouse.exception", getExceptionMessage(e, false)});
}

void OpenTelemetrySpanHolder::addAttribute(std::exception_ptr e)
{
    if (trace_id == UUID() || e == nullptr)
        return;

    this->attributes.push_back(Tuple{"clickhouse.exception", getExceptionMessage(e, false)});
}

bool OpenTelemetryTraceContext::parseTraceparentHeader(const std::string & traceparent,
    std::string & error)
{
    trace_id = 0;

    // Version 00, which is the only one we can parse, is fixed width. Use this
    // fact for an additional sanity check.
    const int expected_length = strlen("xx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-xxxxxxxxxxxxxxxx-xx");
    if (traceparent.length() != expected_length)
    {
        error = fmt::format("unexpected length {}, expected {}",
            traceparent.length(), expected_length);
        return false;
    }

    const char * data = traceparent.data();

    uint8_t version = unhex2(data);
    data += 2;

    if (version != 0)
    {
        error = fmt::format("unexpected version {}, expected 00", version);
        return false;
    }

    if (*data != '-')
    {
        error = fmt::format("Malformed traceparant header: {}", traceparent);
        return false;
    }

    ++data;
    UInt64 trace_id_higher_64 = unhexUInt<UInt64>(data);
    UInt64 trace_id_lower_64 = unhexUInt<UInt64>(data + 16);
    data += 32;

    if (*data != '-')
    {
        error = fmt::format("Malformed traceparant header: {}", traceparent);
        return false;
    }

    ++data;
    UInt64 span_id_64 = unhexUInt<UInt64>(data);
    data += 16;

    if (*data != '-')
    {
        error = fmt::format("Malformed traceparant header: {}", traceparent);
        return false;
    }

    ++data;
    this->trace_flags = unhex2(data);

    // store the 128-bit trace id in big-endian order
    this->trace_id.toUnderType().items[0] = trace_id_higher_64;
    this->trace_id.toUnderType().items[1] = trace_id_lower_64;
    this->span_id = span_id_64;
    return true;
}


std::string OpenTelemetryTraceContext::composeTraceparentHeader() const
{
    // This span is a parent for its children, so we specify this span_id as a
    // parent id.
    return fmt::format("00-{:016x}{:016x}-{:016x}-{:02x}",
                       // Output the trace id in network byte order
                       trace_id.toUnderType().items[0],
                       trace_id.toUnderType().items[1],
                       span_id,
                       // This cast is needed because fmt is being weird and complaining that
                       // "mixing character types is not allowed".
                       static_cast<uint8_t>(trace_flags));
}


}
