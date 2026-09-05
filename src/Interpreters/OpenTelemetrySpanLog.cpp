#include <Interpreters/OpenTelemetrySpanLog.h>

#include <base/getFQDNOrHostName.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeEnum.h>


namespace DB
{

ColumnsDescription OpenTelemetrySpanLogElement::getColumnsDescription()
{
    auto span_kind_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"INTERNAL",    static_cast<Int8>(OpenTelemetry::SpanKind::INTERNAL)},
            {"SERVER",      static_cast<Int8>(OpenTelemetry::SpanKind::SERVER)},
            {"CLIENT",      static_cast<Int8>(OpenTelemetry::SpanKind::CLIENT)},
            {"PRODUCER",    static_cast<Int8>(OpenTelemetry::SpanKind::PRODUCER)},
            {"CONSUMER",    static_cast<Int8>(OpenTelemetry::SpanKind::CONSUMER)}
        }
    );

    auto status_code_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"UNSET",   static_cast<Int8>(OpenTelemetry::SpanStatus::UNSET)},
            {"OK",      static_cast<Int8>(OpenTelemetry::SpanStatus::OK)},
            {"ERROR",   static_cast<Int8>(OpenTelemetry::SpanStatus::ERROR)}
        }
    );

    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return ColumnsDescription
    {
        {"hostname", low_cardinality_string, "The hostname where this span was captured."},
        {"trace_id", std::make_shared<DataTypeUUID>(), "ID of the trace for executed query."},
        {"span_id", std::make_shared<DataTypeUInt64>(), "ID of the trace span."},
        {"parent_span_id", std::make_shared<DataTypeUInt64>(), "ID of the parent trace span."},
        {"operation_name", low_cardinality_string, "The name of the operation."},
        {"kind", std::move(span_kind_type), "The SpanKind of the span. "
            "INTERNAL — Indicates that the span represents an internal operation within an application. "
            "SERVER — Indicates that the span covers server-side handling of a synchronous RPC or other remote request. "
            "CLIENT — Indicates that the span describes a request to some remote service. "
            "PRODUCER — Indicates that the span describes the initiators of an asynchronous request. This parent span will often end before the corresponding child CONSUMER span, possibly even before the child span starts. "
            "CONSUMER - Indicates that the span describes a child of an asynchronous PRODUCER request."},
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
        {"start_time_us", std::make_shared<DataTypeUInt64>(), "The start time of the trace span (in microseconds)."},
        {"finish_time_us", std::make_shared<DataTypeUInt64>(), "The finish time of the trace span (in microseconds)."},
        {"finish_date", std::make_shared<DataTypeDate>(), "The finish date of the trace span."},
        {"status_code", std::move(status_code_type), "The status code of the span."},
        {"status_message", low_cardinality_string, "Error message."},
        {"attribute", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeString>()), "Attribute depending on the trace span. They are filled in according to the recommendations in the OpenTelemetry standard."},
    };
}

NamesAndAliases OpenTelemetrySpanLogElement::getNamesAndAliases()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return
    {
        {"attribute.names", std::make_shared<DataTypeArray>(low_cardinality_string), "mapKeys(attribute)"},
        {"attribute.values", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "mapValues(attribute)"}
    };
}

void OpenTelemetrySpanLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    /// Write into the columns directly instead of boxing every value into a `Field`.
    /// `IColumn::insert(Field)` on a `LowCardinality` column reaches `ColumnUnique::uniqueInsert`,
    /// which allocates a throw-away `ColumnString` per value, and the `attribute` map additionally
    /// materializes a `Map` of `Tuple`s of `Field`s - about seven allocations per attribute.
    /// This is the dominant cost of a span log flush: in a Memory Sanitizer CI run, where the
    /// stateless tests are executed with `opentelemetry_start_trace_probability = 0.1`, building
    /// the block for a batch of 325865 spans took 99 s while the `INSERT` itself took 2.3 s. Once
    /// a flush is slower than the rate at which spans are produced the queue keeps growing, and
    /// `SYSTEM FLUSH LOGS opentelemetry_span_log` starts to exceed its 180 s timeout.
    const auto & hostname = getFQDNOrHostName();
    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(hostname.data(), hostname.size());
    typeid_cast<ColumnUUID &>(*columns[i++]).getData().push_back(span.trace_id);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(span.span_id);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(span.parent_span_id);
    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(span.operation_name.data(), span.operation_name.size());
    typeid_cast<ColumnInt8 &>(*columns[i++]).getData().push_back(static_cast<Int8>(span.kind));
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(span.start_time_us);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(span.finish_time_us);
    typeid_cast<ColumnUInt16 &>(*columns[i++]).getData().push_back(
        DateLUT::instance().toDayNum(span.finish_time_us / 1000000).toUnderType());
    typeid_cast<ColumnInt8 &>(*columns[i++]).getData().push_back(static_cast<Int8>(span.status_code));
    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(span.status_message.data(), span.status_message.size());

    {
        auto & column_map = typeid_cast<ColumnMap &>(*columns[i++]);
        auto & offsets = column_map.getNestedColumn().getOffsets();
        auto & tuple_column = column_map.getNestedData();
        auto & key_column = typeid_cast<ColumnLowCardinality &>(tuple_column.getColumn(0));
        auto & value_column = typeid_cast<ColumnString &>(tuple_column.getColumn(1));

        for (const auto & attribute : span.attributes)
        {
            const auto & key = attribute.getKey();
            key_column.insertData(key.data(), key.size());

            const auto value = attribute.getValue();
            value_column.insertData(value.data(), value.size());
        }

        offsets.push_back(offsets.back() + span.attributes.size());
    }
}

}
