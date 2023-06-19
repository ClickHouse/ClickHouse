#include <Interpreters/OpenTelemetrySpanLog.h>

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

NamesAndTypesList OpenTelemetrySpanLogElement::getNamesAndTypes()
{
    auto span_kind_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"INTERNAL",    static_cast<Int8>(OpenTelemetry::INTERNAL)},
            {"SERVER",      static_cast<Int8>(OpenTelemetry::SERVER)},
            {"CLIENT",      static_cast<Int8>(OpenTelemetry::CLIENT)},
            {"PRODUCER",    static_cast<Int8>(OpenTelemetry::PRODUCER)},
            {"CONSUMER",    static_cast<Int8>(OpenTelemetry::CONSUMER)}
        }
    );

    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return {
        {"trace_id", std::make_shared<DataTypeUUID>()},
        {"span_id", std::make_shared<DataTypeUInt64>()},
        {"parent_span_id", std::make_shared<DataTypeUInt64>()},
        {"operation_name", low_cardinality_string},
        {"kind", std::move(span_kind_type)},
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
        {"attribute", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeString>())},
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

    columns[i++]->insert(trace_id);
    columns[i++]->insert(span_id);
    columns[i++]->insert(parent_span_id);
    columns[i++]->insert(operation_name);
    columns[i++]->insert(kind);
    columns[i++]->insert(start_time_us);
    columns[i++]->insert(finish_time_us);
    columns[i++]->insert(DateLUT::instance().toDayNum(finish_time_us / 1000000).toUnderType());
    // The user might add some ints values, and we will have Int Field, and the
    // insert will fail because the column requires Strings. Convert the fields
    // here, because it's hard to remember to convert them in all other places.
    columns[i++]->insert(attributes);
}

}
