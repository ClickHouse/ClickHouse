#include <base/getFQDNOrHostName.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Common/AsynchronousMetrics.h>


namespace DB
{

ColumnsDescription AsynchronousMetricLogElement::getColumnsDescription()
{
    ParserCodec codec_parser;
    ColumnsDescription columns
    {
        {
            "hostname",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            "Hostname of the server."
        },
        {
            "event_date",
            std::make_shared<DataTypeDate>(),
            "Event date."
        },
        {
            "event_time",
            std::make_shared<DataTypeDateTime>(),
            "Event time."
        }
    };

    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();
    AsynchronousMetrics::Descriptions descriptions = AsynchronousMetrics::getDescriptions();
    for (const auto & description : descriptions)
        columns.add({
            description.name,
            data_type_factory.get(description.type),
            parseQuery(codec_parser, "(ZSTD(3))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH),
            description.doc});

    return columns;
}

void AsynchronousMetricLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(event_date);
    columns[column_idx++]->insert(event_time);

    for (const auto & value : values)
    {
        if (value.isNull())
        {
            columns[column_idx++]->insertDefault();
        }
        else if (value.getType() == Field::Types::Float64)
        {
            /// We will round the values to make them compress better in the table.
            /// Note: as an alternative we can also use fixed point Decimal data type,
            /// but we need to store up to UINT64_MAX sometimes.
            static constexpr double precision = 1000.0;
            columns[column_idx++]->insert(round(value.get<Float64>() * precision) / precision);
        }
        else
        {
            columns[column_idx++]->insert(value);
        }
    }
}

void AsynchronousMetricLog::addValues(const AsynchronousMetricValues & values)
{
    AsynchronousMetricLogElement element;

    element.event_time = time(nullptr);
    element.event_date = DateLUT::instance().toDayNum(element.event_time);
    element.values = values;
}

}
