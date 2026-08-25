#include <DataTypes/DataTypeDate.h>
#include <Common/SystemTableDocumentation.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>


namespace DB
{

ColumnsDescription AsynchronousMetricLogElement::getColumnsDescription()
{
    ParserCodec codec_parser;
    return ColumnsDescription
    {
        {
            "hostname",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Hostname of the server executing the query."
        },
        {
            "event_date",
            std::make_shared<DataTypeDate>(),
            parseQuery(codec_parser, "(Delta(2), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Event date."
        },
        {
            "event_time",
            std::make_shared<DataTypeDateTime>(),
            parseQuery(codec_parser, "(Delta(4), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Event time."
        },
        {
            "metric",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Metric name."
        },
        {
            "key",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "The key of a key-value metric, e.g. the CPU core number or the block device name. Empty for scalar metrics."
        },
        {
            "value",
            std::make_shared<DataTypeFloat64>(),
            parseQuery(codec_parser, "(ZSTD(3))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Metric value."
        }
    };
}

void AsynchronousMetricLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(event_date);
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(metric_name);
    columns[column_idx++]->insert(key);
    columns[column_idx++]->insert(value);
}

void AsynchronousMetricLog::addValues(const AsynchronousMetricValues & values)
{
    AsynchronousMetricLogElement element;

    element.event_time = time(nullptr);
    element.event_date = static_cast<UInt16>(DateLUT::instance().toDayNum(element.event_time));

    /// We will round the values to make them compress better in the table.
    /// Note: as an alternative we can also use fixed point Decimal data type,
    /// but we need to store up to UINT64_MAX sometimes.
    static constexpr double precision = 1000.0;

    auto round_value = [](double value) { return round(value * precision) / precision; };

    for (const auto & [name, value] : values)
    {
        element.metric_name = name;

        if (value.isMap())
        {
            /// A key-value metric is logged as one row per key.
            for (const auto & [key, key_value] : value.key_values)
            {
                element.key = key;
                element.value = round_value(key_value);

                add([&](AsynchronousMetricLogElement & log_element) { log_element = element; });
            }
        }
        else
        {
            element.key.clear();
            element.value = round_value(value.value);

            add([&](AsynchronousMetricLogElement & log_element) { log_element = element; });
        }
    }
}

}

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "asynchronous_metric_log",
    .description = R"DOCS_MD(
Contains the historical values for `system.asynchronous_metrics`, which are saved once per time interval (one second by default). Enabled by default.

Key-value metrics of `system.asynchronous_metrics` (those broken down per CPU core, block device, network interface, or disk) are logged as one row per key, with the key in the `key` column. For scalar metrics the `key` column is empty.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT * FROM system.asynchronous_metric_log LIMIT 3 \G
```

```text
Row 1:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2023-11-14
event_time: 2023-11-14 14:39:07
metric:     AsynchronousHeavyMetricsCalculationTimeSpent
key:
value:      0.001

Row 2:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2023-11-14
event_time: 2023-11-14 14:39:08
metric:     AsynchronousHeavyMetricsCalculationTimeSpent
key:
value:      0

Row 3:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2023-11-14
event_time: 2023-11-14 14:39:09
metric:     AsynchronousHeavyMetricsCalculationTimeSpent
key:
value:      0
```

**See Also**

- [asynchronous_metric_log setting](/reference/settings/server-settings/settings/asynchronous#asynchronous_metric_log) — Enabling and disabling the setting.
- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains metrics, calculated periodically in the background.
- [system.metric_log](/reference/system-tables/metric_log) — Contains history of metrics values from tables `system.metrics` and `system.events`, periodically flushed to disk.
)DOCS_MD")

}
