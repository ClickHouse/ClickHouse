#include <Columns/ColumnArray.h>
#include <Common/SystemTableDocumentation.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/PredicateStatisticsLog.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <base/getFQDNOrHostName.h>


namespace DB
{

namespace
{

ASTPtr codecZSTD(UInt64 level)
{
    return makeASTFunction("CODEC",
        makeASTFunction("ZSTD", make_intrusive<ASTLiteral>(level)));
}

ASTPtr codecDeltaZSTD(UInt64 delta_bytes)
{
    return makeASTFunction("CODEC",
        makeASTFunction("Delta", make_intrusive<ASTLiteral>(delta_bytes)),
        makeASTFunction("ZSTD", make_intrusive<ASTLiteral>(UInt64(1))));
}

}

ColumnsDescription PredicateStatisticsLogElement::getColumnsDescription()
{
    auto lc_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto array_lc_string = std::make_shared<DataTypeArray>(lc_string);
    auto array_uint64 = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    auto array_float64 = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());

    return ColumnsDescription
    {
        {
            "hostname",
            lc_string,
            codecZSTD(1),
            "Hostname of the server executing the query."
        },
        {
            "event_date",
            std::make_shared<DataTypeDate>(),
            codecDeltaZSTD(2),
            "Event date."
        },
        {
            "event_time",
            std::make_shared<DataTypeDateTime>(),
            codecDeltaZSTD(4),
            "Timestamp when this log entry was written."
        },
        {
            "database",
            lc_string,
            codecZSTD(1),
            "Database name of the target table."
        },
        {
            "table",
            lc_string,
            codecZSTD(1),
            "Table name of the target table."
        },
        {
            "query_id",
            std::make_shared<DataTypeString>(),
            codecZSTD(1),
            "Query ID for linking back to query_log."
        },
        {
            "predicate_expression",
            std::make_shared<DataTypeString>(),
            codecZSTD(1),
            "Whole filter expression handled by this prewhere/filter step (ActionsDAG dump)."
        },
        {
            "input_rows",
            std::make_shared<DataTypeUInt64>(),
            codecZSTD(1),
            "Rows entering this prewhere/filter step."
        },
        {
            "passed_rows",
            std::make_shared<DataTypeUInt64>(),
            codecZSTD(1),
            "Rows surviving this prewhere/filter step."
        },
        {
            "filter_selectivity",
            std::make_shared<DataTypeFloat64>(),
            codecZSTD(1),
            "Selectivity of this step: passed_rows / input_rows."
        },

        {
            "total_input_rows",
            std::make_shared<DataTypeUInt64>(),
            codecZSTD(1),
            "Rows entering the first prewhere step (total rows read from granules)."
        },
        {
            "total_passed_rows",
            std::make_shared<DataTypeUInt64>(),
            codecZSTD(1),
            "Rows surviving all prewhere steps (rows delivered to the query)."
        },
        {
            "total_selectivity",
            std::make_shared<DataTypeFloat64>(),
            codecZSTD(1),
            "Selectivity of the whole predicate: total_passed_rows / total_input_rows."
        },

        {
            "index_names",
            array_lc_string,
            codecZSTD(1),
            "Names of indexes applied, e.g. ['PrimaryKey', 'idx_bf_status'] (index rows only)."
        },
        {
            "index_types",
            array_lc_string,
            codecZSTD(1),
            "Types of indexes applied: PrimaryKey, Skip, MinMax, Partition (index rows only)."
        },
        {
            "total_granules",
            array_uint64,
            codecZSTD(1),
            "Granules entering each index stage (index rows only)."
        },
        {
            "granules_after",
            array_uint64,
            codecZSTD(1),
            "Granules remaining after each index stage (index rows only)."
        },
        {
            "index_selectivities",
            array_float64,
            codecZSTD(1),
            "Per-index selectivity: granules_after / total_granules (index rows only)."
        }
    };
}

void PredicateStatisticsLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(event_date);
    columns[i++]->insert(event_time);
    columns[i++]->insert(database);
    columns[i++]->insert(table);
    columns[i++]->insert(query_id);
    columns[i++]->insert(predicate_expression);

    columns[i++]->insert(input_rows);
    columns[i++]->insert(passed_rows);
    columns[i++]->insert(filter_selectivity);

    columns[i++]->insert(total_input_rows);
    columns[i++]->insert(total_passed_rows);
    columns[i++]->insert(total_selectivity);

    /// index-level arrays
    auto fill_string_array = [](const std::vector<String> & data, IColumn & column)
    {
        auto & arr_col = typeid_cast<ColumnArray &>(column);
        auto & lc_data = typeid_cast<ColumnLowCardinality &>(arr_col.getData());
        for (const auto & val : data)
            lc_data.insertData(val.data(), val.size());
        arr_col.getOffsets().push_back(arr_col.getOffsets().back() + data.size());
    };

    auto fill_uint64_array = [](const std::vector<UInt64> & data, IColumn & column)
    {
        auto & arr_col = typeid_cast<ColumnArray &>(column);
        auto & num_data = typeid_cast<ColumnUInt64 &>(arr_col.getData()).getData();
        for (auto val : data)
            num_data.push_back(val);
        arr_col.getOffsets().push_back(arr_col.getOffsets().back() + data.size());
    };

    auto fill_float64_array = [](const std::vector<Float64> & data, IColumn & column)
    {
        auto & arr_col = typeid_cast<ColumnArray &>(column);
        auto & num_data = typeid_cast<ColumnFloat64 &>(arr_col.getData()).getData();
        for (auto val : data)
            num_data.push_back(val);
        arr_col.getOffsets().push_back(arr_col.getOffsets().back() + data.size());
    };

    fill_string_array(index_names, *columns[i++]);
    fill_string_array(index_types, *columns[i++]);
    fill_uint64_array(total_granules, *columns[i++]);
    fill_uint64_array(granules_after, *columns[i++]);
    fill_float64_array(index_selectivities, *columns[i++]);
}

}

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "predicate_statistics_log",
    .description = R"DOCS_MD(
Contains sampled selectivity statistics collected while reading from `MergeTree` tables. The table is populated only when [`predicate_statistics_sample_rate`](/reference/settings/session-settings/other#predicate_statistics_sample_rate) is greater than `0`.

<Info>
**Availability**

`system.predicate_statistics_log` is created only when the server configuration contains a `predicate_statistics_log` section. After creating the log, set `predicate_statistics_sample_rate` to a value greater than `0` to collect rows. Without the log section, queries against the table fail with `UNKNOWN_TABLE`.

```xml
<clickhouse>
    <predicate_statistics_log>
        <database>system</database>
        <table>predicate_statistics_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </predicate_statistics_log>
</clickhouse>
```
</Info>

Use this table to inspect how selective user predicates are in real workloads and how many granules remain after primary-key or skip-index filtering. The data is intended as input for workload-driven index and projection recommendations.

## Row shapes {#row-shapes}

A single query can produce two kinds of rows in `system.predicate_statistics_log`:

- **Filter rows**, emitted per prewhere/filter step in `MergeTreeSelectProcessor`. They populate `predicate_expression`, `input_rows`, `passed_rows`, `filter_selectivity`, and the whole-predicate columns `total_input_rows`, `total_passed_rows`, `total_selectivity`. Index-related columns are empty.
- **Index rows**, emitted per read step in `ReadFromMergeTree`. They populate the `index_names`, `index_types`, `total_granules`, `granules_after`, and `index_selectivities` arrays, one entry per index stage (primary key, partition, skip indexes). Predicate-related columns are empty.

Filter rows and index rows for the same query share the same `query_id` and `table`, so they can be joined when both are needed.

## Sampling and overhead {#sampling-and-overhead}

Sampling is controlled by [`predicate_statistics_sample_rate`](/reference/settings/session-settings/other#predicate_statistics_sample_rate):

- `0` disables collection.
- `1` samples every query.
- `N > 1` samples approximately `1 / N` of queries, hashed by `query_id`.

Lower values produce more data but add CPU work on the read path and more writes to the system log. After enabling the setting, use [`SYSTEM FLUSH LOGS`](/reference/statements/system#flush-logs) if you need rows to appear immediately.
)DOCS_MD",
    .get_columns = PredicateStatisticsLogElement::getColumnsDescription,
    .examples = R"DOCS_MD(
```sql
SET predicate_statistics_sample_rate = 1;

SELECT *
FROM hits
WHERE URL LIKE '%/product/%' AND EventDate >= today() - 7
FORMAT Null;

SYSTEM FLUSH LOGS predicate_statistics_log;

SELECT
    query_id,
    predicate_expression,
    round(filter_selectivity, 3) AS step_selectivity,
    round(total_selectivity, 3) AS query_selectivity,
    index_names,
    index_selectivities
FROM system.predicate_statistics_log
WHERE table = 'hits'
ORDER BY event_time DESC
LIMIT 10;
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [`predicate_statistics_sample_rate`](/reference/settings/session-settings/other#predicate_statistics_sample_rate)
- [system.query_log](/reference/system-tables/query_log)
)DOCS_MD")

}
