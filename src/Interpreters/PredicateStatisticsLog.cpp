#include <Columns/ColumnArray.h>
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
