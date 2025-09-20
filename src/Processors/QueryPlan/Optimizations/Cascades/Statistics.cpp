#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <IO/Operators.h>
#include <boost/algorithm/string/split.hpp>
#include <optional>
#include <unordered_map>
#include <vector>


namespace DB
{

void ExpressionStatistics::dump(WriteBuffer & out) const
{
    out << "estimated_rows: " << estimated_row_count
        << " min_rows: " << min_row_count
        << " max_rows: " << max_row_count
        << "\n";
    for (const auto & column : column_statistics)
        out << "`" << column.first << "` NDV : " << column.second.number_of_distinct_values << "\n";
}

String ExpressionStatistics::dump() const
{
    WriteBufferFromOwnString out;
    dump(out);
    return out.str();
}

/// TODO: this is a temporary hack until table names are properly handled
String getUnqualifiedColumnName(const String & full_column_name)
{
    std::vector<String> identifiers;
    boost::split(identifiers, full_column_name, [](char c) { return c == '.'; });
    return identifiers.back();
}


/// FIXME: This is a stub just for testing
class TPCH100StatisticsStub : public IOptimizerStatistics
{
public:
    TPCH100StatisticsStub()
    {
        column_statistics["c_custkey"] = { .number_of_distinct_values = 15000000 };
        column_statistics["c_name"] = { .number_of_distinct_values = 15000000 };
        column_statistics["c_address"] = { .number_of_distinct_values = 15000000 };
        column_statistics["c_nationkey"] = { .number_of_distinct_values = 25 };
        column_statistics["c_phone"] = { .number_of_distinct_values = 14997159 };
        column_statistics["c_acctbal"] = { .number_of_distinct_values = 1099998 };
        column_statistics["c_mktsegment"] = { .number_of_distinct_values = 5 };
        column_statistics["c_comment"] = { .number_of_distinct_values = 14640181 };
        column_statistics["l_orderkey"] = { .number_of_distinct_values = 150000000 };
        column_statistics["l_partkey"] = { .number_of_distinct_values = 20000000 };
        column_statistics["l_suppkey"] = { .number_of_distinct_values = 1000000 };
        column_statistics["l_linenumber"] = { .number_of_distinct_values = 7 };
        column_statistics["l_quantity"] = { .number_of_distinct_values = 50 };
        column_statistics["l_extendedprice"] = { .number_of_distinct_values = 3786026 };
        column_statistics["l_discount"] = { .number_of_distinct_values = 11 };
        column_statistics["l_tax"] = { .number_of_distinct_values = 9 };
        column_statistics["l_returnflag"] = { .number_of_distinct_values = 3 };
        column_statistics["l_linestatus"] = { .number_of_distinct_values = 2 };
        column_statistics["l_shipdate"] = { .number_of_distinct_values = 2526 };
        column_statistics["l_commitdate"] = { .number_of_distinct_values = 2466 };
        column_statistics["l_receiptdate"] = { .number_of_distinct_values = 2555 };
        column_statistics["l_shipinstruct"] = { .number_of_distinct_values = 4 };
        column_statistics["l_shipmode"] = { .number_of_distinct_values = 7 };
        column_statistics["l_comment"] = { .number_of_distinct_values = 134121421 };
        column_statistics["n_nationkey"] = { .number_of_distinct_values = 25 };
        column_statistics["n_name"] = { .number_of_distinct_values = 25 };
        column_statistics["n_regionkey"] = { .number_of_distinct_values = 5 };
        column_statistics["n_comment"] = { .number_of_distinct_values = 25 };
        column_statistics["o_orderkey"] = { .number_of_distinct_values = 150000000 };
        column_statistics["o_custkey"] = { .number_of_distinct_values = 9999832 };
        column_statistics["o_orderstatus"] = { .number_of_distinct_values = 3 };
        column_statistics["o_totalprice"] = { .number_of_distinct_values = 34700489 };
        column_statistics["o_orderdate"] = { .number_of_distinct_values = 2406 };
        column_statistics["o_orderpriority"] = { .number_of_distinct_values = 5 };
        column_statistics["o_clerk"] = { .number_of_distinct_values = 100000 };
        column_statistics["o_shippriority"] = { .number_of_distinct_values = 1 };
        column_statistics["o_comment"] = { .number_of_distinct_values = 108747580 };
        column_statistics["p_partkey"] = { .number_of_distinct_values = 20000000 };
        column_statistics["p_name"] = { .number_of_distinct_values = 19983675 };
        column_statistics["p_mfgr"] = { .number_of_distinct_values = 5 };
        column_statistics["p_brand"] = { .number_of_distinct_values = 25 };
        column_statistics["p_type"] = { .number_of_distinct_values = 150 };
        column_statistics["p_size"] = { .number_of_distinct_values = 50 };
        column_statistics["p_container"] = { .number_of_distinct_values = 40 };
        column_statistics["p_retailprice"] = { .number_of_distinct_values = 119899 };
        column_statistics["p_comment"] = { .number_of_distinct_values = 3588774 };
        column_statistics["ps_partkey"] = { .number_of_distinct_values = 20000000 };
        column_statistics["ps_suppkey"] = { .number_of_distinct_values = 1000000 };
        column_statistics["ps_availqty"] = { .number_of_distinct_values = 9999 };
        column_statistics["ps_supplycost"] = { .number_of_distinct_values = 99901 };
        column_statistics["ps_comment"] = { .number_of_distinct_values = 71530484 };
        column_statistics["r_regionkey"] = { .number_of_distinct_values = 5 };
        column_statistics["r_name"] = { .number_of_distinct_values = 5 };
        column_statistics["r_comment"] = { .number_of_distinct_values = 5 };
        column_statistics["s_suppkey"] = { .number_of_distinct_values = 1000000 };
        column_statistics["s_name"] = { .number_of_distinct_values = 1000000 };
        column_statistics["s_address"] = { .number_of_distinct_values = 1000000 };
        column_statistics["s_nationkey"] = { .number_of_distinct_values = 25 };
        column_statistics["s_phone"] = { .number_of_distinct_values = 999998 };
        column_statistics["s_acctbal"] = { .number_of_distinct_values = 656829 };
        column_statistics["s_comment"] = { .number_of_distinct_values = 997150 };
    }

    std::optional<UInt64> getNumberOfDistinctValues(const String & full_column_name) const override
    {
        auto column_name = getUnqualifiedColumnName(full_column_name);
        auto column_stats_it = column_statistics.find(column_name);
        if (column_stats_it != column_statistics.end())
        {
            return column_stats_it->second.number_of_distinct_values;
        }

        return std::nullopt;
    }

private:
    struct ColumnStatistics
    {
        UInt64 number_of_distinct_values;
    };

    std::unordered_map<String, ColumnStatistics> column_statistics;
};

OptimizerStatisticsPtr createTPCH100Statistics()
{
    return std::make_unique<TPCH100StatisticsStub>();
}


class EmptyStatistics : public IOptimizerStatistics
{
public:
    std::optional<UInt64> getNumberOfDistinctValues(const String & /*column_name*/) const override { return std::nullopt; }
};

OptimizerStatisticsPtr createEmptyStatistics()
{
    return std::make_unique<EmptyStatistics>();
}

}
