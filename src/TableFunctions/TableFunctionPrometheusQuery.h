#pragma once

#include <Interpreters/StorageID.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationRange.h>
#include <TableFunctions/ITableFunction.h>
#include <boost/blank.hpp>


namespace DB
{

/// Table functions prometheusQuery('mydb', 'my_ts_table', 'promql_query', evaluation_time) evaluates a prometheus query.
/// Depending on the type of the specified prometheus query this table function returns either columns (tags, timestamp, values) or
/// (tags, time_series) or (scalar) or (string).
/// Time series table can be specified either as two arguments 'mydb', 'my_ts_table', or one argument mydb.my_ts_table, or just 'my_ts_table'.
/// Table functions prometheusQueryRange('mydb', 'my_ts_table', 'promql_query', start_time, end_time, step) evaluates a prometheus query
/// over a range of evaluation times.
template <bool range>
class TableFunctionPrometheusQuery : public ITableFunction
{
public:
    static constexpr auto name = range ? "prometheusQueryRange" : "prometheusQuery";

    String getName() const override { return name; }

private:
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    const char * getStorageEngineName() const override
    {
        /// Technically it's PrometheusQuery but it doesn't register itself
        return "";
    }

    StorageID time_series_storage_id = StorageID::createEmpty();
    PrometheusQueryTree promql_query;
    std::conditional_t<!range, Field, boost::blank> evaluation_time;
    std::conditional_t<range, PrometheusQueryEvaluationRange, boost::blank> evaluation_range;
};

}
