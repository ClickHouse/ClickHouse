#pragma once

#include <Interpreters/StorageID.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <TableFunctions/ITableFunction.h>


namespace DB
{

/// Table functions timeSeriesSelector('mydb', 'my_ts_table', 'instant_selector', min_time, max_time) reads time series from a specified
/// TimeSeries table corresponding to a specified instant selector and with timestamps in a specified interval [min_time, max_time].
/// Here instant_selector should be written in PromQL syntax, for example 'http_requests{job="prometheus"}',
/// and table can be specified either as two arguments 'mydb', 'my_ts_table', or one argument mydb.my_ts_table, or just 'my_ts_table'.
class TableFunctionTimeSeriesSelector : public ITableFunction
{
public:
    static constexpr auto name = "timeSeriesSelector";

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
        /// Technically it's TimeSeriesSelector but it doesn't register itself
        return "";
    }

    StorageID time_series_storage_id = StorageID::createEmpty();
    PrometheusQueryTree instant_selector;
    Field min_time;
    Field max_time;
};

}
