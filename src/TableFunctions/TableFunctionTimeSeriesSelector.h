#pragma once

#include <Storages/StorageTimeSeriesSelector.h>
#include <TableFunctions/ITableFunction.h>


namespace DB
{

/// Table function timeSeriesSelector('mydb', 'my_ts_table', 'instant_selector', min_time, max_time, return_columns) reads time series
/// from a specified TimeSeries table corresponding to a specified instant selector and with timestamps in a specified interval [min_time, max_time].
/// Here instant_selector should be written in PromQL syntax, for example 'http_requests{job="prometheus"}',
/// and table can be specified either as two arguments 'mydb', 'my_ts_table', or one argument mydb.my_ts_table, or just 'my_ts_table'.
/// The argument `return_columns` chooses the returned columns besides `id`: 'samples' returns the column `samples` (an array of tuples
/// (timestamp, value) sorted by timestamp; a time series can be returned in multiple rows, e.g. one per bucket of the samples table)
/// and, for a table storing native histogram samples (see `TimeSeriesSettings::hasHistogramsInSamples`), the column `histograms` with
/// the histogram samples of the same rows as stored (an array of the tuples of the `histograms` column of the samples table, sorted by
/// timestamp and cut to the time range; see TimeSeriesHistogramsColumns), 'timestamp_and_value' returns the columns `timestamp` and
/// `value` with one float sample per row and no histogram samples. The samples are converted when the samples table stores them the
/// other way (see `TimeSeriesSettings::hasBucketedSamples`). Only the requested columns are read from the samples table.
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

    StorageTimeSeriesSelector::Configuration config;
};

}
