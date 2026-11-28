#pragma once

#include <Storages/StorageTimeSeriesSelector.h>
#include <TableFunctions/ITableFunction.h>


namespace DB
{

/// Table function timeSeriesSelector('mydb', 'my_ts_table', 'instant_selector', min_time, max_time, return_columns) reads the time
/// series of a TimeSeries table matching an instant selector in PromQL syntax, e.g. 'http_requests{job="prometheus"}', with timestamps
/// in [min_time, max_time]. The table is given as 'mydb', 'my_ts_table', or mydb.my_ts_table, or just 'my_ts_table'.
/// `return_columns` names the columns besides `id`: 'samples' returns the column `samples` (an array of (timestamp, value) sorted by
/// timestamp; a series can come in several rows, e.g. one per bucket of the samples table), 'histograms' the column `histograms` of
/// a table storing native histograms (the stored tuples, see TimeSeriesHistogramsColumns), 'samples_and_histograms' both of the same
/// rows, 'timestamp_and_value' the columns `timestamp` and `value` with one float sample per row. Returned rows have samples in
/// the range, of a returned kind for a range shorter than a bucket. The samples are converted when the table stores them the other way.
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
