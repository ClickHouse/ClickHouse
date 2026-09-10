#pragma once

#include <Storages/StorageTimeSeriesSelector.h>
#include <TableFunctions/ITableFunction.h>


namespace DB
{

/// Table function `timeSeriesHistogramSelector('mydb', 'my_ts_table', 'instant_selector', min_time, max_time)`: the histogram
/// sibling of `timeSeriesSelector`, reading native histogram samples in [min_time, max_time]; requires a histograms target.
class TableFunctionTimeSeriesHistogramSelector : public ITableFunction
{
public:
    static constexpr auto name = "timeSeriesHistogramSelector";

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
