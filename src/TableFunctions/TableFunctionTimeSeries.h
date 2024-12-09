#pragma once

#include <Parsers/ASTViewTargets.h>
#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageTimeSeries.h>


namespace DB
{

/// Table functions timeSeriesData('mydb', 'my_ts_table'), timeSeriesTags('mydb', 'my_ts_table'), timeSeriesMetrics('mydb', 'my_ts_table')
/// return the data table, the tags table, and the metrics table respectively associated with any TimeSeries table mydb.my_ts_table
template <ViewTarget::Kind target_kind>
class TableFunctionTimeSeriesTarget : public ITableFunction
{
public:
    static constexpr auto name = (target_kind == ViewTarget::Data)
        ? "timeSeriesData"
        : ((target_kind == ViewTarget::Tags) ? "timeSeriesTags" : "timeSeriesMetrics");

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
    const char * getStorageTypeName() const override;

    StoragePtr getTargetTable(const ContextPtr & context) const;

    StorageID time_series_storage_id = StorageID::createEmpty();
    String target_table_type_name;
};

}
