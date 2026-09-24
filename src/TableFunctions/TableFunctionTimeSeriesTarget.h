#pragma once

#include <Parsers/ASTViewTargets.h>
#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageTimeSeries.h>


namespace DB
{

/// Table functions timeSeriesSamples('mydb', 'my_ts_table'), timeSeriesRecentSamples('mydb', 'my_ts_table'),
/// timeSeriesTags('mydb', 'my_ts_table'), timeSeriesTimeRanges('mydb', 'my_ts_table'), timeSeriesMetricFamilies('mydb', 'my_ts_table')
/// return the "samples" table, the "recent samples" table, the "tags" table, the "time ranges" table, and the "metric families" table
/// respectively associated with any TimeSeries table mydb.my_ts_table.
/// The functions for the optional targets ("recent samples", "time ranges") throw if the table has no such target.
template <ViewTarget::Kind target_kind>
class TableFunctionTimeSeriesTarget : public ITableFunction
{
public:
    static constexpr auto name = (target_kind == ViewTarget::Samples)
        ? "timeSeriesSamples"
        : ((target_kind == ViewTarget::RecentSamples) ? "timeSeriesRecentSamples"
        : ((target_kind == ViewTarget::Tags) ? "timeSeriesTags"
        : ((target_kind == ViewTarget::TimeRanges) ? "timeSeriesTimeRanges" : "timeSeriesMetricFamilies")));

    String getName() const override { return name; }

private:
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    /// The returned storage is the target table itself, so a persisted table would rename a live table in memory.
    bool canBeUsedToCreateTable() const override { return false; }

    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    const char * getStorageEngineName() const override;

    StoragePtr getTargetTable(const ContextPtr & context) const;

    StorageID time_series_storage_id = StorageID::createEmpty();
    String target_table_type_name;
};

}
