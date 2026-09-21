#pragma once

#include <Parsers/ASTViewTargets.h>
#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageTimeSeries.h>

#include <array>
#include <utility>


namespace DB
{

/// The names of the table functions returning the target tables of a TimeSeries table, by the kind of the target.
constexpr std::array<std::pair<ViewTarget::Kind, const char *>, 4> time_series_target_table_function_names{{
    {ViewTarget::Samples, "timeSeriesSamples"},
    {ViewTarget::Tags, "timeSeriesTags"},
    {ViewTarget::MetricFamilies, "timeSeriesMetricFamilies"},
    {ViewTarget::Histograms, "timeSeriesHistograms"},
}};

/// Returns nullptr for a kind without a table function.
constexpr const char * tryGetTimeSeriesTargetTableFunctionName(ViewTarget::Kind target_kind)
{
    for (const auto & [kind, name] : time_series_target_table_function_names)
    {
        if (kind == target_kind)
            return name;
    }
    return nullptr;
}

/// Table functions timeSeriesSamples('mydb', 'my_ts_table'), timeSeriesTags('mydb', 'my_ts_table'), timeSeriesMetricFamilies('mydb', 'my_ts_table'),
/// timeSeriesHistograms('mydb', 'my_ts_table') return the "samples" table, the "tags" table, the "metric families" table,
/// and the "histograms" table respectively associated with any TimeSeries table mydb.my_ts_table
template <ViewTarget::Kind target_kind>
class TableFunctionTimeSeriesTarget : public ITableFunction
{
    static_assert(tryGetTimeSeriesTargetTableFunctionName(target_kind) != nullptr, "No table function returns this kind of target table of a TimeSeries table");

public:
    static constexpr auto name = tryGetTimeSeriesTargetTableFunctionName(target_kind);

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
