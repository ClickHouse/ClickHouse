#pragma once

#include <Parsers/ASTViewTargets.h>
#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageTimeSeries.h>


namespace DB
{

/// Table functions timeSeriesSamples('mydb', 'my_ts_table'), timeSeriesTags('mydb', 'my_ts_table'), timeSeriesMetrics('mydb', 'my_ts_table')
/// return the "samples" table, the "tags" table, and the "metrics" table respectively associated with any TimeSeries table mydb.my_ts_table
template <ViewTarget::Kind target_kind>
class TableFunctionTimeSeriesTarget : public ITableFunction
{
public:
    static constexpr auto name = (target_kind == ViewTarget::Samples)
        ? "timeSeriesSamples"
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
    const char * getStorageEngineName() const override;

    /// This function hands back a pre-existing table that stores data on disk, which a persistent table
    /// cannot proxy: the proxy renames that table in memory on its first read and then violates its own
    /// assumption that a table function stores no data.
    bool canBeUsedToCreateTable() const override { return false; }

    /// Authorizes the target by the name the TimeSeries table configures and by the identity that name
    /// resolves to, so neither a hidden target nor a rename escapes the check.
    StoragePtr getAuthorizedTargetTable(const ContextPtr & context, AccessType access_type) const;

    StorageID time_series_storage_id = StorageID::createEmpty();
    String target_table_type_name;
};

}
