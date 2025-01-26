#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageTimeSeries.h>


namespace DB
{

/// Table function prometheusQuery('mydb', 'ts_table', 'query') executes a prometheus query on a TimeSeries table.
/// This table function can execute either instant or range queries.
class TableFunctionPrometheusQuery : public ITableFunction
{
public:
    static constexpr auto name = "prometheusQuery";
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

    StorageID time_series_storage_id = StorageID::createEmpty();
    String promql_query;
};

}
