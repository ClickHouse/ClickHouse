#pragma once
#include <optional>
#include <Common/config.h>

#if USE_CASSANDRA
#include <TableFunctions/ITableFunction.h>
#include <Storages/ExternalDataSourceConfiguration.h>


namespace DB
{


class TableFunctionCassandra : public ITableFunction
{

public:
    static constexpr auto name = "cassandra";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "Cassandra"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    std::optional<StorageCassandraConfiguration> configuration;
};
}

#endif
