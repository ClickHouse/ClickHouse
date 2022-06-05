#include "config_core.h"

#include <TableFunctions/ITableFunction.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/StorageRedis.h>

namespace DB
{

class TableFunctionRedis : public ITableFunction
{
public:
    static constexpr auto name = "redis";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "Redis"; }

ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    std::optional<StorageRedisConfiguration> configuration;
};

}
