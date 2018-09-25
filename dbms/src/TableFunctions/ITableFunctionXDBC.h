#pragma once

#include <Common/config.h>
#include <Common/IXDBCBridgeHelper.h>
#include <TableFunctions/ITableFunction.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
/**
 * Base class for table functions, that works over external bridge
 * Xdbc (Xdbc connect string, table) - creates a temporary StorageXDBC.
 */
class ITableFunctionXDBC : public ITableFunction
{
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context) const override;

    /* A factory method for creating the storage implementation */
    virtual StoragePtr createStorage(const std::string & table_name_,
                                     const std::string & connection_string,
                                     const std::string & remote_database_name,
                                     const std::string & remote_table_name,
                                     const ColumnsDescription & columns_,
                                     const Context & context_) const = 0;

    /* A factory method to create bridge helper, that will assist in remote interaction */
    virtual BridgeHelperPtr createBridgeHelper(const Poco::Util::AbstractConfiguration & config_,
            const Poco::Timespan & http_timeout_,
            const std::string & connection_string_) const = 0;
};
}
