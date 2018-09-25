#pragma once

#include <Common/config.h>
#include <TableFunctions/ITableFunctionXDBC.h>

namespace DB
{
/* jdbc (odbc connect string, table) - creates a temporary StorageJDBC.
 */
class TableFunctionJDBC : public ITableFunctionXDBC
{
public:
    static constexpr auto name = "jdbc";
    std::string getName() const override
    {
        return name;
    }
private:
    StoragePtr createStorage(const std::string &table_name_, const std::string &connection_string,
                             const std::string &remote_database_name, const std::string &remote_table_name,
                             const ColumnsDescription &columns_, const Context &context_) const override;

    BridgeHelperPtr createBridgeHelper(const Poco::Util::AbstractConfiguration & config_,
                                       const Poco::Timespan & http_timeout_,
                                       const std::string & connection_string_) const override;
};
}
