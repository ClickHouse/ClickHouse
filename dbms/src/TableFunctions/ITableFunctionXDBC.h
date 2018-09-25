#pragma once

#include <Common/config.h>
#include <Common/XDBCBridgeHelper.h>
#include <TableFunctions/ITableFunction.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Storages/StorageXDBC.h>

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
                                     const std::string & remote_database_name,
                                     const std::string & remote_table_name,
                                     const ColumnsDescription & columns_,
                                     const Context & context_,
                                     BridgeHelperPtr bridge_helper) const = 0;

    /* A factory method to create bridge helper, that will assist in remote interaction */
    virtual BridgeHelperPtr createBridgeHelper(const Poco::Util::AbstractConfiguration & config_,
            const Poco::Timespan & http_timeout_,
            const std::string & connection_string_) const = 0;
};

class TableFunctionJDBC : public ITableFunctionXDBC
{
public:
    static constexpr auto name = "jdbc";
    std::string getName() const override
    {
        return name;
    }
private:
    StoragePtr createStorage(const std::string &table_name_, const std::string &remote_database_name,
                             const std::string &remote_table_name,
                             const ColumnsDescription &columns_,
                             const Context &context_, BridgeHelperPtr bridge_helper) const override {

        return std::make_shared<StorageJDBC>(table_name_, remote_database_name, remote_table_name, columns_, context_, bridge_helper);
    }


    BridgeHelperPtr createBridgeHelper(const Poco::Util::AbstractConfiguration & config_,
                                       const Poco::Timespan & http_timeout_,
                                       const std::string & connection_string_) const override {
        return std::make_shared<JDBCBridgeHelper>(config_, http_timeout_, connection_string_);
    }
};

class TableFunctionIDBC : public ITableFunctionXDBC
{
public:
    static constexpr auto name = "idbc";
    std::string getName() const override
    {
        return name;
    }
private:
    StoragePtr createStorage(const std::string &table_name_, const std::string &remote_database_name,
                             const std::string &remote_table_name,
                             const ColumnsDescription &columns_,
                             const Context &context_, BridgeHelperPtr bridge_helper) const override {

        return std::make_shared<StorageIDBC>(table_name_, remote_database_name, remote_table_name, columns_, context_, bridge_helper);
    }


    BridgeHelperPtr createBridgeHelper(const Poco::Util::AbstractConfiguration & config_,
                                       const Poco::Timespan & http_timeout_,
                                       const std::string & connection_string_) const override {
        return std::make_shared<XDBCBridgeHelper<ODBCBridgeMixin>>(config_, http_timeout_, connection_string_);
    }
};

}
