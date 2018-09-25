#include <Storages/ColumnsDescription.h>
#include <TableFunctions/TableFunctionJDBC.h>
#include <Common/JDBCBridgeHelper.h>


namespace DB
{
    StoragePtr TableFunctionJDBC::createStorage(const std::string & , const std::string & ,
                                         const std::string & , const std::string & ,
                                         const ColumnsDescription & , const Context & ) const {
        return nullptr;
    }


    BridgeHelperPtr TableFunctionJDBC::createBridgeHelper(const Poco::Util::AbstractConfiguration & config_,
                                       const Poco::Timespan & http_timeout_,
                                       const std::string & connection_string_) const {
        return std::make_shared<JDBCBridgeHelper>(config_, http_timeout_, connection_string_);
    }
}
