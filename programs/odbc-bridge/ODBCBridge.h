#pragma once

#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Common/Bridge/IBridge.h>
#include "HandlerFactory.h"

#if USE_ODBC
#    include <Poco/Data/ODBC/Connector.h>
#endif


namespace DB
{

class ODBCBridge : public IBridge
{

protected:
    const std::string bridgeName() const override
    {
        return "ODBCBridge";
    }

    HandlerFactoryPtr getHandlerFactoryPtr(Context & context) const override
    {
        return std::make_shared<ODBCBridgeHandlerFactory>("ODBCRequestHandlerFactory-factory", keep_alive_timeout, context);
    }

    void registerODBCConnector() const override
    {
#if USE_ODBC
        // It doesn't make much sense to build this bridge without ODBC, but we
        // still do this.
        Poco::Data::ODBC::Connector::registerConnector();
#endif
    }

};
}
