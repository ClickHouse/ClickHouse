#pragma once

#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <bridge/IBridge.h>
#include "HandlerFactory.h"


namespace DB
{

class ODBCBridge : public IBridge
{

protected:
    std::string bridgeName() const override
    {
        return "ODBCBridge";
    }

    HandlerFactoryPtr getHandlerFactoryPtr(ContextPtr context) const override
    {
        return std::make_shared<ODBCBridgeHandlerFactory>("ODBCRequestHandlerFactory-factory", keep_alive_timeout, context);
    }
};
}
