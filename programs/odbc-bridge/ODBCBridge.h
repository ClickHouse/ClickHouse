#pragma once

#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Bridge/IBridge.h>
#include "ODBCHandlerFactory.h"


namespace DB
{

class ODBCBridge : public IBridge
{

protected:
    std::string bridgeName() const override;
    HandlerFactoryPtr getHandlerFactoryPtr(ContextPtr context) const override;
};
}
