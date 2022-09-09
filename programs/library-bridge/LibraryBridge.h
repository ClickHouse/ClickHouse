#pragma once

#include <Interpreters/Context.h>
#include <Bridge/IBridge.h>
#include "HandlerFactory.h"


namespace DB
{

class LibraryBridge : public IBridge
{

protected:
    std::string bridgeName() const override
    {
        return "LibraryBridge";
    }

    HandlerFactoryPtr getHandlerFactoryPtr(ContextPtr context) const override
    {
        return std::make_shared<LibraryBridgeHandlerFactory>("LibraryRequestHandlerFactory-factory", keep_alive_timeout, context);
    }
};

}
