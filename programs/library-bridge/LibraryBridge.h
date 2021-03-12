#pragma once

#include <Interpreters/Context.h>
#include <bridge/IBridge.h>
#include "HandlerFactory.h"


namespace DB
{

class LibraryBridge : public IBridge
{

protected:
    const std::string bridgeName() const override
    {
        return "LibraryBridge";
    }

    HandlerFactoryPtr getHandlerFactoryPtr(Context & context) const override
    {
        return std::make_shared<LibraryBridgeHandlerFactory>("LibraryRequestHandlerFactory-factory", keep_alive_timeout, context);
    }
};

}
