#pragma once

#include <Interpreters/Context_fwd.h>
#include <Bridge/IBridge.h>
#include "LibraryBridgeHandlerFactory.h"


namespace DB
{

class LibraryBridge : public IBridge
{

protected:
    std::string bridgeName() const override;
    HandlerFactoryPtr getHandlerFactoryPtr(ContextPtr context) const override;
};

}
