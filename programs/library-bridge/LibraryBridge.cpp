#include "LibraryBridge.h"

#include <iostream>

int mainEntryClickHouseLibraryBridge(int argc, char ** argv)
{
    DB::LibraryBridge app;
    try
    {
        return app.run(argc, argv);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}

namespace DB
{

std::string LibraryBridge::bridgeName() const
{
    return "LibraryBridge";
}

LibraryBridge::HandlerFactoryPtr LibraryBridge::getHandlerFactoryPtr(ContextPtr context) const
{
    return std::make_shared<LibraryBridgeHandlerFactory>("LibraryRequestHandlerFactory", context);
}

}
