#include "LibraryBridgeHelper.h"

namespace DB
{

LibraryBridgeHelper::LibraryBridgeHelper(ContextPtr context_)
    : IBridgeHelper(context_)
    , config(context_->getConfigRef())
    , log(&Poco::Logger::get("LibraryBridgeHelper"))
    , http_timeout(context_->getGlobalContext()->getSettingsRef().http_receive_timeout.value)
    , bridge_host(config.getString("library_bridge.host", DEFAULT_HOST))
    , bridge_port(config.getUInt("library_bridge.port", DEFAULT_PORT))
    , http_timeouts(ConnectionTimeouts::getHTTPTimeouts(context_))
{
}


void LibraryBridgeHelper::startBridge(std::unique_ptr<ShellCommand> cmd) const
{
    getContext()->addBridgeCommand(std::move(cmd));
}


Poco::URI LibraryBridgeHelper::createBaseURI() const
{
    Poco::URI uri;
    uri.setHost(bridge_host);
    uri.setPort(bridge_port);
    uri.setScheme("http");
    return uri;
}


}
