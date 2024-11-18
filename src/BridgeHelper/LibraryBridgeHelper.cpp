#include "LibraryBridgeHelper.h"

#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <IO/ConnectionTimeouts.h>

namespace DB
{
namespace Setting
{
    extern const SettingsSeconds http_receive_timeout;
}
namespace ServerSetting
{
    extern const ServerSettingsSeconds keep_alive_timeout;
}

LibraryBridgeHelper::LibraryBridgeHelper(ContextPtr context_)
    : IBridgeHelper(context_)
    , config(context_->getConfigRef())
    , log(getLogger("LibraryBridgeHelper"))
    , http_timeout(context_->getGlobalContext()->getSettingsRef()[Setting::http_receive_timeout].value)
    , bridge_host(config.getString("library_bridge.host", DEFAULT_HOST))
    , bridge_port(config.getUInt("library_bridge.port", DEFAULT_PORT))
    , http_timeouts(ConnectionTimeouts::getHTTPTimeouts(context_->getSettingsRef(), context_->getServerSettings()))
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
