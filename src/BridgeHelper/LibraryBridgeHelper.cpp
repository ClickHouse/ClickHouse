#include <BridgeHelper/LibraryBridgeHelper.h>

#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Common/ShellCommandsHolder.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
}

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
    , http_timeout(context_->getGlobalContext()->getSettingsRef()[Setting::http_receive_timeout])
    , bridge_host(config.getString("library_bridge.host", DEFAULT_HOST))
    , bridge_port(config.getUInt("library_bridge.port", DEFAULT_PORT))
    , http_timeouts(ConnectionTimeouts::getHTTPTimeouts(context_->getSettingsRef(), context_->getServerSettings()))
{
}


void LibraryBridgeHelper::startBridge(std::unique_ptr<ShellCommand> cmd) const
{
    ShellCommandsHolder::instance().addCommand(std::move(cmd));
}


std::optional<String> LibraryBridgeHelper::getLibrariesSandboxPath() const
{
    /// The only remaining user of `clickhouse-library-bridge` is `catboostEvaluate`, which loads the CatBoost
    /// shared library configured in `catboost_lib_path`. The bridge `dlopen`s it, so it must always be
    /// restricted to that path. Refuse to start an unrestricted bridge - `system.models` and
    /// `SYSTEM RELOAD MODELS` can reach this code even when CatBoost is not configured at all.
    if (!config.has("catboost_lib_path"))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "The `catboost_lib_path` setting is not configured, so {} cannot be started: "
            "it is the only path the bridge is allowed to load shared libraries from", serviceAlias());

    return config.getString("catboost_lib_path");
}


Poco::URI LibraryBridgeHelper::createBaseURI() const
{
    Poco::URI uri;
    uri.setHost(bridge_host);
    uri.setPort(static_cast<uint16_t>(bridge_port));
    uri.setScheme("http");
    return uri;
}


}
