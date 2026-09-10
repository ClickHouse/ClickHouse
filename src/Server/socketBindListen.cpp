#include <Server/socketBindListen.h>

#include <Core/ServerSettings.h>
#include <Common/makeSocketAddress.h>

namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsUInt32 listen_backlog;
    extern const ServerSettingsBool listen_reuse_port;
}

Poco::Net::SocketAddress socketBindListen(
    const ServerSettings & server_settings,
    Poco::Net::ServerSocket & socket,
    const std::string & host,
    UInt16 port,
    LoggerRawPtr log)
{
    auto address = makeSocketAddress(host, port, log);
    socket.bind(address, /* reuseAddress= */ true, /* reusePort= */ server_settings[ServerSetting::listen_reuse_port]);
    /// If caller requests any available port from the OS, discover it after binding.
    if (port == 0)
    {
        address = socket.address();
        LOG_DEBUG(log, "Requested any available port (port == 0), actual port is {:d}", address.port());
    }

    socket.listen(/* backlog= */ server_settings[ServerSetting::listen_backlog]);

    return address;
}

}
