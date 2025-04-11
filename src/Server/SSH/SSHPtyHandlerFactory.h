#pragma once

#include "config.h"

#if USE_SSH && defined(OS_LINUX)

#include <optional>

#include <Core/ServerSettings.h>
#include <Server/SSH/SSHPtyHandler.h>
#include <Server/TCPServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Server/IServer.h>
#include <Common/LibSSHLogger.h>
#include <Server/SSH/SSHBind.h>
#include <Server/SSH/SSHSession.h>

namespace Poco
{
class Logger;
}
namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class SSHPtyHandlerFactory : public TCPServerConnectionFactory
{
private:
    IServer & server;
    Poco::Logger * log;
    ::ssh::SSHBind ssh_bind;

    /// These defaults are reasonable enough. It doesn't make sense to allow
    /// changing them through the configuration.
    static constexpr size_t MAX_AUTH_ATTEMPTS = 3;
    static constexpr size_t AUTHENTICATION_TIMEOUT_SECONDS = 10;
    static constexpr size_t FINISH_TIMEOUT_SECONDS = 5;
    static constexpr size_t EVENT_POLL_TIMEOUT_MILLISECONDS = 100;

    /// Non static part of the configuration.
    bool enable_client_options_passing = false;

public:
    explicit SSHPtyHandlerFactory(
        IServer & server_, const Poco::Util::AbstractConfiguration & config)
        : server(server_), log(&Poco::Logger::get("SSHHandlerFactory"))
    {
        LOG_INFO(log, "Initializing sshbind");
        ssh_bind.disableDefaultConfig();

        String prefix = "ssh_server.";
        auto rsa_key = config.getString(prefix + "host_rsa_key", "");
        auto ecdsa_key = config.getString(prefix + "host_ecdsa_key", "");
        auto ed25519_key = config.getString(prefix + "host_ed25519_key", "");

        if (rsa_key.empty() && ecdsa_key.empty() && ed25519_key.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Host key for ssh endpoint is not initialized");
        if (!rsa_key.empty())
            ssh_bind.setHostKey(rsa_key);
        if (!ecdsa_key.empty())
            ssh_bind.setHostKey(ecdsa_key);
        if (!ed25519_key.empty())
            ssh_bind.setHostKey(ed25519_key);

        enable_client_options_passing = config.getBool(prefix + "enable_client_options_passing", false);
        if (enable_client_options_passing)
            LOG_WARNING(log, "Client options propagation is enabled. This is considered unsafe and shouldn't be used in production.");
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer &) override
    {
        LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());
        ::ssh::libsshLogger::initialize();
        ::ssh::SSHSession session;
        session.disableSocketOwning();
        session.disableDefaultConfig();
        ssh_bind.acceptFd(session, socket.sockfd());

        auto options = SSHPtyHandler::Options
        {
            .max_auth_attempts = MAX_AUTH_ATTEMPTS,
            .auth_timeout_seconds = AUTHENTICATION_TIMEOUT_SECONDS,
            .finish_timeout_seconds = FINISH_TIMEOUT_SECONDS,
            .event_poll_interval_milliseconds = EVENT_POLL_TIMEOUT_MILLISECONDS,
            .enable_client_options_passing = enable_client_options_passing,
        };

        return new SSHPtyHandler(
            server,
            std::move(session),
            socket,
            options);
    }
};

}

#endif
