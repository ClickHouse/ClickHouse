#pragma once

#include "config.h"

#if USE_SSH

#include <optional>

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
    ::ssh::SSHBind bind;
    unsigned int max_auth_attempts;
    unsigned int auth_timeout_seconds;
    unsigned int finish_timeout_seconds;
    unsigned int event_poll_interval_milliseconds;
    std::optional<int> read_write_timeout_seconds; // optional here, as libssh has its own defaults
    std::optional<int> read_write_timeout_micro_seconds;

public:
    explicit SSHPtyHandlerFactory(
        IServer & server_, const Poco::Util::AbstractConfiguration & config)
        : server(server_), log(&Poco::Logger::get("SSHHandlerFactory"))
    {
        LOG_INFO(log, "Initializing sshbind");
        bind.disableDefaultConfig();


        String prefix = "ssh.";
        auto rsa_key = config.getString(prefix + "host_rsa_key", "");
        auto ecdsa_key = config.getString(prefix + "host_ecdsa_key", "");
        auto ed25519_key = config.getString(prefix + "host_ed25519_key", "");
        max_auth_attempts = config.getUInt("max_auth_attempts", 4);
        auth_timeout_seconds = config.getUInt("auth_timeout_seconds", 10);
        finish_timeout_seconds = config.getUInt("finish_timeout_seconds", 5);
        event_poll_interval_milliseconds = config.getUInt("event_poll_interval_milliseconds", 100);
        if (config.has("read_write_timeout_seconds"))
        {
            read_write_timeout_seconds = config.getInt("read_write_timeout_seconds");
            if (read_write_timeout_seconds.value() < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Negative timeout specified");
        }
        if (config.has("read_write_timeout_micro_seconds"))
        {

            read_write_timeout_micro_seconds = config.getInt("read_write_timeout_micro_seconds");
            if (read_write_timeout_micro_seconds.value() < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Negative timeout specified");
        }

        if (event_poll_interval_milliseconds == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Poll interval must be positive");
        if (auth_timeout_seconds * 1000 < event_poll_interval_milliseconds)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Poll interval exceeds auth timeout");
        if (finish_timeout_seconds * 1000 < event_poll_interval_milliseconds)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Poll interval exceeds finish timeout");


        if (rsa_key.empty() && ecdsa_key.empty() && ed25519_key.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Host key for ssh endpoint is not initialized");
        if (!rsa_key.empty())
            bind.setHostKey(rsa_key);
        if (!ecdsa_key.empty())
            bind.setHostKey(ecdsa_key);
        if (!ed25519_key.empty())
            bind.setHostKey(ed25519_key);
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer &) override
    {
        LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());
        ::ssh::libsshLogger::initialize();
        ::ssh::SSHSession session;
        session.disableSocketOwning();
        session.disableDefaultConfig();
        if (read_write_timeout_seconds.has_value() || read_write_timeout_micro_seconds.has_value())
        {
            session.setTimeout(read_write_timeout_seconds.value_or(0), read_write_timeout_micro_seconds.value_or(0));
        }
        bind.acceptFd(session, socket.sockfd());

        return new SSHPtyHandler(
            server,
            std::move(session),
            socket,
            max_auth_attempts,
            auth_timeout_seconds,
            finish_timeout_seconds,
            event_poll_interval_milliseconds);
    }
};

}

#endif
