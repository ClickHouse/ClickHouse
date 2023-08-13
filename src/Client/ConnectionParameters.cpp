#include "ConnectionParameters.h"
#include <fstream>
#include <Core/Defines.h>
#include <Core/Protocol.h>
#include <Core/Types.h>
#include <IO/ConnectionTimeouts.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/isLocalAddress.h>
#include <Common/DNSResolver.h>
#include <base/scope_guard.h>

#include <readpassphrase/readpassphrase.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ConnectionParameters::ConnectionParameters(const Poco::Util::AbstractConfiguration & config,
                                           std::string connection_host,
                                           std::optional<UInt16> connection_port)
    : host(connection_host)
    , port(connection_port.value_or(getPortFromConfig(config)))
{
    bool is_secure = config.getBool("secure", false);
    security = is_secure ? Protocol::Secure::Enable : Protocol::Secure::Disable;

    default_database = config.getString("database", "");

    /// changed the default value to "default" to fix the issue when the user in the prompt is blank
    user = config.getString("user", "default");

    bool password_prompt = false;
    if (config.getBool("ask-password", false))
    {
        if (config.has("password"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Specified both --password and --ask-password. Remove one of them");
        password_prompt = true;
    }
    else
    {
        password = config.getString("password", "");
        if (password == ASK_PASSWORD)
            password_prompt = true;
    }
    if (password_prompt)
    {
        std::string prompt{"Password for user (" + user + "): "};
        char buf[1000] = {};
        if (auto * result = readpassphrase(prompt.c_str(), buf, sizeof(buf), 0))
            password = result;
    }
    quota_key = config.getString("quota_key", "");

    /// By default compression is disabled if address looks like localhost.

    /// Avoid DNS request if the host is "localhost".
    /// If ClickHouse is run under QEMU-user with a binary for a different architecture,
    /// and there are all listed startup dependency shared libraries available, but not the runtime dependencies of glibc,
    /// the glibc cannot open "plugins" for DNS resolving, and the DNS resolution does not work.
    /// At the same time, I want clickhouse-local to always work, regardless.
    /// TODO: get rid of glibc, or replace getaddrinfo to c-ares.

    compression = config.getBool("compression", host != "localhost" && !isLocalAddress(DNSResolver::instance().resolveHost(host)))
                  ? Protocol::Compression::Enable : Protocol::Compression::Disable;

    timeouts = ConnectionTimeouts(
            Poco::Timespan(config.getInt("connect_timeout", DBMS_DEFAULT_CONNECT_TIMEOUT_SEC), 0),
            Poco::Timespan(config.getInt("send_timeout", DBMS_DEFAULT_SEND_TIMEOUT_SEC), 0),
            Poco::Timespan(config.getInt("receive_timeout", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC), 0),
            Poco::Timespan(config.getInt("tcp_keep_alive_timeout", 0), 0),
            Poco::Timespan(config.getInt("handshake_timeout_ms", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC * 1000), 0));

    timeouts.sync_request_timeout = Poco::Timespan(config.getInt("sync_request_timeout", DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC), 0);
}

ConnectionParameters::ConnectionParameters(const Poco::Util::AbstractConfiguration & config)
    : ConnectionParameters(config, config.getString("host", "localhost"), getPortFromConfig(config))
{
}

UInt16 ConnectionParameters::getPortFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    bool is_secure = config.getBool("secure", false);
    return config.getInt("port",
        config.getInt(is_secure ? "tcp_port_secure" : "tcp_port",
            is_secure ? DBMS_DEFAULT_SECURE_PORT : DBMS_DEFAULT_PORT));
}
}
