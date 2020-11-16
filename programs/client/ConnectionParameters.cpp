#include "ConnectionParameters.h"
#include <fstream>
#include <iostream>
#include <Core/Defines.h>
#include <Core/Protocol.h>
#include <Core/Types.h>
#include <IO/ConnectionTimeouts.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <common/setTerminalEcho.h>
#include <ext/scope_guard.h>

#if !defined(ARCADIA_BUILD)
#include <readpassphrase.h> // Y_IGNORE
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ConnectionParameters::ConnectionParameters(const Poco::Util::AbstractConfiguration & config)
{
    bool is_secure = config.getBool("secure", false);
    security = is_secure ? Protocol::Secure::Enable : Protocol::Secure::Disable;

    host = config.getString("host", "localhost");
    port = config.getInt(
        "port", config.getInt(is_secure ? "tcp_port_secure" : "tcp_port", is_secure ? DBMS_DEFAULT_SECURE_PORT : DBMS_DEFAULT_PORT));

    default_database = config.getString("database", "");

    /// changed the default value to "default" to fix the issue when the user in the prompt is blank
    user = config.getString("user", "default");

    bool password_prompt = false;
    if (config.getBool("ask-password", false))
    {
        if (config.has("password"))
            throw Exception("Specified both --password and --ask-password. Remove one of them", ErrorCodes::BAD_ARGUMENTS);
        password_prompt = true;
    }
    else
    {
        password = config.getString("password", "");
        /// if the value of --password is omitted, the password will be set implicitly to "\n"
        if (password == "\n")
            password_prompt = true;
    }
    if (password_prompt)
    {
#if !defined(ARCADIA_BUILD)
        std::string prompt{"Password for user (" + user + "): "};
        char buf[1000] = {};
        if (auto * result = readpassphrase(prompt.c_str(), buf, sizeof(buf), 0))
            password = result;
#endif
    }

    compression = config.getBool("compression", true) ? Protocol::Compression::Enable : Protocol::Compression::Disable;

    timeouts = ConnectionTimeouts(
        Poco::Timespan(config.getInt("connect_timeout", DBMS_DEFAULT_CONNECT_TIMEOUT_SEC), 0),
        Poco::Timespan(config.getInt("send_timeout", DBMS_DEFAULT_SEND_TIMEOUT_SEC), 0),
        Poco::Timespan(config.getInt("receive_timeout", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC), 0),
        Poco::Timespan(config.getInt("tcp_keep_alive_timeout", 0), 0));
}
}
