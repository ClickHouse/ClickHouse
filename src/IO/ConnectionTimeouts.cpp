#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <IO/ConnectionTimeouts.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace Setting
{
    extern const SettingsSeconds connect_timeout;
    extern const SettingsSeconds send_timeout;
    extern const SettingsSeconds receive_timeout;
    extern const SettingsSeconds tcp_keep_alive_timeout;
    extern const SettingsMilliseconds handshake_timeout_ms;
    extern const SettingsMilliseconds hedged_connection_timeout_ms;
    extern const SettingsMilliseconds receive_data_timeout_ms;
    extern const SettingsMilliseconds connect_timeout_with_failover_ms;
    extern const SettingsMilliseconds connect_timeout_with_failover_secure_ms;
    extern const SettingsSeconds http_connection_timeout;
    extern const SettingsSeconds http_send_timeout;
    extern const SettingsSeconds http_receive_timeout;
}

namespace ServerSetting
{
    extern const ServerSettingsSeconds keep_alive_timeout;
    extern const ServerSettingsSeconds replicated_fetches_http_connection_timeout;
    extern const ServerSettingsSeconds replicated_fetches_http_receive_timeout;
    extern const ServerSettingsSeconds replicated_fetches_http_send_timeout;
}

Poco::Timespan ConnectionTimeouts::saturate(Poco::Timespan timespan, Poco::Timespan limit)
{
    if (limit.totalMicroseconds() == 0)
        return timespan;
    return (timespan > limit) ? limit : timespan;
}

/// Timeouts for the case when we have just single attempt to connect.
ConnectionTimeouts ConnectionTimeouts::getTCPTimeoutsWithoutFailover(const Settings & settings)
{
    return ConnectionTimeouts()
        .withConnectionTimeout(settings[Setting::connect_timeout])
        .withSendTimeout(settings[Setting::send_timeout])
        .withReceiveTimeout(settings[Setting::receive_timeout])
        .withTCPKeepAliveTimeout(settings[Setting::tcp_keep_alive_timeout])
        .withHandshakeTimeout(settings[Setting::handshake_timeout_ms])
        .withHedgedConnectionTimeout(settings[Setting::hedged_connection_timeout_ms])
        .withReceiveDataTimeout(settings[Setting::receive_data_timeout_ms]);
}

/// Timeouts for the case when we will try many addresses in a loop.
ConnectionTimeouts ConnectionTimeouts::getTCPTimeoutsWithFailover(const Settings & settings)
{
    return getTCPTimeoutsWithoutFailover(settings)
        .withUnsecureConnectionTimeout(settings[Setting::connect_timeout_with_failover_ms])
        .withSecureConnectionTimeout(settings[Setting::connect_timeout_with_failover_secure_ms]);
}

ConnectionTimeouts ConnectionTimeouts::getHTTPTimeouts(const Settings & settings, const ServerSettings & server_settings)
{
    return ConnectionTimeouts()
        .withConnectionTimeout(settings[Setting::http_connection_timeout])
        .withSendTimeout(settings[Setting::http_send_timeout])
        .withReceiveTimeout(settings[Setting::http_receive_timeout])
        .withHTTPKeepAliveTimeout(server_settings[ServerSetting::keep_alive_timeout])
        .withTCPKeepAliveTimeout(settings[Setting::tcp_keep_alive_timeout])
        .withHandshakeTimeout(settings[Setting::handshake_timeout_ms]);
}

ConnectionTimeouts ConnectionTimeouts::getFetchPartHTTPTimeouts(const ServerSettings & server_settings, const Settings & user_settings)
{
    auto timeouts = getHTTPTimeouts(user_settings, server_settings);

    if (server_settings[ServerSetting::replicated_fetches_http_connection_timeout].changed)
        timeouts.connection_timeout = server_settings[ServerSetting::replicated_fetches_http_connection_timeout];

    if (server_settings[ServerSetting::replicated_fetches_http_send_timeout].changed)
        timeouts.send_timeout = server_settings[ServerSetting::replicated_fetches_http_send_timeout];

    if (server_settings[ServerSetting::replicated_fetches_http_receive_timeout].changed)
        timeouts.receive_timeout = server_settings[ServerSetting::replicated_fetches_http_receive_timeout];

    return timeouts;
}

class SendReceiveTimeoutsForFirstAttempt
{
private:
    static constexpr size_t known_methods_count = 6;
    using KnownMethodsArray = std::array<String, known_methods_count>;
    static const KnownMethodsArray known_methods;

    /// HTTP_POST is used for CompleteMultipartUpload requests. Its latency could be high.
    /// These requests need longer timeout, especially when minio is used.
    /// The same assumption are made for HTTP_DELETE, HTTP_PATCH
    /// That requests are more heavy that HTTP_GET, HTTP_HEAD, HTTP_PUT

    static constexpr Poco::Timestamp::TimeDiff first_byte_ms[known_methods_count][2] =
    {
        /* GET */ {200, 200},
        /* POST */ {200, 200},
        /* DELETE */ {200, 200},
        /* PUT */ {200, 200},
        /* HEAD */ {200, 200},
        /* PATCH */ {200, 200},
    };

    static constexpr Poco::Timestamp::TimeDiff rest_bytes_ms[known_methods_count][2] =
    {
        /* GET */ {500, 500},
        /* POST */ {1000, 30000},
        /* DELETE */ {1000, 10000},
        /* PUT */ {1000, 3000},
        /* HEAD */ {500, 500},
        /* PATCH */ {1000, 10000},
    };

    static_assert(sizeof(first_byte_ms) == sizeof(rest_bytes_ms));
    static_assert(sizeof(first_byte_ms) == known_methods_count * sizeof(Poco::Timestamp::TimeDiff) * 2);

    static size_t getMethodIndex(const String & method)
    {
        KnownMethodsArray::const_iterator it = std::find(known_methods.begin(), known_methods.end(), method);
        chassert(it != known_methods.end());
        if (it == known_methods.end())
            return 0;
        return std::distance(known_methods.begin(), it);
    }

public:
    static std::pair<Poco::Timespan, Poco::Timespan> getSendReceiveTimeout(const String & method, bool first_byte)
    {
        auto idx = getMethodIndex(method);

        if (first_byte)
            return std::make_pair(
                Poco::Timespan(first_byte_ms[idx][0] * 1000),
                Poco::Timespan(first_byte_ms[idx][1] * 1000)
            );

        return std::make_pair(
            Poco::Timespan(rest_bytes_ms[idx][0] * 1000),
            Poco::Timespan(rest_bytes_ms[idx][1] * 1000)
        );
    }
};

const SendReceiveTimeoutsForFirstAttempt::KnownMethodsArray SendReceiveTimeoutsForFirstAttempt::known_methods =
{
        "GET", "POST", "DELETE", "PUT", "HEAD", "PATCH"
};


ConnectionTimeouts ConnectionTimeouts::getAdaptiveTimeouts(const String & method, bool first_attempt, bool first_byte) const
{
    if (!first_attempt)
        return *this;

    auto [send, recv] = SendReceiveTimeoutsForFirstAttempt::getSendReceiveTimeout(method, first_byte);

    return ConnectionTimeouts(*this)
        .withSendTimeout(saturate(send, send_timeout))
        .withReceiveTimeout(saturate(recv, receive_timeout));
}

void setTimeouts(Poco::Net::HTTPClientSession & session, const ConnectionTimeouts & timeouts)
{
    session.setTimeout(timeouts.connection_timeout, timeouts.send_timeout, timeouts.receive_timeout);
    /// we can not change keep alive timeout for already initiated connections
    if (!session.connected())
    {
        session.setKeepAliveTimeout(timeouts.http_keep_alive_timeout);
        session.setKeepAliveMaxRequests(int(timeouts.http_keep_alive_max_requests));
    }
}

ConnectionTimeouts getTimeouts(const Poco::Net::HTTPClientSession & session)
{
    return ConnectionTimeouts()
            .withConnectionTimeout(session.getConnectionTimeout())
            .withSendTimeout(session.getSendTimeout())
            .withReceiveTimeout(session.getReceiveTimeout())
            .withHTTPKeepAliveTimeout(session.getKeepAliveTimeout());
}

}
