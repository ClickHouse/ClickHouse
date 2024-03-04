#include <IO/ConnectionTimeouts.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context.h>

namespace DB
{

ConnectionTimeouts::ConnectionTimeouts(
    Poco::Timespan connection_timeout_,
    Poco::Timespan send_timeout_,
    Poco::Timespan receive_timeout_)
    : connection_timeout(connection_timeout_)
    , send_timeout(send_timeout_)
    , receive_timeout(receive_timeout_)
    , tcp_keep_alive_timeout(0)
    , http_keep_alive_timeout(0)
    , secure_connection_timeout(connection_timeout)
    , hedged_connection_timeout(receive_timeout_)
    , receive_data_timeout(receive_timeout_)
    , handshake_timeout(receive_timeout_)
{
}

ConnectionTimeouts::ConnectionTimeouts(
    Poco::Timespan connection_timeout_,
    Poco::Timespan send_timeout_,
    Poco::Timespan receive_timeout_,
    Poco::Timespan tcp_keep_alive_timeout_,
    Poco::Timespan handshake_timeout_)
    : connection_timeout(connection_timeout_)
    , send_timeout(send_timeout_)
    , receive_timeout(receive_timeout_)
    , tcp_keep_alive_timeout(tcp_keep_alive_timeout_)
    , http_keep_alive_timeout(0)
    , secure_connection_timeout(connection_timeout)
    , hedged_connection_timeout(receive_timeout_)
    , receive_data_timeout(receive_timeout_)
    , handshake_timeout(handshake_timeout_)
{
}

ConnectionTimeouts::ConnectionTimeouts(
    Poco::Timespan connection_timeout_,
    Poco::Timespan send_timeout_,
    Poco::Timespan receive_timeout_,
    Poco::Timespan tcp_keep_alive_timeout_,
    Poco::Timespan http_keep_alive_timeout_,
    Poco::Timespan handshake_timeout_)
    : connection_timeout(connection_timeout_)
    , send_timeout(send_timeout_)
    , receive_timeout(receive_timeout_)
    , tcp_keep_alive_timeout(tcp_keep_alive_timeout_)
    , http_keep_alive_timeout(http_keep_alive_timeout_)
    , secure_connection_timeout(connection_timeout)
    , hedged_connection_timeout(receive_timeout_)
    , receive_data_timeout(receive_timeout_)
    , handshake_timeout(handshake_timeout_)
{
}

ConnectionTimeouts::ConnectionTimeouts(
    Poco::Timespan connection_timeout_,
    Poco::Timespan send_timeout_,
    Poco::Timespan receive_timeout_,
    Poco::Timespan tcp_keep_alive_timeout_,
    Poco::Timespan http_keep_alive_timeout_,
    Poco::Timespan secure_connection_timeout_,
    Poco::Timespan hedged_connection_timeout_,
    Poco::Timespan receive_data_timeout_,
    Poco::Timespan handshake_timeout_)
    : connection_timeout(connection_timeout_)
    , send_timeout(send_timeout_)
    , receive_timeout(receive_timeout_)
    , tcp_keep_alive_timeout(tcp_keep_alive_timeout_)
    , http_keep_alive_timeout(http_keep_alive_timeout_)
    , secure_connection_timeout(secure_connection_timeout_)
    , hedged_connection_timeout(hedged_connection_timeout_)
    , receive_data_timeout(receive_data_timeout_)
    , handshake_timeout(handshake_timeout_)
{
}

Poco::Timespan ConnectionTimeouts::saturate(Poco::Timespan timespan, Poco::Timespan limit)
{
    if (limit.totalMicroseconds() == 0)
        return timespan;
    else
        return (timespan > limit) ? limit : timespan;
}

ConnectionTimeouts ConnectionTimeouts::getSaturated(Poco::Timespan limit) const
{
    return ConnectionTimeouts(saturate(connection_timeout, limit),
                              saturate(send_timeout, limit),
                              saturate(receive_timeout, limit),
                              saturate(tcp_keep_alive_timeout, limit),
                              saturate(http_keep_alive_timeout, limit),
                              saturate(secure_connection_timeout, limit),
                              saturate(hedged_connection_timeout, limit),
                              saturate(receive_data_timeout, limit),
                              saturate(handshake_timeout, limit));
}

/// Timeouts for the case when we have just single attempt to connect.
ConnectionTimeouts ConnectionTimeouts::getTCPTimeoutsWithoutFailover(const Settings & settings)
{
    return ConnectionTimeouts(settings.connect_timeout, settings.send_timeout, settings.receive_timeout, settings.tcp_keep_alive_timeout, settings.handshake_timeout_ms);
}

/// Timeouts for the case when we will try many addresses in a loop.
ConnectionTimeouts ConnectionTimeouts::getTCPTimeoutsWithFailover(const Settings & settings)
{
    return ConnectionTimeouts(
        settings.connect_timeout_with_failover_ms,
        settings.send_timeout,
        settings.receive_timeout,
        settings.tcp_keep_alive_timeout,
        0,
        settings.connect_timeout_with_failover_secure_ms,
        settings.hedged_connection_timeout_ms,
        settings.receive_data_timeout_ms,
        settings.handshake_timeout_ms);
}

ConnectionTimeouts ConnectionTimeouts::getHTTPTimeouts(const Settings & settings, Poco::Timespan http_keep_alive_timeout)
{
    return ConnectionTimeouts(
        settings.http_connection_timeout,
        settings.http_send_timeout,
        settings.http_receive_timeout,
        settings.tcp_keep_alive_timeout,
        http_keep_alive_timeout,
        settings.http_receive_timeout);
}

ConnectionTimeouts ConnectionTimeouts::getFetchPartHTTPTimeouts(const ServerSettings & server_settings, const Settings & user_settings)
{
    auto timeouts = getHTTPTimeouts(user_settings, server_settings.keep_alive_timeout);

    if (server_settings.replicated_fetches_http_connection_timeout.changed)
        timeouts.connection_timeout = server_settings.replicated_fetches_http_connection_timeout;

    if (server_settings.replicated_fetches_http_send_timeout.changed)
        timeouts.send_timeout = server_settings.replicated_fetches_http_send_timeout;

    if (server_settings.replicated_fetches_http_receive_timeout.changed)
        timeouts.receive_timeout = server_settings.replicated_fetches_http_receive_timeout;

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

    auto aggressive = *this;
    aggressive.send_timeout = saturate(send, send_timeout);
    aggressive.receive_timeout = saturate(recv, receive_timeout);

    return aggressive;
}

}
