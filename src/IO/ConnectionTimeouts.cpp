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

ConnectionTimeouts ConnectionTimeouts::getAdaptiveTimeouts(Aws::Http::HttpMethod method, UInt32 attempt) const
{
    constexpr size_t first_method_index = size_t(Aws::Http::HttpMethod::HTTP_GET);
    constexpr size_t last_method_index = size_t(Aws::Http::HttpMethod::HTTP_PATCH);
    constexpr size_t methods_count = last_method_index - first_method_index + 1;

    /// HTTP_POST is used for CompleteMultipartUpload requests.
    /// These requests need longer timeout, especially when minio is used
    /// The same assumption are made for HTTP_DELETE, HTTP_PATCH
    /// That requests are more heavy that HTTP_GET, HTTP_HEAD, HTTP_PUT

    static const UInt32 first_attempt_send_receive_timeouts_ms[methods_count][2] = {
        /*HTTP_GET*/    {200,   200},
        /*HTTP_POST*/   {200, 30000},
        /*HTTP_DELETE*/ {200,  1000},
        /*HTTP_PUT*/    {200,   200},
        /*HTTP_HEAD*/   {200,   200},
        /*HTTP_PATCH*/  {200,  1000},
    };

    static const UInt32 second_attempt_send_receive_timeouts_ms[methods_count][2] = {
        /*HTTP_GET*/    {1000,  1000},
        /*HTTP_POST*/   {1000, 30000},
        /*HTTP_DELETE*/ {1000, 10000},
        /*HTTP_PUT*/    {1000,  1000},
        /*HTTP_HEAD*/   {1000,  1000},
        /*HTTP_PATCH*/  {1000, 10000},
    };

    static_assert(methods_count == 6);
    static_assert(sizeof(first_attempt_send_receive_timeouts_ms) == sizeof(second_attempt_send_receive_timeouts_ms));
    static_assert(sizeof(first_attempt_send_receive_timeouts_ms) == methods_count * sizeof(UInt32) * 2);

    auto aggressive = *this;

    if (attempt > 2)
        return aggressive;

    auto timeout_map = first_attempt_send_receive_timeouts_ms;
    if (attempt == 2)
        timeout_map = second_attempt_send_receive_timeouts_ms;

    const size_t method_index = size_t(method) - first_method_index;
    aggressive.send_timeout = saturate(Poco::Timespan(timeout_map[method_index][0]), send_timeout);
    aggressive.receive_timeout = saturate(Poco::Timespan(timeout_map[method_index][1]), receive_timeout);

    return aggressive;
}

}
