#include <gtest/gtest.h>

#include <Storages/Kafka/Clients/BrokerConnection.h>
#include <Storages/Kafka/Clients/Protocol/BrokerError.h>
#include <Storages/Kafka/Clients/Protocol/Messages/ApiVersions.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Metadata.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Sasl.h>

#include <Common/config_version.h>

#include "ScriptedTransport.h"

#include <functional>
#include <memory>
#include <optional>

#include <unistd.h>

using namespace Kafka;
using namespace Kafka::Protocol;
using Kafka::Tests::ScriptedTransport;

namespace
{

const auto forever = Net::Deadline::never();

String frameResponse(Int32 correlation_id, bool flexible_header, const std::function<void(Writer &)> & body)
{
    return buildFrame([&](Writer & w)
    {
        ResponseHeader{correlation_id}.encode(w, flexible_header);
        body(w);
    });
}

ApiVersionsResponse defaultApiVersions()
{
    ApiVersionsResponse response;
    response.error_code = 0;
    response.api_keys = {
        {toWire(ApiKey::ApiVersions), 0, 4},
        {toWire(ApiKey::Metadata), 0, 13},
        {toWire(ApiKey::Fetch), 4, 17},
        {toWire(ApiKey::SaslHandshake), 0, 1},
        {toWire(ApiKey::SaslAuthenticate), 0, 2},
    };
    return response;
}

/// correlation_id 1 is always the ApiVersions handshake (response header v0).
String apiVersionsFrame(const ApiVersionsResponse & response = defaultApiVersions())
{
    return frameResponse(1, false, [&](Writer & w) { response.encode(w, 3); });
}

/// The connection logs through options.log unconditionally; the default
/// ConnectionOptions carries no logger, so the test rig supplies one.
ConnectionOptions withTestLogger(ConnectionOptions options)
{
    if (!options.log)
        options.log = getLogger("KafkaClientTest");
    return options;
}

struct Rig
{
    ScriptedTransport * transport;
    BrokerConnection connection;

    explicit Rig(ConnectionOptions options = {})
        : transport(new ScriptedTransport())
        , connection(std::unique_ptr<Net::Transport>(transport), withTestLogger(std::move(options)))
    {
    }
};

}

TEST(KafkaClientConnection, OpenPerformsApiVersionsHandshakeAndFillsNegotiationMap)
{
    Rig rig;
    rig.transport->script(apiVersionsFrame());
    rig.connection.open("broker", 9092, forever);

    EXPECT_EQ(rig.connection.negotiateVersion(ApiKey::Metadata, 12, 12), 12);
    EXPECT_EQ(rig.connection.negotiateVersion(ApiKey::Fetch, 13, 13), 13);

    /// The handshake request on the wire is a well-formed v3 ApiVersions
    /// request with a non-compact client_id.
    const auto frames = rig.transport->writtenFrames();
    ASSERT_EQ(frames.size(), 1);
    Reader r(frames[0]);
    const auto header = RequestHeader::decode(r, true);
    EXPECT_EQ(header.api_key, toWire(ApiKey::ApiVersions));
    EXPECT_EQ(header.api_version, 3);
    EXPECT_EQ(header.correlation_id, 1);
    EXPECT_EQ(header.client_id, std::optional<String>{"ClickHouse"});
    const auto request = ApiVersionsRequest::decode(r, 3);
    EXPECT_EQ(request.client_software_name, VERSION_NAME);
    EXPECT_TRUE(r.empty());
}

TEST(KafkaClientConnection, CallClampsTheVersionAndRoundTripsAMetadataRequest)
{
    Rig rig;
    rig.transport->script(apiVersionsFrame());
    rig.connection.open("broker", 9092, forever);

    MetadataResponse scripted;
    scripted.throttle_time_ms = 0;
    scripted.brokers = {{1, "broker", 9092, std::nullopt}};
    scripted.cluster_id = "cid";
    scripted.controller_id = 1;
    rig.transport->script(frameResponse(2, true, [&](Writer & w) { scripted.encode(w, 12); }));

    MetadataRequest request;
    request.topics = std::vector<MetadataRequestTopic>{{Uuid{}, std::optional<String>{"t"}}};
    const auto response = rig.connection.call(request, forever);
    EXPECT_EQ(response, scripted);

    const auto frames = rig.transport->writtenFrames();
    ASSERT_EQ(frames.size(), 2);
    Reader r(frames[1]);
    const auto header = RequestHeader::decode(r, true);
    EXPECT_EQ(header.api_key, toWire(ApiKey::Metadata));
    EXPECT_EQ(header.api_version, 12); // clamped: we [12,12], broker [0,13]
    EXPECT_EQ(header.correlation_id, 2);
    const auto got = MetadataRequest::decode(r, 12);
    EXPECT_EQ(got, request);
}

TEST(KafkaClientConnection, ResponsesFragmentedIntoSingleBytesStillParse)
{
    Rig rig;
    rig.transport->script(apiVersionsFrame(), 1);
    rig.connection.open("broker", 9092, forever);
    EXPECT_EQ(rig.connection.negotiateVersion(ApiKey::Metadata, 12, 12), 12);
}

TEST(KafkaClientConnection, VersionNegotiationFailuresAreLoudDisjointRanges)
{
    Rig rig;
    auto response = defaultApiVersions();
    response.api_keys[1] = {toWire(ApiKey::Metadata), 0, 11}; // below our min 12
    rig.transport->script(apiVersionsFrame(response));
    rig.connection.open("broker", 9092, forever);

    try
    {
        rig.connection.negotiateVersion(ApiKey::Metadata, 12, 12);
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::NotSupported);
        EXPECT_TRUE(String(e.message()).contains("Metadata"));
        EXPECT_TRUE(String(e.message()).contains("[v0, v11]"));
    }
}

TEST(KafkaClientConnection, VersionNegotiationFailuresAreLoudApiNotAdvertisedAtAll)
{
    Rig rig;
    auto response = defaultApiVersions();
    response.api_keys[1] = {toWire(ApiKey::Metadata), 0, 11}; // below our min 12
    rig.transport->script(apiVersionsFrame(response));
    rig.connection.open("broker", 9092, forever);

    EXPECT_THROW(rig.connection.negotiateVersion(ApiKey::Produce, 9, 9), Exception);
}

TEST(KafkaClientConnection, CorrelationIdMismatchPoisonsTheConnection)
{
    Rig rig;
    rig.transport->script(frameResponse(99, false, [](Writer & w) { defaultApiVersions().encode(w, 3); }));
    try
    {
        rig.connection.open("broker", 9092, forever);
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Protocol);
        EXPECT_TRUE(String(e.message()).contains("Correlation"));
    }
    EXPECT_TRUE(rig.connection.isPoisoned());
    EXPECT_FALSE(rig.connection.isOpen());
}

TEST(KafkaClientConnection, EofMidFrameIsAConnectionError)
{
    Rig rig;
    const String frame = apiVersionsFrame();
    rig.transport->script(frame.substr(0, frame.size() / 2));
    rig.transport->scriptEof();
    try
    {
        rig.connection.open("broker", 9092, forever);
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Connection);
        EXPECT_TRUE(e.isRetriable());
    }
}

TEST(KafkaClientConnection, GarbageSizePrefixPointsAtANonPlaintextListener)
{
    Rig rig;
    rig.transport->script(String("\x7F\x7F\x7F\x7F more-garbage", 18));
    try
    {
        rig.connection.open("broker", 9092, forever);
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Protocol);
        EXPECT_TRUE(String(e.message()).contains("PLAINTEXT"));
    }
}

TEST(KafkaClientConnection, BrokerRejectingApiVersionsV3YieldsAClearNotSupported)
{
    Rig rig;
    ApiVersionsResponse response;
    response.error_code = BrokerError::UNSUPPORTED_VERSION;
    rig.transport->script(apiVersionsFrame(response));
    try
    {
        rig.connection.open("broker", 9092, forever);
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::NotSupported);
        EXPECT_TRUE(String(e.message()).contains("older"));
    }
}

TEST(KafkaClientConnection, SaslPlainAuthenticatesAfterTheHandshake)
{
    ConnectionOptions options;
    options.client_id = "c";
    options.sasl = SaslPlainConfig{.username = "user", .password = "secret", .authorization_id = ""};
    Rig rig(std::move(options));

    rig.transport->script(apiVersionsFrame());
    /// SaslHandshake v1 is non-flexible: response header v0.
    SaslHandshakeResponse handshake;
    handshake.error_code = 0;
    handshake.mechanisms = {"PLAIN"};
    rig.transport->script(frameResponse(2, false, [&](Writer & w) { handshake.encode(w, 1); }));
    SaslAuthenticateResponse authenticate;
    authenticate.error_code = 0;
    rig.transport->script(frameResponse(3, true, [&](Writer & w) { authenticate.encode(w, 2); }));

    rig.connection.open("broker", 9092, forever);

    const auto frames = rig.transport->writtenFrames();
    ASSERT_EQ(frames.size(), 3);
    /// Frame 2: handshake with header v1 (non-flexible).
    {
        Reader r(frames[1]);
        const auto header = RequestHeader::decode(r, false);
        EXPECT_EQ(header.api_key, toWire(ApiKey::SaslHandshake));
        const auto request = SaslHandshakeRequest::decode(r, 1);
        EXPECT_EQ(request.mechanism, "PLAIN");
        EXPECT_TRUE(r.empty());
    }
    /// Frame 3: authenticate carries authzid \0 user \0 password.
    {
        Reader r(frames[2]);
        RequestHeader::decode(r, true);
        const auto request = SaslAuthenticateRequest::decode(r, 2);
        EXPECT_EQ(request.auth_bytes, String("\0user\0secret", 12));
    }
}

TEST(KafkaClientConnection, SaslMechanismRejectionNamesTheBrokersOffer)
{
    ConnectionOptions options;
    options.sasl = SaslPlainConfig{.username = "u", .password = "p", .authorization_id = ""};
    Rig rig(std::move(options));

    rig.transport->script(apiVersionsFrame());
    SaslHandshakeResponse handshake;
    handshake.error_code = BrokerError::UNSUPPORTED_SASL_MECHANISM;
    handshake.mechanisms = {"SCRAM-SHA-256"};
    rig.transport->script(frameResponse(2, false, [&](Writer & w) { handshake.encode(w, 1); }));

    try
    {
        rig.connection.open("broker", 9092, forever);
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Broker);
        EXPECT_EQ(e.getBrokerCode(), BrokerError::UNSUPPORTED_SASL_MECHANISM);
        EXPECT_TRUE(String(e.message()).contains("SCRAM-SHA-256"));
    }
}

TEST(KafkaClientConnection, WakeupSourceBasics)
{
    Net::WakeupSource wakeup;
    EXPECT_EQ(wakeup.getGeneration(), 0);
    wakeup.wakeup();
    EXPECT_EQ(wakeup.getGeneration(), 1);
    /// Pipe is readable until drained.
    char buf[8];
    wakeup.drain();
    EXPECT_LT(::read(wakeup.getFd(), buf, sizeof(buf)), 0); // empty after drain (EAGAIN)
}
