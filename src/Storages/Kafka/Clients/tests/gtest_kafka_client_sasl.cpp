#include <Storages/Kafka/Clients/Error.h>
#include <Storages/Kafka/Clients/SaslAuthentication.h>

#include "config.h"

#include <gtest/gtest.h>

#include <base/types.h>

#include <memory>
#include <optional>

using Kafka::Bytes;
using Kafka::createSaslMechanism;
using Kafka::createScramMechanism;
using Kafka::ErrorKind;
using Kafka::Exception;
using Kafka::OAuthToken;
using Kafka::SaslConfig;
using Kafka::SaslOauthBearerConfig;
using Kafka::SaslPlainConfig;
using Kafka::SaslScramConfig;

TEST(KafkaClientSasl, PlainFirstMessageLayout)
{
    auto mechanism = createSaslMechanism(SaslConfig{SaslPlainConfig{.username = "user", .password = "pass", .authorization_id = {}}}, "broker.test");
    EXPECT_STREQ(mechanism->name(), "PLAIN");
    EXPECT_EQ(mechanism->firstMessage(), Bytes("\0user\0pass", 10));
    /// PLAIN is one round: the server's response carries no data.
    EXPECT_EQ(mechanism->nextMessage({}), std::nullopt);
}

TEST(KafkaClientSasl, PlainFirstMessageLayoutWithAuthorizationId)
{
    auto mechanism
        = createSaslMechanism(SaslConfig{SaslPlainConfig{.username = "user", .password = "pass", .authorization_id = "admin"}}, "broker.test");
    EXPECT_EQ(mechanism->firstMessage(), Bytes("admin\0user\0pass", 15));
    EXPECT_EQ(mechanism->nextMessage({}), std::nullopt);
}

TEST(KafkaClientSasl, OauthBearerFirstMessageLayout)
{
    SaslOauthBearerConfig config;
    config.token_provider = [] { return OAuthToken{.value = "tok123", .expiry = {}}; };
    auto mechanism = createSaslMechanism(SaslConfig{config}, "broker.test");
    EXPECT_STREQ(mechanism->name(), "OAUTHBEARER");
    EXPECT_EQ(mechanism->firstMessage(), Bytes("n,,\x01" "auth=Bearer tok123" "\x01\x01"));
    /// An empty server response means success.
    EXPECT_EQ(mechanism->nextMessage({}), std::nullopt);
}

TEST(KafkaClientSasl, OauthBearerAnswersTheErrorChallengeWithADummyByte)
{
    SaslOauthBearerConfig config;
    config.token_provider = [] { return OAuthToken{.value = "tok123", .expiry = {}}; };
    auto mechanism = createSaslMechanism(SaslConfig{config}, "broker.test");
    mechanism->firstMessage();

    /// A non-empty response is the RFC 7628 JSON error challenge; the client
    /// must answer with a single 0x01 byte.
    const auto dummy = mechanism->nextMessage(R"({"status":"invalid_token"})");
    ASSERT_TRUE(dummy.has_value());
    EXPECT_EQ(*dummy, Bytes(1, '\x01'));

    /// The server must fail the exchange after the dummy byte, not keep talking.
    try
    {
        mechanism->nextMessage({});
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Protocol);
        EXPECT_TRUE(String(e.message()).contains("invalid_token"));
    }
}

TEST(KafkaClientSasl, OauthBearerRequiresATokenProviderAndANonEmptyToken)
{
    auto without_provider = createSaslMechanism(SaslConfig{SaslOauthBearerConfig{}}, "broker.test");
    try
    {
        without_provider->firstMessage();
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Config);
    }

    SaslOauthBearerConfig empty_token;
    empty_token.token_provider = [] { return OAuthToken{}; };
    auto with_empty_token = createSaslMechanism(SaslConfig{empty_token}, "broker.test");
    try
    {
        with_empty_token->firstMessage();
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Config);
    }
}

#if USE_SSL

namespace
{

constexpr auto rfc7677_server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";

/// The SCRAM-SHA-256 example exchange of RFC 7677 section 3.
std::unique_ptr<Kafka::SaslMechanism> makeRfc7677Mechanism()
{
    return createScramMechanism(
        SaslScramConfig{.algorithm = SaslScramConfig::Algorithm::Sha256, .username = "user", .password = "pencil"},
        "rOprNGfwEbeRWgbNEkqO");
}

}

TEST(KafkaClientSasl, ScramSha256MatchesTheRfc7677Example)
{
    auto mechanism = makeRfc7677Mechanism();
    EXPECT_STREQ(mechanism->name(), "SCRAM-SHA-256");
    EXPECT_EQ(mechanism->firstMessage(), "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");

    const auto client_final = mechanism->nextMessage(rfc7677_server_first);
    ASSERT_TRUE(client_final.has_value());
    EXPECT_EQ(
        *client_final,
        "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=");

    EXPECT_EQ(mechanism->nextMessage("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="), std::nullopt);

    /// The exchange is over; more server messages are a protocol violation.
    try
    {
        mechanism->nextMessage({});
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Protocol);
    }
}

TEST(KafkaClientSasl, ScramSha256RejectsAWrongServerSignature)
{
    auto mechanism = makeRfc7677Mechanism();
    mechanism->firstMessage();
    mechanism->nextMessage(rfc7677_server_first);
    try
    {
        mechanism->nextMessage("v=aaaaTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Protocol);
        EXPECT_TRUE(String(e.message()).contains("signature mismatch"));
    }
}

TEST(KafkaClientSasl, ScramSha256RejectsAServerNonceThatDoesNotExtendTheClientNonce)
{
    auto mechanism = makeRfc7677Mechanism();
    mechanism->firstMessage();
    try
    {
        mechanism->nextMessage("r=someoneElsesNonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096");
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Protocol);
        EXPECT_TRUE(String(e.message()).contains("nonce"));
    }
}

TEST(KafkaClientSasl, ScramSha256RejectsAMalformedServerFirstMessage)
{
    /// Attributes out of order.
    auto out_of_order = makeRfc7677Mechanism();
    out_of_order->firstMessage();
    try
    {
        out_of_order->nextMessage("s=W22ZaJ0SNY7soEsUEjb6gQ==,r=rOprNGfwEbeRWgbNEkqOx,i=4096");
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Protocol);
    }

    /// Non-numeric iteration count.
    auto bad_iterations = makeRfc7677Mechanism();
    bad_iterations->firstMessage();
    try
    {
        bad_iterations->nextMessage("r=rOprNGfwEbeRWgbNEkqOx,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=many");
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Protocol);
    }
}

TEST(KafkaClientSasl, ScramUsernameEscaping)
{
    auto mechanism = createScramMechanism(
        SaslScramConfig{.algorithm = SaslScramConfig::Algorithm::Sha256, .username = "a,b=c", .password = "secret"}, "xyz");
    EXPECT_EQ(mechanism->firstMessage(), "n,,n=a=2Cb=3Dc,r=xyz");
}

TEST(KafkaClientSasl, ScramSha512Name)
{
    auto mechanism = createScramMechanism(
        SaslScramConfig{.algorithm = SaslScramConfig::Algorithm::Sha512, .username = "user", .password = "secret"}, "nonce");
    EXPECT_STREQ(mechanism->name(), "SCRAM-SHA-512");
}

#endif

TEST(KafkaClientSasl, GssapiSupportMatchesTheBuild)
{
#if USE_KRB5
    auto mechanism = createSaslMechanism(SaslConfig{Kafka::SaslGssapiConfig{}}, "broker.test");
    EXPECT_STREQ(mechanism->name(), "GSSAPI");
    Kafka::checkSaslMechanismSupported(SaslConfig{Kafka::SaslGssapiConfig{}});
    /// There is no KDC and no ticket in the test environment, so the first
    /// handshake step must fail with a Kafka exception (and must not crash).
    EXPECT_THROW(mechanism->firstMessage(), Exception);
#else
    EXPECT_THROW(createSaslMechanism(SaslConfig{Kafka::SaslGssapiConfig{}}, "broker.test"), Exception);
    EXPECT_THROW(Kafka::checkSaslMechanismSupported(SaslConfig{Kafka::SaslGssapiConfig{}}), Exception);
#endif
}

TEST(KafkaClientSasl, CheckSaslMechanismSupportedValidatesCredentials)
{
    /// PLAIN is available in every build.
    Kafka::checkSaslMechanismSupported(SaslConfig{SaslPlainConfig{.username = "u", .password = "p", .authorization_id = {}}});

    /// OAUTHBEARER without a token provider fails closed at validation time.
    try
    {
        Kafka::checkSaslMechanismSupported(SaslConfig{SaslOauthBearerConfig{}});
        FAIL() << "expected a configuration error";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Config);
    }

    /// GSSAPI with an empty service name is invalid in krb5 builds and
    /// unsupported otherwise; either way it must throw.
    EXPECT_THROW(
        Kafka::checkSaslMechanismSupported(SaslConfig{Kafka::SaslGssapiConfig{.service_name = "", .principal = {}}}), Exception);
}
