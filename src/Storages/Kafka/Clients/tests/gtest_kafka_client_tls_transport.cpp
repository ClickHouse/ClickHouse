#include "config.h"

#if USE_SSL

#include <Storages/Kafka/Clients/Net/TLSTransport.h>

#include <gtest/gtest.h>

#include <Poco/Net/Context.h>
#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/TemporaryFile.h>

#include <openssl/ssl.h>

#include <chrono>
#include <fstream>
#include <optional>
#include <thread>
#include <vector>

using namespace Kafka;
using namespace Kafka::Net;
using namespace std::chrono_literals;

namespace
{

/// Self-signed RSA-2048 certificate for CN=127.0.0.1 with
/// subjectAltName=IP:127.0.0.1, valid for 36500 days. Generated once with:
///   openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem
///       -days 36500 -nodes -subj "/CN=127.0.0.1"
///       -addext "subjectAltName=IP:127.0.0.1"
constexpr const char * server_certificate_pem = R"(-----BEGIN CERTIFICATE-----
MIIDHDCCAgSgAwIBAgIUBytUJo9ATkM3I20F2beGvIFV7vAwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJMTI3LjAuMC4xMCAXDTI2MTEwNjEwMTEyM1oYDzIxMjYx
MDEzMTAxMTIzWjAUMRIwEAYDVQQDDAkxMjcuMC4wLjEwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQC0mJpTESLcI+jm03As01wBDj1/KxONdqDRu5b2Ua7w
OMEmuGhku0VQHQdpWeftGGn3n+Zz8AqG2Rknu86T86oT0L6hhXr4LRvjpfyN7cpm
+TF3JiXr0s+BPhIzD+zZtSf9Exq6Sm00q22cdpfowRgAdknY/tiCGU1car/87rtI
LDFZ8yGSkoIKBItyCSXcGgrNd0qVDBXLw8D0qoPx+2vkfdYpwrpy9zJ1BhwTvabg
Hw/44woDU8Ee8Rpg1TRRe/1ksuMX4yIgCspH/r23ovUpR2p4X6Tr0+R98alIZy/Q
R8guxFcQmwGLSeU7VS7+wy5nzCr3ui3miiNySIMJ+x13AgMBAAGjZDBiMB0GA1Ud
DgQWBBTic0wYnYzbM9m4fJz/yaWNA0vpgDAfBgNVHSMEGDAWgBTic0wYnYzbM9m4
fJz/yaWNA0vpgDAPBgNVHRMBAf8EBTADAQH/MA8GA1UdEQQIMAaHBH8AAAEwDQYJ
KoZIhvcNAQELBQADggEBADLNBBG6cKJWeSHVAzTL5m8xH3yB0TMtCjne8w+funZ1
opn9Lmn9TvAn4P6t8egP9VBOe7SZHoeZIWbqcjLwx9946i5JdwhHUA+kkvIwz6cd
nk82oIu0GFg1i6SO6pgNc05tHcoRxT8+lgulTSJUwAablZlxktiRZPUYOqBX5m4V
QTn8jgpIDubnKLCIYKOo1DeoVvzhYGubPUJ+AQmPVq8I6CeZd3h1MaPPPyWrQVYZ
hy4QsEJBIELNoaYzHZp4gLse8uTE4RJDjpud6dK4gafK47yIEj1XJ3MigdE1Z/PG
5njTO7rgCLx3wd2KFBSgw6Xd8PxUGahbqDl9pLIZrcA=
-----END CERTIFICATE-----
)";

constexpr const char * server_key_pem = R"(-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC0mJpTESLcI+jm
03As01wBDj1/KxONdqDRu5b2Ua7wOMEmuGhku0VQHQdpWeftGGn3n+Zz8AqG2Rkn
u86T86oT0L6hhXr4LRvjpfyN7cpm+TF3JiXr0s+BPhIzD+zZtSf9Exq6Sm00q22c
dpfowRgAdknY/tiCGU1car/87rtILDFZ8yGSkoIKBItyCSXcGgrNd0qVDBXLw8D0
qoPx+2vkfdYpwrpy9zJ1BhwTvabgHw/44woDU8Ee8Rpg1TRRe/1ksuMX4yIgCspH
/r23ovUpR2p4X6Tr0+R98alIZy/QR8guxFcQmwGLSeU7VS7+wy5nzCr3ui3miiNy
SIMJ+x13AgMBAAECggEADOTiYTDq22sy8JwYY72ZfDfPOzpQ9JDg4uibsC2H1H3w
3c9dIL1gMbFI5gQ1yMStTYacoRrP3ZiNhmaPKsVtwhQYMEHD/wkqzGz9pF0zI3lX
nJ6xfBi6mQsx/cmoIFPMrZyEvtfxMR6rk6LCA4EZ3MU/+N7FMuYUkxcfawiD1Z9h
r4R9hvt3DitByYyQ3715SmVKQz0DxNdKopWhs8RiVmCZ6FFpb1PIkpfselADLRnp
SQNNQs+WZ7A7/7lONALTbKrtZB+MCizkQJNbS5q/1CWk07YXrklfPpj7dTULaKWS
ztDwVDyYcSKSNhgQETaG25Cys5YXY1S33mlfg1JIcQKBgQC2og9V/CkFV12QIA/3
vn0HMKEHGtpU4tUOTFYoPmbz5w1h/ogEm7QAlu7eHa4LrlRgZigWg4jX4odK07Ii
grp+70sE+qLDbIgjzxONtMbP+H31jAORMVa0JuKweZdkPWanBLORvmiQjDRMpAg8
2lpyPJAk0o72XPnp5UXpYkxaRwKBgQD9JRB2yaF2jcdKbJIcDbXChfapbdCITSsE
guk/HJLwsdLOz31TKpV50+yHA6Xg7RII38a57/59yZrI41XKT5kC/Hw1J/NkbwQ1
fNVrdynBg3ccDch7nxN8ewuvrUF35tMd85jDpuSmwVVn+Lv44kbwMZGYU1HFZaYw
djW/58KLUQKBgEOlDu0YMtFppJwHZCKtCX4qQtpw1cFvKt7a42aQhm18fPZ44kRi
r28weFUIxThKX7I85EhCe+MKYnYSpv4lUZJGa5DyjGq5vksVHfb0qFbLgUv7ii6z
CZw8cuZOgwZ7v5PJKAGpdu1jINPqRarUbX+2cuVQn+ccVOyPIS/qWsFtAoGBALDz
Xq3aRc5sTrmcaQnJH5upLwHCLHmKj55osAiYvbIYa0Z7TZx78N6nH/hEuYgfz3J6
khxZQWjGKzdvigGQYGHxAhXaJk23OJb2+uxS8NI/RlxK2jBnwyRJkqKrlk7s//g4
1jk1iTFzhXLXMPZJndK6o45593fnlSLdDrdlltzxAoGBAK4FruOGhkHjtvSrfV+v
bItkrToTdkhxG2D9QIFCy/XUAALSfUSm3Dr/7i/nVFC56bwl2xKA96YSITYwMPY0
I2o4hbAhU0JcVZ2kWKJrn7LoWQMg3SiJ6QAC7lvbyH6ld1iFy5n+SumvDuag3Bf1
2v2FnWrjI8s7jEQTzF6UYvJA
-----END PRIVATE KEY-----
)";

}

/// Thread-own TLS echo server on an ephemeral loopback port, with the
/// self-signed certificate above written to temporary PEM files.
class KafkaClientTLSTransport : public ::testing::Test
{
protected:
    void SetUp() override
    {
        writeFile(cert_file.path(), server_certificate_pem);
        writeFile(key_file.path(), server_key_pem);

        Poco::Net::Context::Params params;
        params.certificateFile = cert_file.path();
        params.privateKeyFile = key_file.path();
        params.verificationMode = Poco::Net::Context::VERIFY_NONE;
        server_context = new Poco::Net::Context(Poco::Net::Context::SERVER_USE, params);
        /// No TLS 1.3 session tickets: their post-handshake bytes would make
        /// the readable() assertions racy (the raw fd would show data that is
        /// not application data).
        SSL_CTX_set_num_tickets(server_context->sslContext(), 0);

        server.emplace(Poco::Net::SocketAddress("127.0.0.1", 0), /*backlog*/ 64, server_context);
    }

    static void writeFile(const String & path, const char * content)
    {
        std::ofstream out(path, std::ios::trunc);
        out << content;
        ASSERT_TRUE(out.good());
    }

    UInt16 port() const { return server->address().port(); }

    /// The client TlsConfig that trusts the self-signed server certificate.
    TlsConfig trustedConfig() const
    {
        TlsConfig tls;
        tls.ca_location = cert_file.path();
        tls.verify = true;
        return tls;
    }

    /// Accepts one connection and echoes exactly `n` bytes. The server-side
    /// handshake runs inside the first receiveBytes; a client that aborts it
    /// must only end the thread, not the test, hence the try/catch.
    std::thread echoThread(size_t n)
    {
        return std::thread(
            [this, n]
            {
                try
                {
                    Poco::Net::StreamSocket peer = server->acceptConnection();
                    std::vector<char> buf(n);
                    size_t got = 0;
                    while (got < n)
                    {
                        const int rc = peer.receiveBytes(buf.data() + got, static_cast<int>(n - got));
                        if (rc <= 0)
                            return;
                        got += static_cast<size_t>(rc);
                    }
                    size_t sent = 0;
                    while (sent < n)
                    {
                        const int rc = peer.sendBytes(buf.data() + sent, static_cast<int>(n - sent));
                        if (rc <= 0)
                            return;
                        sent += static_cast<size_t>(rc);
                    }
                }
                catch (...) /// NOLINT(bugprone-empty-catch)
                {
                }
            });
    }

    Poco::TemporaryFile cert_file;
    Poco::TemporaryFile key_file;
    Poco::Net::Context::Ptr server_context;
    std::optional<Poco::Net::SecureServerSocket> server;
};

TEST_F(KafkaClientTLSTransport, RoundTripWithVerification)
{
    auto echo = echoThread(5);

    TLSTransport transport(nullptr, trustedConfig());
    transport.connect("127.0.0.1", port(), Deadline::after(5s));
    EXPECT_TRUE(transport.isOpen());

    transport.writeAll("hello", Deadline::after(5s));
    char buf[5];
    size_t got = 0;
    while (got < 5)
        got += transport.readSome(buf + got, 5 - got, Deadline::after(5s));
    EXPECT_EQ(String(buf, 5), "hello");

    echo.join();
    transport.close();
    EXPECT_FALSE(transport.isOpen());
}

TEST_F(KafkaClientTLSTransport, RejectsUnknownCertificate)
{
    auto echo = echoThread(1);

    /// Defaults: verify = true, empty ca_location = the system store, which
    /// does not contain the self-signed server certificate.
    TlsConfig tls;
    TLSTransport transport(nullptr, tls);
    try
    {
        transport.connect("127.0.0.1", port(), Deadline::after(5s));
        FAIL() << "expected a TLS verification failure";
    }
    catch (const Kafka::Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Connection);
    }
    EXPECT_FALSE(transport.isOpen());

    echo.join();
}

TEST_F(KafkaClientTLSTransport, NoVerificationConnects)
{
    auto echo = echoThread(4);

    TlsConfig tls;
    tls.verify = false;
    TLSTransport transport(nullptr, tls);
    transport.connect("127.0.0.1", port(), Deadline::after(5s));
    EXPECT_TRUE(transport.isOpen());

    transport.writeAll("ping", Deadline::after(5s));
    char buf[4];
    size_t got = 0;
    while (got < 4)
        got += transport.readSome(buf + got, 4 - got, Deadline::after(5s));
    EXPECT_EQ(String(buf, 4), "ping");

    echo.join();
    transport.close();
}

TEST_F(KafkaClientTLSTransport, ReadableSeesSslBufferedData)
{
    /// Trigger byte, then a single two-byte TLS record in response: after the
    /// first byte is consumed, the second stays decrypted inside OpenSSL
    /// (SSL_pending) and never shows on the raw fd.
    std::thread responder(
        [this]
        {
            try
            {
                Poco::Net::StreamSocket peer = server->acceptConnection();
                char trigger;
                int got = 0;
                while (got < 1)
                {
                    const int rc = peer.receiveBytes(&trigger, 1);
                    if (rc <= 0)
                        return;
                    got += rc;
                }
                peer.sendBytes("ab", 2);
            }
            catch (...) /// NOLINT(bugprone-empty-catch)
            {
            }
        });

    TLSTransport transport(nullptr, trustedConfig());
    transport.connect("127.0.0.1", port(), Deadline::after(5s));
    EXPECT_FALSE(transport.readable());

    transport.writeAll("x", Deadline::after(5s));

    /// Bounded wait for the response record to reach the socket.
    const auto arrival = Deadline::after(5s);
    while (!transport.readable() && !arrival.expired())
        std::this_thread::sleep_for(1ms);
    EXPECT_TRUE(transport.readable());

    char byte;
    ASSERT_EQ(transport.readSome(&byte, 1, Deadline::after(5s)), 1u);
    EXPECT_EQ(byte, 'a');

    /// The second byte came in the same TLS record, so it is buffered inside
    /// OpenSSL now; readable() must still see it although the kernel buffer
    /// no longer holds it.
    EXPECT_TRUE(transport.readable());
    ASSERT_EQ(transport.readSome(&byte, 1, Deadline::after(5s)), 1u);
    EXPECT_EQ(byte, 'b');

    responder.join();
    transport.close();
}

#endif
