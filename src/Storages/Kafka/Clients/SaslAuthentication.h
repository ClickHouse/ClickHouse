#pragma once

/// SASL mechanisms over the SaslHandshake/SaslAuthenticate request pair.
/// A mechanism is a pure challenge-response state machine: the connection
/// layer moves the byte blobs over the wire, the mechanism computes them.
///
/// Implemented: PLAIN (RFC 4616), SCRAM-SHA-256/512 (RFC 5802/7677, requires
/// a build with SSL for the OpenSSL crypto), OAUTHBEARER (RFC 7628, token
/// supplied by a caller-provided provider — the AWS MSK IAM pattern) and
/// GSSAPI/Kerberos (RFC 4752, requires a build with krb5; the ticket comes
/// from the default credential cache, filled by ClickHouse's `kerberosInit`).

#include <Storages/Kafka/Clients/Config.h>
#include <Storages/Kafka/Clients/Types.h>

#include <memory>
#include <optional>

namespace Kafka
{

class SaslMechanism
{
public:
    virtual ~SaslMechanism() = default;

    /// Wire name sent in SaslHandshake ("PLAIN", "SCRAM-SHA-256", ...).
    virtual const char * name() const = 0;

    /// The initial client message (auth_bytes of the first SaslAuthenticate).
    virtual Bytes firstMessage() = 0;

    /// Consume the server's auth_bytes and produce the next client message;
    /// nullopt = the exchange is complete (after verifying the server data).
    /// Throws Exception(ErrorKind::Broker/Protocol) on verification failures.
    virtual std::optional<Bytes> nextMessage(const Bytes & server_response) = 0;
};

/// Builds the mechanism for the configured credentials. `broker_host` is the
/// connected broker's host name (GSSAPI authenticates against the service
/// principal `<service>/<broker_host>`; the other mechanisms ignore it).
/// Throws Exception(ErrorKind::NotSupported) for mechanisms the build lacks
/// (SCRAM without SSL support, GSSAPI without krb5 support).
std::unique_ptr<SaslMechanism> createSaslMechanism(const SaslConfig & config, const String & broker_host);

/// Fail-close configuration check without constructing a mechanism (and
/// without side effects like calling token providers): throws
/// Exception(ErrorKind::NotSupported) when the build lacks the mechanism and
/// Exception(ErrorKind::Config) for incomplete credentials.
void checkSaslMechanismSupported(const SaslConfig & config);

/// Test hook: a fixed client nonce makes the SCRAM exchange deterministic.
std::unique_ptr<SaslMechanism> createScramMechanism(const SaslScramConfig & config, String client_nonce);

}
