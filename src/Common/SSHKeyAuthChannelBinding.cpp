#include <Common/SSHKeyAuthChannelBinding.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

#include "config.h"

#if USE_SSL
#    include <Common/OpenSSLHelpers.h>
#    include <Poco/Net/SecureStreamSocketImpl.h>
#    include <openssl/ssl.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int OPENSSL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// A constant tag at the start of the signed message, so that a signature made for this purpose
/// cannot be mistaken for a signature over any other kind of message made with the same key.
constexpr std::string_view SSH_KEY_AUTH_MESSAGE_TAG = "ClickHouse native ssh_key auth v1";

constexpr std::string_view CHANNEL_BINDING_TYPE_NONE = "none";

#if USE_SSL
constexpr std::string_view CHANNEL_BINDING_TYPE_TLS_EXPORTER = "tls-exporter";

/// RFC 9266, section 2.
constexpr std::string_view TLS_EXPORTER_LABEL = "EXPORTER-Channel-Binding";
constexpr size_t TLS_EXPORTER_LENGTH = 32;
#endif

}

SSHKeyAuthChannelBinding getSSHKeyAuthChannelBinding([[maybe_unused]] const Poco::Net::SocketImpl * socket_impl)
{
#if USE_SSL
    if (const auto * secure_impl = dynamic_cast<const Poco::Net::SecureStreamSocketImpl *>(socket_impl))
    {
        SSL * ssl = secure_impl->ssl();
        if (!ssl || !SSL_is_init_finished(ssl))
            throw Exception(ErrorCodes::OPENSSL_ERROR,
                "Cannot derive the TLS channel binding for ssh_key authentication: the TLS handshake is not finished");

        /// Before TLS 1.3, the exporter is unique to the session only with the Extended Master Secret extension (RFC 7627).
        /// Without it, two sessions can be made to share a master secret (the triple handshake attack),
        /// so refuse to authenticate instead of producing a binding that does not bind.
        if (SSL_version(ssl) < TLS1_3_VERSION)
        {
            if (SSL_get_extms_support(ssl) != 1)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                    "Cannot use ssh_key authentication over a TLS session without the Extended Master Secret extension. "
                    "Use TLS 1.3, or enable Extended Master Secret for TLS 1.2");

            /// RFC 9266, section 4.2: the binding is not defined for a connection with renegotiation enabled.
            /// Disable it for the rest of this connection before deriving the binding, so that its session
            /// (and the value both ends have mixed into the signature) cannot be replaced later.
            /// TLS 1.3 has no renegotiation.
            SSL_set_options(ssl, SSL_OP_NO_RENEGOTIATION);
        }

        /// RFC 9266: the label without a terminating NUL, a zero-length context (which is different from no context
        /// before TLS 1.3), and 32 bytes of output.
        String value(TLS_EXPORTER_LENGTH, '\0');
        if (1 != SSL_export_keying_material(
                ssl,
                reinterpret_cast<unsigned char *>(value.data()), value.size(),
                TLS_EXPORTER_LABEL.data(), TLS_EXPORTER_LABEL.size(),
                /* context = */ nullptr, /* contextlen = */ 0, /* use_context = */ 1))
        {
            throw Exception(ErrorCodes::OPENSSL_ERROR,
                "Cannot derive the TLS channel binding for ssh_key authentication: {}", getOpenSSLErrors());
        }

        return {String(CHANNEL_BINDING_TYPE_TLS_EXPORTER), std::move(value)};
    }
#endif

    return {String(CHANNEL_BINDING_TYPE_NONE), String{}};
}

String buildSSHKeyAuthMessage(
    UInt64 protocol_version,
    const String & default_database,
    const String & user,
    const String & challenge,
    const SSHKeyAuthChannelBinding & binding)
{
    WriteBufferFromOwnString buf;
    writeStringBinary(SSH_KEY_AUTH_MESSAGE_TAG, buf);
    writeStringBinary(std::to_string(protocol_version), buf);
    writeStringBinary(default_database, buf);
    writeStringBinary(user, buf);
    writeStringBinary(challenge, buf);
    writeStringBinary(binding.type, buf);
    writeStringBinary(binding.value, buf);
    return buf.str();
}

}
