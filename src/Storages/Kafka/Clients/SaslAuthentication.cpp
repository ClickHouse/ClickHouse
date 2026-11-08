#include <Storages/Kafka/Clients/SaslAuthentication.h>

#include <Storages/Kafka/Clients/Error.h>

#include "config.h"

#include <Common/Base64.h>
#include <Common/getRandomASCIIString.h>

#if USE_SSL
#include <openssl/evp.h>
#include <openssl/hmac.h>
#endif

#if USE_KRB5
#include <base/scope_guard.h>

#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>

#include <mutex>
#endif

#include <algorithm>
#include <charconv>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace Kafka
{

namespace
{

/// PLAIN (RFC 4616): a single message, a single round trip.
class PlainMechanism : public SaslMechanism
{
public:
    explicit PlainMechanism(SaslPlainConfig config_) : config(std::move(config_)) { }

    const char * name() const override { return "PLAIN"; }

    /// message = [authzid] UTF8NUL authcid UTF8NUL passwd
    Bytes firstMessage() override
    {
        Bytes message;
        message.reserve(config.authorization_id.size() + config.username.size() + config.password.size() + 2);
        message += config.authorization_id;
        message += '\0';
        message += config.username;
        message += '\0';
        message += config.password;
        return message;
    }

    /// The server's successful response carries no data to verify.
    std::optional<Bytes> nextMessage(const Bytes &) override { return std::nullopt; }

private:
    const SaslPlainConfig config;
};

#if USE_SSL

/// RFC 5802 section 5.1: '=' and ',' inside a saslname are escaped.
String saslName(const String & username)
{
    String escaped;
    escaped.reserve(username.size());
    for (const char c : username)
    {
        if (c == '=')
            escaped += "=3D";
        else if (c == ',')
            escaped += "=2C";
        else
            escaped += c;
    }
    return escaped;
}

String hmacSign(const EVP_MD * md, const String & key, const String & data)
{
    String result(EVP_MAX_MD_SIZE, '\0');
    unsigned int size = 0;
    if (!HMAC(
            md,
            key.data(),
            static_cast<int>(key.size()),
            reinterpret_cast<const unsigned char *>(data.data()),
            data.size(),
            reinterpret_cast<unsigned char *>(result.data()),
            &size))
        throw Exception(ErrorKind::NotSupported, "OpenSSL HMAC computation failed");
    result.resize(size);
    return result;
}

String digestOf(const EVP_MD * md, const String & data)
{
    String result(EVP_MAX_MD_SIZE, '\0');
    unsigned int size = 0;
    if (!EVP_Digest(data.data(), data.size(), reinterpret_cast<unsigned char *>(result.data()), &size, md, nullptr))
        throw Exception(ErrorKind::NotSupported, "OpenSSL digest computation failed");
    result.resize(size);
    return result;
}

/// Splits a SCRAM message into single-letter attribute/value pairs
/// ("r=abc,i=4096" -> {{'r', "abc"}, {'i', "4096"}}). Values never contain
/// commas by the RFC 5802 grammar, so splitting on commas is exact.
std::vector<std::pair<char, String>> parseScramAttributes(const String & message)
{
    std::vector<std::pair<char, String>> attributes;
    size_t pos = 0;
    while (pos <= message.size())
    {
        size_t comma = message.find(',', pos);
        if (comma == String::npos)
            comma = message.size();
        /// An attribute is one letter, '=', and a possibly empty value.
        if (comma < pos + 2 || message[pos + 1] != '=')
            throw Exception(ErrorKind::Protocol, "Malformed SCRAM attribute in server message '{}'", message);
        attributes.emplace_back(message[pos], message.substr(pos + 2, comma - pos - 2));
        pos = comma + 1;
    }
    return attributes;
}

/// SCRAM (RFC 5802; the SHA-256/512 parameters come from RFC 7677).
/// Three client messages at most: client-first, client-final, done.
class ScramMechanism : public SaslMechanism
{
public:
    ScramMechanism(SaslScramConfig config_, String client_nonce_)
        : config(std::move(config_))
        , client_nonce(std::move(client_nonce_))
    {
    }

    const char * name() const override
    {
        return config.algorithm == SaslScramConfig::Algorithm::Sha256 ? "SCRAM-SHA-256" : "SCRAM-SHA-512";
    }

    Bytes firstMessage() override
    {
        /// "n,," is the GS2 header: no channel binding, no authorization id.
        client_first_bare = "n=" + saslName(config.username) + ",r=" + client_nonce;
        return "n,," + client_first_bare;
    }

    std::optional<Bytes> nextMessage(const Bytes & server_response) override
    {
        switch (state)
        {
            case State::AwaitServerFirst:
                state = State::AwaitServerFinal;
                return respondToServerFirst(server_response);
            case State::AwaitServerFinal:
                state = State::Done;
                verifyServerFinal(server_response);
                return std::nullopt;
            case State::Done:
                break;
        }
        throw Exception(ErrorKind::Protocol, "Unexpected extra SCRAM server message");
    }

private:
    enum class State : UInt8
    {
        AwaitServerFirst,
        AwaitServerFinal,
        Done,
    };

    const EVP_MD * messageDigest() const
    {
        return config.algorithm == SaslScramConfig::Algorithm::Sha256 ? EVP_sha256() : EVP_sha512();
    }

    /// server-first-message = "r=" nonce ",s=" base64-salt ",i=" posit-number
    /// ["," extensions]; extensions after i are ignored per the RFC.
    Bytes respondToServerFirst(const String & server_first)
    {
        const auto attributes = parseScramAttributes(server_first);
        if (attributes.size() < 3 || attributes[0].first != 'r' || attributes[1].first != 's' || attributes[2].first != 'i')
            throw Exception(
                ErrorKind::Protocol,
                "Malformed SCRAM server-first message '{}': expected the attributes r, s, i",
                server_first);

        const String & server_nonce = attributes[0].second;
        if (!server_nonce.starts_with(client_nonce))
            throw Exception(ErrorKind::Protocol, "Server nonce does not extend the client nonce");

        const String salt = DB::base64Decode(attributes[1].second);

        const String & iterations_text = attributes[2].second;
        UInt64 iterations = 0;
        const auto [parse_end, parse_error]
            = std::from_chars(iterations_text.data(), iterations_text.data() + iterations_text.size(), iterations);
        if (parse_error != std::errc() || parse_end != iterations_text.data() + iterations_text.size() || iterations == 0)
            throw Exception(ErrorKind::Protocol, "Invalid SCRAM iteration count '{}'", iterations_text);
        /// A hostile server could stall the client with an absurd PBKDF2 cost.
        if (iterations > 10'000'000)
            throw Exception(ErrorKind::Protocol, "SCRAM iteration count {} is unreasonably large", iterations);

        const EVP_MD * md = messageDigest();
        const auto digest_size = static_cast<size_t>(EVP_MD_size(md));

        String salted_password(digest_size, '\0');
        if (!PKCS5_PBKDF2_HMAC(
                config.password.data(),
                static_cast<int>(config.password.size()),
                reinterpret_cast<const unsigned char *>(salt.data()),
                static_cast<int>(salt.size()),
                static_cast<int>(iterations),
                md,
                static_cast<int>(digest_size),
                reinterpret_cast<unsigned char *>(salted_password.data())))
            throw Exception(ErrorKind::NotSupported, "OpenSSL PBKDF2 computation failed");

        const String client_key = hmacSign(md, salted_password, "Client Key");
        const String stored_key = digestOf(md, client_key);

        /// "biws" = base64("n,,"): the GS2 header echoed in the channel-binding attribute.
        const String client_final_without_proof = "c=biws,r=" + server_nonce;
        const String auth_message = client_first_bare + "," + server_first + "," + client_final_without_proof;

        const String client_signature = hmacSign(md, stored_key, auth_message);
        String client_proof = client_key;
        for (size_t i = 0; i < client_proof.size(); ++i)
            client_proof[i] = static_cast<char>(client_proof[i] ^ client_signature[i]);

        const String server_key = hmacSign(md, salted_password, "Server Key");
        expected_server_signature = hmacSign(md, server_key, auth_message);

        return client_final_without_proof + ",p=" + DB::base64Encode(client_proof);
    }

    /// server-final-message = ("e=" server-error-value / "v=" base64-signature)
    /// ["," extensions]; only the first attribute matters.
    void verifyServerFinal(const String & server_final)
    {
        const size_t comma = server_final.find(',');
        const size_t value_size = comma == String::npos ? String::npos : comma - 2;

        if (server_final.starts_with("e="))
            throw Exception(ErrorKind::Protocol, "SCRAM authentication failed: {}", server_final.substr(2, value_size));
        if (!server_final.starts_with("v="))
            throw Exception(ErrorKind::Protocol, "Malformed SCRAM server-final message '{}'", server_final);

        const String server_signature = DB::base64Decode(server_final.substr(2, value_size));

        /// Constant-time comparison: accumulate the differences over every byte
        /// instead of returning at the first mismatch.
        UInt8 difference = server_signature.size() == expected_server_signature.size() ? 0 : 1;
        const size_t common_size = std::min(server_signature.size(), expected_server_signature.size());
        for (size_t i = 0; i < common_size; ++i)
            difference |= static_cast<UInt8>(server_signature[i]) ^ static_cast<UInt8>(expected_server_signature[i]);
        if (difference != 0)
            throw Exception(
                ErrorKind::Protocol,
                "Server signature mismatch — the broker does not know the password or the exchange was tampered with");
    }

    const SaslScramConfig config;
    const String client_nonce;
    State state = State::AwaitServerFirst;
    /// client-first-message-bare, kept for the AuthMessage.
    String client_first_bare;
    /// HMAC(ServerKey, AuthMessage), computed in round one and checked in round two.
    String expected_server_signature;
};

#endif

/// OAUTHBEARER (RFC 7628): the token goes out in the first message; on failure
/// the server sends a JSON error challenge, the client answers with a single
/// 0x01 byte, and the server fails the authentication with an error code
/// (which the connection layer reports as an ErrorKind::Broker exception).
class OauthBearerMechanism : public SaslMechanism
{
public:
    explicit OauthBearerMechanism(SaslOauthBearerConfig config_) : config(std::move(config_)) { }

    const char * name() const override { return "OAUTHBEARER"; }

    Bytes firstMessage() override
    {
        if (!config.token_provider)
            throw Exception(ErrorKind::Config, "SASL/OAUTHBEARER requires a token provider");
        const OAuthToken token = config.token_provider();
        if (token.value.empty())
            throw Exception(ErrorKind::Config, "The OAUTHBEARER token provider returned an empty token");
        /// GS2 header, then the auth key/value pair framed by 0x01 separators.
        return "n,,\x01" "auth=Bearer " + token.value + "\x01\x01";
    }

    std::optional<Bytes> nextMessage(const Bytes & server_response) override
    {
        /// After the dummy response the server must fail the authentication
        /// with an error code, not keep talking.
        if (error_challenge)
            throw Exception(
                ErrorKind::Protocol, "Unexpected OAUTHBEARER server message after the error challenge: {}", *error_challenge);
        if (server_response.empty())
            return std::nullopt;
        /// A non-empty response is the JSON error challenge.
        error_challenge = server_response;
        return Bytes(1, '\x01');
    }

private:
    const SaslOauthBearerConfig config;
    std::optional<String> error_challenge;
};

#if USE_KRB5

/// All GSSAPI calls are serialized with a local mutex, mirroring the
/// precaution of DB::GSSAcceptorContext (src/Access/GSSAcceptor.cpp): the
/// vendored krb5 reads shared profile and credential-cache state on these
/// paths. Kerberos handshakes are rare (once per connection), so the
/// serialization costs nothing.
std::mutex gss_mutex;

String gssBufferToString(const gss_buffer_desc & buf)
{
    if (buf.length == 0 || buf.value == nullptr)
        return {};
    return String(static_cast<const char *>(buf.value), buf.length);
}

/// Human-readable text for a GSSAPI major/minor status pair.
String gssStatusToString(OM_uint32 major, OM_uint32 minor)
{
    String messages;
    const auto append_messages = [&](OM_uint32 code, int type)
    {
        OM_uint32 message_context = 0;
        do
        {
            gss_buffer_desc status_buf{};
            OM_uint32 display_minor = 0;
            const OM_uint32 display_major = gss_display_status(&display_minor, code, type, GSS_C_NO_OID, &message_context, &status_buf);
            if (GSS_ERROR(display_major))
                return;
            const String message = gssBufferToString(status_buf);
            OM_uint32 release_minor = 0;
            gss_release_buffer(&release_minor, &status_buf);
            if (!message.empty())
            {
                if (!messages.empty())
                    messages += ", ";
                messages += message;
            }
        } while (message_context != 0);
    };
    append_messages(major, GSS_C_GSS_CODE);
    append_messages(minor, GSS_C_MECH_CODE);
    return messages;
}

/// GSSAPI/Kerberos (RFC 4752) over SaslHandshake/SaslAuthenticate:
///  1. gss_init_sec_context rounds establish the context (mutual auth);
///  2. the server sends a wrapped security-layer offer (1 byte of layer bits +
///     3 bytes of max message size);
///  3. the client answers with a wrapped choice — no security layer (the
///     Kafka SASL framing never uses one) — and the broker reports success.
class GssapiMechanism : public SaslMechanism
{
public:
    GssapiMechanism(SaslGssapiConfig config_, String broker_host_)
        : config(std::move(config_))
        , broker_host(std::move(broker_host_))
    {
    }

    ~GssapiMechanism() override
    {
        std::lock_guard lock(gss_mutex);
        OM_uint32 minor = 0;
        if (context != GSS_C_NO_CONTEXT)
            gss_delete_sec_context(&minor, &context, GSS_C_NO_BUFFER);
        if (target_name != GSS_C_NO_NAME)
            gss_release_name(&minor, &target_name);
        if (credential != GSS_C_NO_CREDENTIAL)
            gss_release_cred(&minor, &credential);
    }

    const char * name() const override { return "GSSAPI"; }

    Bytes firstMessage() override
    {
        std::lock_guard lock(gss_mutex);
        importTargetName();
        acquireCredential();
        return step({});
    }

    std::optional<Bytes> nextMessage(const Bytes & server_response) override
    {
        std::lock_guard lock(gss_mutex);
        switch (state)
        {
            case State::Negotiating:
                return step(server_response);
            case State::SecurityLayer:
                return negotiateSecurityLayer(server_response);
            case State::Done:
                if (!server_response.empty())
                    throw Exception(ErrorKind::Protocol, "Unexpected GSSAPI server message after the security-layer exchange");
                return std::nullopt;
        }
    }

private:
    enum class State : UInt8
    {
        Negotiating,
        SecurityLayer,
        Done,
    };

    [[noreturn]] void throwGssError(ErrorKind kind, const char * action, OM_uint32 major, OM_uint32 minor) const
    {
        throw Exception(
            kind, "GSSAPI {} for {}@{} failed: {}", action, config.service_name, broker_host, gssStatusToString(major, minor));
    }

    void importTargetName()
    {
        if (broker_host.empty())
            throw Exception(ErrorKind::Config, "SASL/GSSAPI requires the broker host name for the service principal");
        String target = config.service_name + "@" + broker_host;
        gss_buffer_desc name_buffer{target.size(), target.data()};
        OM_uint32 minor = 0;
        const OM_uint32 major = gss_import_name(&minor, &name_buffer, GSS_C_NT_HOSTBASED_SERVICE, &target_name);
        if (GSS_ERROR(major))
            throwGssError(ErrorKind::Config, "service name import", major, minor);
    }

    /// Only an explicitly configured principal needs an acquired credential;
    /// GSS_C_NO_CREDENTIAL means the credential cache's default principal.
    void acquireCredential()
    {
        if (config.principal.empty())
            return;

        String principal = config.principal;
        gss_buffer_desc principal_buffer{principal.size(), principal.data()};
        gss_name_t principal_name = GSS_C_NO_NAME;
        OM_uint32 minor = 0;
        OM_uint32 major = gss_import_name(&minor, &principal_buffer, GSS_KRB5_NT_PRINCIPAL_NAME, &principal_name);
        if (GSS_ERROR(major))
            throwGssError(ErrorKind::Config, "principal name import", major, minor);
        SCOPE_EXIT({
            OM_uint32 release_minor = 0;
            gss_release_name(&release_minor, &principal_name);
        });

        major = gss_acquire_cred(&minor, principal_name, GSS_C_INDEFINITE, GSS_C_NO_OID_SET, GSS_C_INITIATE, &credential, nullptr, nullptr);
        if (GSS_ERROR(major))
            throwGssError(ErrorKind::Config, "credential acquisition", major, minor);
    }

    /// One gss_init_sec_context round. Returns the token to send; an empty
    /// token is only valid when the context completed (the driver then sends
    /// an empty SaslAuthenticate to receive the security-layer offer).
    Bytes step(const Bytes & input)
    {
        /// The buffer is read-only for GSSAPI; the struct just lacks const.
        gss_buffer_desc input_buffer{input.size(), const_cast<char *>(input.data())};
        gss_buffer_desc output_buffer{};
        OM_uint32 minor = 0;
        const OM_uint32 major = gss_init_sec_context(
            &minor,
            credential,
            &context,
            target_name,
            const_cast<gss_OID>(gss_mech_krb5),
            GSS_C_MUTUAL_FLAG | GSS_C_SEQUENCE_FLAG,
            0,
            GSS_C_NO_CHANNEL_BINDINGS,
            input.empty() ? GSS_C_NO_BUFFER : &input_buffer,
            nullptr,
            &output_buffer,
            nullptr,
            nullptr);
        SCOPE_EXIT({
            OM_uint32 release_minor = 0;
            gss_release_buffer(&release_minor, &output_buffer);
        });
        if (GSS_ERROR(major))
            throwGssError(ErrorKind::Connection, "handshake", major, minor);

        Bytes out = gssBufferToString(output_buffer);
        if (major & GSS_S_CONTINUE_NEEDED)
        {
            /// The SASL framing cannot make progress without a token.
            if (out.empty())
                throw Exception(ErrorKind::Protocol, "GSSAPI continuation without a token from {}@{}", config.service_name, broker_host);
        }
        else
        {
            state = State::SecurityLayer;
        }
        return out;
    }

    std::optional<Bytes> negotiateSecurityLayer(const Bytes & server_response)
    {
        if (server_response.empty())
            throw Exception(ErrorKind::Protocol, "Expected the GSSAPI security-layer offer, got an empty message");

        gss_buffer_desc wrapped{server_response.size(), const_cast<char *>(server_response.data())};
        gss_buffer_desc offer_buffer{};
        OM_uint32 minor = 0;
        int conf_state = 0;
        OM_uint32 major = gss_unwrap(&minor, context, &wrapped, &offer_buffer, &conf_state, nullptr);
        if (GSS_ERROR(major))
            throwGssError(ErrorKind::Connection, "security-layer unwrap", major, minor);
        const String offer = gssBufferToString(offer_buffer);
        OM_uint32 release_minor = 0;
        gss_release_buffer(&release_minor, &offer_buffer);

        /// Byte 0: bitmask of offered layers (0x01 = none, 0x02 = integrity,
        /// 0x04 = confidentiality); bytes 1-3: max wrapped message size.
        if (offer.size() < 4)
            throw Exception(ErrorKind::Protocol, "GSSAPI security-layer offer is too short: {} bytes", offer.size());
        if ((static_cast<UInt8>(offer[0]) & 0x01) == 0)
            throw Exception(
                ErrorKind::NotSupported,
                "The broker requires a SASL security layer (integrity or confidentiality), which is not supported");

        /// Choose no security layer, max message size 0.
        char choice[4] = {0x01, 0, 0, 0};
        gss_buffer_desc choice_buffer{sizeof(choice), choice};
        gss_buffer_desc wrapped_choice{};
        major = gss_wrap(&minor, context, 0, GSS_C_QOP_DEFAULT, &choice_buffer, &conf_state, &wrapped_choice);
        SCOPE_EXIT({
            OM_uint32 wrap_release_minor = 0;
            gss_release_buffer(&wrap_release_minor, &wrapped_choice);
        });
        if (GSS_ERROR(major))
            throwGssError(ErrorKind::Connection, "security-layer wrap", major, minor);

        state = State::Done;
        return gssBufferToString(wrapped_choice);
    }

    const SaslGssapiConfig config;
    const String broker_host;
    State state = State::Negotiating;
    gss_ctx_id_t context = GSS_C_NO_CONTEXT;
    gss_name_t target_name = GSS_C_NO_NAME;
    gss_cred_id_t credential = GSS_C_NO_CREDENTIAL;
};

#endif

}

#if USE_SSL

std::unique_ptr<SaslMechanism> createScramMechanism(const SaslScramConfig & config, String client_nonce)
{
    return std::make_unique<ScramMechanism>(config, std::move(client_nonce));
}

#else

std::unique_ptr<SaslMechanism> createScramMechanism(const SaslScramConfig & config, String /*client_nonce*/)
{
    throw Exception(
        ErrorKind::NotSupported,
        "SCRAM-SHA-{} requires a build with SSL support",
        config.algorithm == SaslScramConfig::Algorithm::Sha256 ? 256 : 512);
}

#endif

std::unique_ptr<SaslMechanism> createSaslMechanism(const SaslConfig & config, const String & broker_host)
{
    return std::visit(
        [&broker_host](const auto & mechanism_config) -> std::unique_ptr<SaslMechanism>
        {
            using T = std::decay_t<decltype(mechanism_config)>;
            if constexpr (std::is_same_v<T, SaslPlainConfig>)
                return std::make_unique<PlainMechanism>(mechanism_config);
            else if constexpr (std::is_same_v<T, SaslScramConfig>)
                return createScramMechanism(mechanism_config, DB::getRandomASCIIString(24));
            else if constexpr (std::is_same_v<T, SaslGssapiConfig>)
            {
#if USE_KRB5
                return std::make_unique<GssapiMechanism>(mechanism_config, broker_host);
#else
                throw Exception(ErrorKind::NotSupported, "SASL/GSSAPI requires a build with krb5 support");
#endif
            }
            else
                return std::make_unique<OauthBearerMechanism>(mechanism_config);
        },
        config);
}

void checkSaslMechanismSupported(const SaslConfig & config)
{
    std::visit(
        []([[maybe_unused]] const auto & mechanism_config)
        {
            using T = std::decay_t<decltype(mechanism_config)>;
            if constexpr (std::is_same_v<T, SaslScramConfig>)
            {
#if !USE_SSL
                throw Exception(
                    ErrorKind::NotSupported,
                    "SCRAM-SHA-{} requires a build with SSL support",
                    mechanism_config.algorithm == SaslScramConfig::Algorithm::Sha256 ? 256 : 512);
#endif
            }
            else if constexpr (std::is_same_v<T, SaslOauthBearerConfig>)
            {
                if (!mechanism_config.token_provider)
                    throw Exception(ErrorKind::Config, "SASL/OAUTHBEARER requires a token provider");
            }
            else if constexpr (std::is_same_v<T, SaslGssapiConfig>)
            {
#if USE_KRB5
                if (mechanism_config.service_name.empty())
                    throw Exception(ErrorKind::Config, "The SASL/GSSAPI service name must not be empty");
#else
                throw Exception(ErrorKind::NotSupported, "SASL/GSSAPI requires a build with krb5 support");
#endif
            }
        },
        config);
}

}
