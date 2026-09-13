#include <TLSSupport.h>

#if USE_SILK

#include <Common/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>

#if USE_SSL
#include <Server/CertificateReloader.h>

#include <Poco/Net/Context.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/Utility.h>
#include <Poco/StringTokenizer.h>
#endif


namespace DB::Proxy
{

#if USE_SSL

using Poco::Net::Context;
using SSLManager = Poco::Net::SSLManager;

namespace
{

int parseDisabledProtocols(const Poco::Util::AbstractConfiguration & config, const std::string & prefix)
{
    int disabled_protocols = 0;
    Poco::StringTokenizer tokenizer(
        config.getString(prefix + SSLManager::CFG_DISABLE_PROTOCOLS, ""),
        ";,", Poco::StringTokenizer::TOK_TRIM | Poco::StringTokenizer::TOK_IGNORE_EMPTY);
    for (const auto & token : tokenizer)
    {
        if (token == "sslv2") disabled_protocols |= Context::PROTO_SSLV2;
        else if (token == "sslv3") disabled_protocols |= Context::PROTO_SSLV3;
        else if (token == "tlsv1") disabled_protocols |= Context::PROTO_TLSV1;
        else if (token == "tlsv1_1") disabled_protocols |= Context::PROTO_TLSV1_1;
        else if (token == "tlsv1_2") disabled_protocols |= Context::PROTO_TLSV1_2;
    }
    return disabled_protocols;
}

}

Poco::Net::Context::Ptr makeServerTLSContext(const Poco::Util::AbstractConfiguration & config)
{
    const std::string prefix = SSLManager::CFG_SERVER_PREFIX;

    Context::Params params;
    params.privateKeyFile = config.getString(prefix + SSLManager::CFG_PRIV_KEY_FILE, "");
    params.certificateFile = config.getString(prefix + SSLManager::CFG_CERTIFICATE_FILE, params.privateKeyFile);
    params.caLocation = config.getString(prefix + SSLManager::CFG_CA_LOCATION, "");

    params.verificationMode = Context::VERIFY_NONE;
    if (config.hasProperty(prefix + SSLManager::CFG_VER_MODE))
        params.verificationMode = Poco::Net::Utility::convertVerificationMode(config.getString(prefix + SSLManager::CFG_VER_MODE));

    params.verificationDepth = config.getInt(prefix + SSLManager::CFG_VER_DEPTH, SSLManager::VAL_VER_DEPTH);
    params.loadDefaultCAs = config.getBool(prefix + SSLManager::CFG_ENABLE_DEFAULT_CA, SSLManager::VAL_ENABLE_DEFAULT_CA);
    params.cipherList = config.getString(prefix + SSLManager::CFG_CIPHER_LIST, SSLManager::VAL_CIPHER_LIST);
    params.dhParamsFile = config.getString(prefix + SSLManager::CFG_DH_PARAMS_FILE, "");
    params.ecdhCurve = config.getString(prefix + SSLManager::CFG_ECDH_CURVE, "");

    Context::Usage usage = Context::SERVER_USE;
    if (config.getBool(prefix + SSLManager::CFG_REQUIRE_TLSV1_2, false))
        usage = Context::TLSV1_2_SERVER_USE;
    else if (config.getBool(prefix + SSLManager::CFG_REQUIRE_TLSV1_1, false))
        usage = Context::TLSV1_1_SERVER_USE;
    else if (config.getBool(prefix + SSLManager::CFG_REQUIRE_TLSV1, false))
        usage = Context::TLSV1_SERVER_USE;

    Context::Ptr ctx = new Context(usage, params);
    ctx->disableProtocols(parseDisabledProtocols(config, prefix));

    if (config.getBool(prefix + SSLManager::CFG_EXTENDED_VERIFICATION, false))
        ctx->enableExtendedCertificateVerification(true);
    if (config.getBool(prefix + SSLManager::CFG_PREFER_SERVER_CIPHERS, false))
        ctx->preferServerCiphers();

    /// Install the per-connection certificate callback (also serves ACME-provisioned certificates)
    /// and enable hot reload of the key pair on config change.
    CertificateReloader::instance().tryLoad(config, ctx->sslContext(), prefix);

    return ctx;
}

Poco::Net::Context::Ptr makeClientTLSContext(const Poco::Util::AbstractConfiguration & config)
{
    const std::string prefix = SSLManager::CFG_CLIENT_PREFIX;

    Context::Params params;
    params.privateKeyFile = config.getString(prefix + SSLManager::CFG_PRIV_KEY_FILE, "");
    params.certificateFile = config.getString(prefix + SSLManager::CFG_CERTIFICATE_FILE, params.privateKeyFile);
    params.caLocation = config.getString(prefix + SSLManager::CFG_CA_LOCATION, "");

    /// The proxy trusts backends it was configured with; certificate verification of the backend leg
    /// defaults to relaxed and can be tightened through the `openSSL.client` section.
    params.verificationMode = Context::VERIFY_RELAXED;
    if (config.hasProperty(prefix + SSLManager::CFG_VER_MODE))
        params.verificationMode = Poco::Net::Utility::convertVerificationMode(config.getString(prefix + SSLManager::CFG_VER_MODE));

    params.verificationDepth = config.getInt(prefix + SSLManager::CFG_VER_DEPTH, SSLManager::VAL_VER_DEPTH);
    params.loadDefaultCAs = config.getBool(prefix + SSLManager::CFG_ENABLE_DEFAULT_CA, true);
    params.cipherList = config.getString(prefix + SSLManager::CFG_CIPHER_LIST, SSLManager::VAL_CIPHER_LIST);

    Context::Usage usage = Context::CLIENT_USE;
    if (config.getBool(prefix + SSLManager::CFG_REQUIRE_TLSV1_2, false))
        usage = Context::TLSV1_2_CLIENT_USE;
    else if (config.getBool(prefix + SSLManager::CFG_REQUIRE_TLSV1_1, false))
        usage = Context::TLSV1_1_CLIENT_USE;
    else if (config.getBool(prefix + SSLManager::CFG_REQUIRE_TLSV1, false))
        usage = Context::TLSV1_CLIENT_USE;

    Context::Ptr ctx = new Context(usage, params);
    ctx->disableProtocols(parseDisabledProtocols(config, prefix));

    if (config.getBool(prefix + SSLManager::CFG_EXTENDED_VERIFICATION, false))
        ctx->enableExtendedCertificateVerification(true);

    return ctx;
}

#endif

namespace
{

/// A bounds-checked cursor over a byte buffer, used to parse the ClientHello without over-reading.
struct Cursor
{
    const char * data = nullptr;
    size_t size = 0;
    size_t pos = 0;

    bool has(size_t n) const { return pos + n <= size; }
    UInt8 u8() { return static_cast<UInt8>(data[pos++]); }
    UInt32 u16() { UInt32 v = (static_cast<UInt8>(data[pos]) << 8) | static_cast<UInt8>(data[pos + 1]); pos += 2; return v; }
    UInt32 u24() { UInt32 v = (static_cast<UInt8>(data[pos]) << 16) | (static_cast<UInt8>(data[pos + 1]) << 8) | static_cast<UInt8>(data[pos + 2]); pos += 3; return v; }
    void skip(size_t n) { pos += n; }
};

}

std::optional<String> peekTLSClientHelloSNI(RecordingReader & reader)
{
    /// A `ClientHello` is allowed to span several handshake records, so the records are collected
    /// until the whole handshake message is buffered. The total is bounded: the peek happens before
    /// a backend is chosen, so a client must not be able to make the proxy buffer without limit.
    constexpr size_t max_handshake_bytes = 64 * 1024;

    String body;
    auto read_record = [&]
    {
        /// TLS record header: content type (1) + version (2) + length (2).
        if (!reader.ensure(5) || reader.peekByte() != 0x16)     /// Not a handshake record.
            return false;
        reader.skip(3);
        const size_t record_length = (static_cast<size_t>(reader.readByte()) << 8) | reader.readByte();
        if (record_length == 0 || body.size() + record_length > max_handshake_bytes || !reader.ensure(record_length))
            return false;
        body += reader.readFixed(record_length);
        return true;
    };

    /// Handshake header: type (1) + length (3).
    while (body.size() < 4)
        if (!read_record())
            return {};

    if (static_cast<UInt8>(body[0]) == 0x01)     /// A ClientHello: collect its continuation records.
    {
        const size_t message_end = 4
            + ((static_cast<size_t>(static_cast<UInt8>(body[1])) << 16)
                | (static_cast<size_t>(static_cast<UInt8>(body[2])) << 8)
                | static_cast<UInt8>(body[3]));
        if (message_end > max_handshake_bytes)
            return {};
        while (body.size() < message_end)
            if (!read_record())
                return {};
    }

    Cursor cursor{body.data(), body.size()};

    /// Handshake header: type (1) + length (3).
    if (!cursor.has(4) || cursor.u8() != 0x01)     /// Not a ClientHello.
        return {};
    UInt32 handshake_length = cursor.u24();
    if (!cursor.has(handshake_length))
        return {};

    /// client_version (2) + random (32).
    if (!cursor.has(2 + 32))
        return {};
    cursor.skip(2 + 32);

    /// session_id.
    if (!cursor.has(1))
        return {};
    cursor.skip(cursor.u8());

    /// cipher_suites.
    if (!cursor.has(2))
        return {};
    UInt32 cipher_suites_length = cursor.u16();
    if (!cursor.has(cipher_suites_length))
        return {};
    cursor.skip(cipher_suites_length);

    /// compression_methods.
    if (!cursor.has(1))
        return {};
    cursor.skip(cursor.u8());

    /// extensions.
    if (!cursor.has(2))
        return {};
    UInt32 extensions_length = cursor.u16();
    size_t extensions_end = std::min(cursor.pos + extensions_length, cursor.size);

    while (cursor.pos + 4 <= extensions_end)
    {
        UInt32 ext_type = cursor.u16();
        UInt32 ext_length = cursor.u16();
        if (cursor.pos + ext_length > extensions_end)
            return {};

        if (ext_type == 0x0000)     /// server_name.
        {
            Cursor sni{body.data() + cursor.pos, ext_length};
            if (!sni.has(2))
                return {};
            UInt32 list_length = sni.u16();
            size_t list_end = std::min(sni.pos + list_length, sni.size);
            while (sni.pos + 3 <= list_end)
            {
                UInt8 name_type = sni.u8();
                UInt32 name_length = sni.u16();
                if (sni.pos + name_length > list_end)
                    return {};
                if (name_type == 0x00)     /// host_name.
                    return String(body.data() + cursor.pos + sni.pos, name_length);
                sni.skip(name_length);
            }
            return {};
        }

        cursor.skip(ext_length);
    }

    return {};
}

}

#endif
