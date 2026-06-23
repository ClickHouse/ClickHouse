#include <Server/TLSHandler.h>

#include <Poco/SharedPtr.h>
#include <Common/Exception.h>


#if USE_SSL
#    include <Poco/Net/Utility.h>
#    include <Poco/StringTokenizer.h>
#    include <Server/CertificateReloader.h>
#endif

#if !defined(USE_SSL) || USE_SSL == 0
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}
#endif

DB::TLSHandler::TLSHandler(
    const StreamSocket & socket,
    [[maybe_unused]] const LayeredConfiguration & config_,
    [[maybe_unused]] const std::string & prefix_,
    TCPProtocolStackData & stack_data_)
    : Poco::Net::TCPServerConnection(socket)
#if USE_SSL
    , config(config_)
    , prefix(prefix_)
#endif
    , stack_data(stack_data_)
{
#if USE_SSL
    params.privateKeyFile = config.getString(prefix + SSLManager::CFG_PRIV_KEY_FILE, "");
    params.certificateFile = config.getString(prefix + SSLManager::CFG_CERTIFICATE_FILE, params.privateKeyFile);
    if (!params.privateKeyFile.empty() && !params.certificateFile.empty())
    {
        // for backwards compatibility
        auto ctx = SSLManager::instance().defaultServerContext();
        params.caLocation = config.getString(prefix + SSLManager::CFG_CA_LOCATION, ctx->getCAPaths().caLocation);

        // optional options for which we have defaults defined
        params.verificationMode = SSLManager::VAL_VER_MODE;
        if (config.hasProperty(prefix + SSLManager::CFG_VER_MODE))
        {
            // either: none, relaxed, strict, once
            std::string mode = config.getString(prefix + SSLManager::CFG_VER_MODE);
            params.verificationMode = Poco::Net::Utility::convertVerificationMode(mode);
        }

        params.verificationDepth = config.getInt(prefix + SSLManager::CFG_VER_DEPTH, SSLManager::VAL_VER_DEPTH);
        params.loadDefaultCAs = config.getBool(prefix + SSLManager::CFG_ENABLE_DEFAULT_CA, SSLManager::VAL_ENABLE_DEFAULT_CA);
        params.cipherList = config.getString(prefix + SSLManager::CFG_CIPHER_LIST, SSLManager::VAL_CIPHER_LIST);
        params.cipherList = config.getString(prefix + SSLManager::CFG_CYPHER_LIST, params.cipherList); // for backwards compatibility

        bool require_tlsv1 = config.getBool(prefix + SSLManager::CFG_REQUIRE_TLSV1, false);
        bool require_tlsv1_1 = config.getBool(prefix + SSLManager::CFG_REQUIRE_TLSV1_1, false);
        bool require_tlsv1_2 = config.getBool(prefix + SSLManager::CFG_REQUIRE_TLSV1_2, false);
        if (require_tlsv1_2)
            usage = Context::TLSV1_2_SERVER_USE;
        else if (require_tlsv1_1)
            usage = Context::TLSV1_1_SERVER_USE;
        else if (require_tlsv1)
            usage = Context::TLSV1_SERVER_USE;
        else
            usage = Context::SERVER_USE;

        params.dhParamsFile = config.getString(prefix + SSLManager::CFG_DH_PARAMS_FILE, "");
        params.ecdhCurve    = config.getString(prefix + SSLManager::CFG_ECDH_CURVE, "");

        std::string disabled_protocols_list = config.getString(prefix + SSLManager::CFG_DISABLE_PROTOCOLS, "");
        Poco::StringTokenizer dp_tok(disabled_protocols_list, ";,", Poco::StringTokenizer::TOK_TRIM | Poco::StringTokenizer::TOK_IGNORE_EMPTY);
        disabled_protocols = 0;
        for (const auto & token : dp_tok)
        {
            if (token == "sslv2")
                disabled_protocols |= Context::PROTO_SSLV2;
            else if (token == "sslv3")
                disabled_protocols |= Context::PROTO_SSLV3;
            else if (token == "tlsv1")
                disabled_protocols |= Context::PROTO_TLSV1;
            else if (token == "tlsv1_1")
                disabled_protocols |= Context::PROTO_TLSV1_1;
            else if (token == "tlsv1_2")
                disabled_protocols |= Context::PROTO_TLSV1_2;
        }

        extended_verification = config.getBool(prefix + SSLManager::CFG_EXTENDED_VERIFICATION, false);
        prefer_server_ciphers = config.getBool(prefix + SSLManager::CFG_PREFER_SERVER_CIPHERS, false);
    }
#endif
}


void DB::TLSHandler::run()
{
#if USE_SSL
    auto ctx = SSLManager::instance().defaultServerContext();
    if (!params.privateKeyFile.empty() && !params.certificateFile.empty())
    {
        ctx = SSLManager::instance().getCustomServerContext(prefix);
        if (!ctx)
        {
            ctx = new Context(usage, params);
            ctx->disableProtocols(disabled_protocols);
            ctx->enableExtendedCertificateVerification(extended_verification);
            if (prefer_server_ciphers)
                ctx->preferServerCiphers();
            CertificateReloader::instance().tryLoad(config, ctx->sslContext(), prefix);
            ctx = SSLManager::instance().setCustomServerContext(prefix, ctx);
        }
    }
    socket() = SecureStreamSocket::attach(socket(), ctx);
    stack_data.socket = socket();
    stack_data.certificate = params.certificateFile;
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.");
#endif
}
