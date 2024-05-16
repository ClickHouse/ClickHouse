#include <Poco/Net/Utility.h>
#include <Poco/StringTokenizer.h>
#include <Server/TLSHandler.h>

DB::TLSHandler::TLSHandler(const StreamSocket & socket, const LayeredConfiguration & config, const std::string & prefix, TCPProtocolStackData & stack_data_)
    : Poco::Net::TCPServerConnection(socket)
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

        bool requireTLSv1 = config.getBool(prefix + SSLManager::CFG_REQUIRE_TLSV1, false);
        bool requireTLSv1_1 = config.getBool(prefix + SSLManager::CFG_REQUIRE_TLSV1_1, false);
        bool requireTLSv1_2 = config.getBool(prefix + SSLManager::CFG_REQUIRE_TLSV1_2, false);
        if (requireTLSv1_2)
            usage = Context::TLSV1_2_SERVER_USE;
        else if (requireTLSv1_1)
            usage = Context::TLSV1_1_SERVER_USE;
        else if (requireTLSv1)
            usage = Context::TLSV1_SERVER_USE;
        else
            usage = Context::SERVER_USE;

        params.dhParamsFile = config.getString(prefix + SSLManager::CFG_DH_PARAMS_FILE, "");
        params.ecdhCurve    = config.getString(prefix + SSLManager::CFG_ECDH_CURVE, "");

        std::string disabledProtocolsList = config.getString(prefix + SSLManager::CFG_DISABLE_PROTOCOLS, "");
        Poco::StringTokenizer dpTok(disabledProtocolsList, ";,", Poco::StringTokenizer::TOK_TRIM | Poco::StringTokenizer::TOK_IGNORE_EMPTY);
        disabledProtocols = 0;
        for (Poco::StringTokenizer::Iterator it = dpTok.begin(); it != dpTok.end(); ++it)
        {
            if (*it == "sslv2")
                disabledProtocols |= Context::PROTO_SSLV2;
            else if (*it == "sslv3")
                disabledProtocols |= Context::PROTO_SSLV3;
            else if (*it == "tlsv1")
                disabledProtocols |= Context::PROTO_TLSV1;
            else if (*it == "tlsv1_1")
                disabledProtocols |= Context::PROTO_TLSV1_1;
            else if (*it == "tlsv1_2")
                disabledProtocols |= Context::PROTO_TLSV1_2;
        }

	    extendedVerification = config.getBool(prefix + SSLManager::CFG_EXTENDED_VERIFICATION, false);
	    preferServerCiphers = config.getBool(prefix + SSLManager::CFG_PREFER_SERVER_CIPHERS, false);
    }
#endif
}
