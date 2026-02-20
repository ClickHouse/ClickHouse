#include "MySQLHandlerFactory.h"
#include <Common/OpenSSLHelpers.h>
#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Poco/Util/Application.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>
#include <Server/MySQLHandler.h>

#if USE_SSL
#    include <Poco/Net/SSLManager.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_CLOSE_FILE;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int OPENSSL_ERROR;
}

MySQLHandlerFactory::MySQLHandlerFactory(IServer & server_, const ProfileEvents::Event & read_event_, const ProfileEvents::Event & write_event_)
    : server(server_)
    , log(getLogger("MySQLHandlerFactory"))
    , read_event(read_event_)
    , write_event(write_event_)
{
#if USE_SSL
    try
    {
        Poco::Net::SSLManager::instance().defaultServerContext();
    }
    catch (...)
    {
        LOG_TRACE(log, "Failed to create SSL context. SSL will be disabled. Error: {}", getCurrentExceptionMessage(false));
        ssl_enabled = false;
    }

    /// Reading rsa keys for SHA256 authentication plugin.
    try
    {
        readRSAKeys();
    }
    catch (...)
    {
        LOG_TRACE(log, "Failed to read RSA key pair from server certificate. Error: {}", getCurrentExceptionMessage(false));
        generateRSAKeys();
    }
#endif
}

#if USE_SSL
void MySQLHandlerFactory::readRSAKeys()
{
    const Poco::Util::LayeredConfiguration & config = Poco::Util::Application::instance().config();
    String certificate_file_property = "openSSL.server.certificateFile";
    String private_key_file_property = "openSSL.server.privateKeyFile";

    if (!config.has(certificate_file_property))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Certificate file is not set.");

    if (!config.has(private_key_file_property))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Private key file is not set.");

    {
        String certificate_file = config.getString(certificate_file_property);
        FILE * fp = fopen(certificate_file.data(), "r");
        if (fp == nullptr)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Cannot open certificate file: {}.", certificate_file);
        SCOPE_EXIT(if (0 != fclose(fp)) throw ErrnoException(
                       ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file with the certificate in MySQLHandlerFactory"););

        X509 * x509 = PEM_read_X509(fp, nullptr, nullptr, nullptr);
        SCOPE_EXIT(X509_free(x509));
        if (x509 == nullptr)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Failed to read PEM certificate from {}. Error: {}", certificate_file, getOpenSSLErrors());

        EVP_PKEY * p = X509_get_pubkey(x509);
        if (p == nullptr)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Failed to get RSA key from X509. Error: {}", getOpenSSLErrors());
        SCOPE_EXIT(EVP_PKEY_free(p));

        public_key.reset(EVP_PKEY_get1_RSA(p));
        if (public_key.get() == nullptr)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Failed to get RSA key from ENV_PKEY. Error: {}", getOpenSSLErrors());
    }

    {
        String private_key_file = config.getString(private_key_file_property);

        FILE * fp = fopen(private_key_file.data(), "r");
        if (fp == nullptr)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Cannot open private key file {}.", private_key_file);
        SCOPE_EXIT(if (0 != fclose(fp)) throw ErrnoException(
                       ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file with the certificate in MySQLHandlerFactory"););

        private_key.reset(PEM_read_RSAPrivateKey(fp, nullptr, nullptr, nullptr));
        if (!private_key)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Failed to read RSA private key from {}. Error: {}", private_key_file, getOpenSSLErrors());
    }
}

void MySQLHandlerFactory::generateRSAKeys()
{
    LOG_TRACE(log, "Generating new RSA key pair.");
    public_key.reset(RSA_new());
    if (!public_key)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failed to allocate RSA key. Error: {}", getOpenSSLErrors());

    BIGNUM * e = BN_new();
    if (!e)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failed to allocate BIGNUM. Error: {}", getOpenSSLErrors());
    SCOPE_EXIT(BN_free(e));

    if (!BN_set_word(e, 65537) || !RSA_generate_key_ex(public_key.get(), 2048, e, nullptr))
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failed to generate RSA key. Error: {}", getOpenSSLErrors());

    private_key.reset(RSAPrivateKey_dup(public_key.get()));
    if (!private_key)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failed to copy RSA key. Error: {}", getOpenSSLErrors());
}
#endif

Poco::Net::TCPServerConnection * MySQLHandlerFactory::createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    uint32_t connection_id = last_connection_id++;
    LOG_TRACE(log, "MySQL connection. Id: {}. Address: {}", connection_id, socket.peerAddress().toString());
#if USE_SSL
    return new MySQLHandlerSSL(server, tcp_server, socket, ssl_enabled, connection_id, *public_key, *private_key);
#else
    return new MySQLHandler(server, tcp_server, socket, ssl_enabled, connection_id);
#endif

}

}
