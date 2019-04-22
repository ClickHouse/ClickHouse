#pragma once

#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Crypto/X509Certificate.h>
#include <common/logger_useful.h>
#include "IServer.h"
#include "MySQLHandler.h"

namespace Poco { class Logger; }

namespace DB
{

    class MySQLHandlerFactory : public Poco::Net::TCPServerConnectionFactory
    {
    private:
        IServer & server;
        Poco::Logger * log;
        RSA * public_key = nullptr, * private_key = nullptr;

    public:
        explicit MySQLHandlerFactory(IServer & server_)
            : server(server_)
            , log(&Logger::get("MySQLHandlerFactory"))
        {
            /// Reading rsa keys for SHA256 authentication plugin.
            const Poco::Util::LayeredConfiguration & config = Poco::Util::Application::instance().config();
            String certificateFileProperty = "openSSL.server.certificateFile";
            String privateKeyFileProperty = "openSSL.server.privateKeyFile";

            if (!config.has(certificateFileProperty))
            {
                LOG_INFO(log, "Certificate file is not set.");
                generateRSAKeys();
                return;
            }
            if (!config.has(privateKeyFileProperty)) {
                LOG_INFO(log, "Private key file is not set.");
                generateRSAKeys();
                return;
            }

            String certificateFile = config.getString(certificateFileProperty);
            FILE * fp = fopen(certificateFile.data(), "r");
            if (fp == nullptr) {
                LOG_WARNING(log, "Cannot open certificate file: " << certificateFile << ".");
                generateRSAKeys();
                return;
            }
            X509* x509 = PEM_read_X509(fp, nullptr, nullptr, nullptr);
            EVP_PKEY* p = X509_get_pubkey(x509);
            public_key = EVP_PKEY_get1_RSA(p);
            X509_free(x509);
            fclose(fp);

            String privateKeyFile = config.getString(privateKeyFileProperty);
            fp = fopen(privateKeyFile.data(), "r");
            if (fp == nullptr) {
                LOG_WARNING(log, "Cannot open private key file " << privateKeyFile << ".");
                generateRSAKeys();
                return;
            }
            private_key = PEM_read_RSAPrivateKey(fp, nullptr, nullptr, nullptr);
            fclose(fp);
        }

        void generateRSAKeys()
        {
            LOG_INFO(log, "Generating new RSA key.");
            RSA *rsa = RSA_new();
            if (rsa == nullptr) {
                throw Exception("Failed to allocate RSA key.", 1002);
            }
            BIGNUM *e = BN_new();
            if (!e) {
                RSA_free(rsa);
                throw Exception("Failed to allocate BIGNUM.", 1002);
            }
            if (!BN_set_word(e, 65537) || !RSA_generate_key_ex(rsa, 2048, e, nullptr))
            {
                RSA_free(rsa);
                BN_free(e);
                throw Exception("Failed to generate RSA key.", 1002);
            }
            BN_free(e);

            public_key = rsa;
            private_key = RSAPrivateKey_dup(rsa);
        }

        Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
        {
            LOG_TRACE(log, "MySQL connection. Address: " << socket.peerAddress().toString());
            return new MySQLHandler(server, socket, public_key, private_key);
        }

        ~MySQLHandlerFactory() override {
            RSA_free(public_key);
            RSA_free(private_key);
        }
    };

}
