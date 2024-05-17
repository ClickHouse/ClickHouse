#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include <Poco/SharedPtr.h>
#include <Common/Exception.h>
#include <Server/TCPProtocolStackData.h>
#include <Poco/Util/LayeredConfiguration.h>

#include "config.h"

#if USE_SSL
#   include <Poco/Net/Context.h>
#   include <Poco/Net/SecureStreamSocket.h>
#   include <Poco/Net/SSLManager.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

class TLSHandler : public Poco::Net::TCPServerConnection
{
#if USE_SSL
    using SecureStreamSocket = Poco::Net::SecureStreamSocket;
    using SSLManager = Poco::Net::SSLManager;
#endif
    using Context = Poco::Net::Context;
    using StreamSocket = Poco::Net::StreamSocket;
    using LayeredConfiguration = Poco::Util::LayeredConfiguration;
public:
    explicit TLSHandler(const StreamSocket & socket, const LayeredConfiguration & config, const std::string & prefix, TCPProtocolStackData & stack_data_);

    void run() override
    {
#if USE_SSL
        auto ctx = SSLManager::instance().defaultServerContext();
        if (!params.privateKeyFile.empty() && !params.certificateFile.empty())
        {
            ctx = new Context(usage, params);
            ctx->disableProtocols(disabled_protocols);
            ctx->enableExtendedCertificateVerification(extended_verification);
            if (prefer_server_ciphers)
                ctx->preferServerCiphers();
        }
        socket() = SecureStreamSocket::attach(socket(), ctx);
        stack_data.socket = socket();
        stack_data.certificate = params.certificateFile;
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.");
#endif
    }
private:
    Context::Params params [[maybe_unused]];
    Context::Usage usage [[maybe_unused]];
    int disabled_protocols = 0;
    bool extended_verification = false;
    bool prefer_server_ciphers = false;
    TCPProtocolStackData & stack_data [[maybe_unused]];
};


}
