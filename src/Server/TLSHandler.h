#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include <Poco/SharedPtr.h>
#include <Common/Exception.h>
#include <Server/TCPProtocolStackData.h>

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
    using Context = Poco::Net::Context;
#endif
    using StreamSocket = Poco::Net::StreamSocket;
public:
    explicit TLSHandler(const StreamSocket & socket, const std::string & key_, const std::string & certificate_, TCPProtocolStackData & stack_data_)
        : Poco::Net::TCPServerConnection(socket)
        , key(key_)
        , certificate(certificate_)
        , stack_data(stack_data_)
    {}

    void run() override
    {
#if USE_SSL
        auto ctx = SSLManager::instance().defaultServerContext();
        if (!key.empty() && !certificate.empty())
            ctx = new Context(Context::Usage::SERVER_USE, key, certificate, ctx->getCAPaths().caLocation);
        socket() = SecureStreamSocket::attach(socket(), ctx);
        stack_data.socket = socket();
        stack_data.certificate = certificate;
#else
        throw Exception{"SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }
private:
    std::string key [[maybe_unused]];
    std::string certificate [[maybe_unused]];
    TCPProtocolStackData & stack_data [[maybe_unused]];
};


}
