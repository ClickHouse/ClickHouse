#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include "Server/TCPProtocolStackData.h"

#if USE_SSL
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
    using StreamSocket = Poco::Net::StreamSocket;
public:
    explicit TLSHandler(const StreamSocket & socket, const std::string & conf_name_, TCPProtocolStackData & stack_data_)
        : Poco::Net::TCPServerConnection(socket)
        , conf_name(conf_name_)
        , stack_data(stack_data_)
    {}

    void run() override
    {
#if USE_SSL
        socket() = Poco::Net::SecureStreamSocket::attach(socket(), Poco::Net::SSLManager::instance().defaultServerContext());
        stack_data.socket = socket();
#else
        throw Exception{"SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }
private:
    std::string conf_name;
    TCPProtocolStackData & stack_data [[maybe_unused]];
};


}
