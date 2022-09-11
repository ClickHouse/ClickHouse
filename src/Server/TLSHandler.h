#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/SSLManager.h>
#include "Server/TCPProtocolStackData.h"


namespace DB
{


class TLSHandler : public Poco::Net::TCPServerConnection
{
    using StreamSocket = Poco::Net::StreamSocket;
    using SecureStreamSocket = Poco::Net::SecureStreamSocket;
public:
    explicit TLSHandler(const StreamSocket & socket, const std::string & conf_name_, TCPProtocolStackData & stack_data_)
        : Poco::Net::TCPServerConnection(socket)
        , conf_name(conf_name_)
        , stack_data(stack_data_)
    {}

    void run() override
    {
        socket() = SecureStreamSocket::attach(socket(), Poco::Net::SSLManager::instance().defaultServerContext());
        stack_data.socket = socket();
    }
private:
    std::string conf_name;
    TCPProtocolStackData & stack_data;
};


}
