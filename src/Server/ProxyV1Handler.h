#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include <Server/IServer.h>
#include <Server/TCPProtocolStackData.h>
#include <Common/logger_useful.h>


namespace DB
{

class ProxyV1Handler : public Poco::Net::TCPServerConnection
{
    using StreamSocket = Poco::Net::StreamSocket;
public:
    explicit ProxyV1Handler(const StreamSocket & socket, IServer & server_, const std::string & conf_name_, TCPProtocolStackData & stack_data_)
        : Poco::Net::TCPServerConnection(socket), log(&Poco::Logger::get("ProxyV1Handler")), server(server_), conf_name(conf_name_), stack_data(stack_data_) {}

    void run() override;

protected:
    bool readWord(int max_len, std::string & word, bool & eol);

private:
    Poco::Logger * log;
    IServer & server;
    std::string conf_name;
    TCPProtocolStackData & stack_data;
};

}
