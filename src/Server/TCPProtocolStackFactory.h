#pragma once

#include <Server/TCPServerConnectionFactory.h>
#include <Server/IServer.h>
#include <Server/TCPProtocolStackHandler.h>
#include <Poco/Logger.h>
#include <Poco/Net/NetException.h>
#include <Common/logger_useful.h>
#include <Access/Common/AllowedClientHosts.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ADDRESS_PATTERN_TYPE;
    extern const int IP_ADDRESS_NOT_ALLOWED;
}


class TCPProtocolStackFactory : public TCPServerConnectionFactory
{
private:
    IServer & server [[maybe_unused]];
    LoggerPtr log;
    std::string conf_name;
    std::vector<TCPServerConnectionFactory::Ptr> stack;
    AllowedClientHosts allowed_client_hosts;

    class DummyTCPHandler : public Poco::Net::TCPServerConnection
    {
    public:
        using Poco::Net::TCPServerConnection::TCPServerConnection;
        void run() override {}
    };

public:
    template <typename... T>
    explicit TCPProtocolStackFactory(IServer & server_, const std::string & conf_name_, T... factory)
        : server(server_), log(getLogger("TCPProtocolStackFactory")), conf_name(conf_name_), stack({factory...})
    {
        const auto & config = server.config();
        /// Fill list of allowed hosts.
        const auto networks_config = conf_name + ".networks";
        if (config.has(networks_config))
        {
            Poco::Util::AbstractConfiguration::Keys keys;
            config.keys(networks_config, keys);
            for (const String & key : keys)
            {
                String value = config.getString(networks_config + "." + key);
                if (key.starts_with("ip"))
                    allowed_client_hosts.addSubnet(value);
                else if (key.starts_with("host_regexp"))
                    allowed_client_hosts.addNameRegexp(value);
                else if (key.starts_with("host"))
                    allowed_client_hosts.addName(value);
                else
                    throw Exception(ErrorCodes::UNKNOWN_ADDRESS_PATTERN_TYPE, "Unknown address pattern type: {}", key);
            }
        }
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override
    {
        if (!allowed_client_hosts.empty() && !allowed_client_hosts.contains(socket.peerAddress().host()))
            throw Exception(ErrorCodes::IP_ADDRESS_NOT_ALLOWED, "Connections from {} are not allowed", socket.peerAddress().toString());

        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());
            return new TCPProtocolStackHandler(server, tcp_server, socket, stack, conf_name);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }

    void append(TCPServerConnectionFactory::Ptr factory)
    {
        stack.push_back(std::move(factory));
    }

    size_t size() { return stack.size(); }
    bool empty() { return stack.empty(); }
};


}
