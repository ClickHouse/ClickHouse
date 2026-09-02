#pragma once

#include <Poco/Net/TCPServerConnection.h>
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

class TLSHandler : public Poco::Net::TCPServerConnection
{
#if USE_SSL
    using SecureStreamSocket = Poco::Net::SecureStreamSocket;
    using SSLManager = Poco::Net::SSLManager;
    using Context = Poco::Net::Context;
#endif
    using StreamSocket = Poco::Net::StreamSocket;
    using LayeredConfiguration = Poco::Util::LayeredConfiguration;
public:
    explicit TLSHandler(const StreamSocket & socket, const LayeredConfiguration & config_, const std::string & prefix_, TCPProtocolStackData & stack_data_);

    void run() override;

private:
#if USE_SSL
    Context::Params params [[maybe_unused]];
    Context::Usage usage [[maybe_unused]];
    int disabled_protocols = 0;
    bool extended_verification = false;
    bool prefer_server_ciphers = false;
    const LayeredConfiguration & config [[maybe_unused]];
    std::string prefix [[maybe_unused]];
#endif
    TCPProtocolStackData & stack_data [[maybe_unused]];
};


}
