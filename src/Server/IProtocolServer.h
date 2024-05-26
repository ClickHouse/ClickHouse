#include <string>
#include <base/types.h>

namespace DB
{

class IProtocolServer
{
public:
    virtual ~IProtocolServer() = default;
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual bool isStopping() const = 0;
    virtual UInt16 portNumber() const = 0;
    virtual size_t currentConnections() const = 0;
    virtual size_t currentThreads() const = 0;
    virtual const std::string& getListenHost() const { return listen_host; }
    virtual const std::string& getPortName() const { return port_name; }
    virtual const std::string& getDescription() const { return description; }


private:
    std::string listen_host;
    std::string port_name;
    std::string description;
    // std::unique_ptr<ProtocolServer> protocol_server;
};

}
