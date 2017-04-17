#pragma once

#include <Core/Types.h>
#include <Interpreters/InterserverIOHandler.h>
#include <IO/WriteBuffer.h>

namespace DB
{

class Context;

namespace RemoteDiskSpaceMonitor
{

/** Service to get information about free disk space.
  */
class Service final : public InterserverIOEndpoint
{
public:
    Service(const Context & context_);
    Service(const Service &) = delete;
    Service & operator=(const Service &) = delete;
    std::string getId(const std::string & node_id) const override;
    void processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out, Poco::Net::HTTPServerResponse & response) override;

private:
    const Context & context;
};

/** Client to get information about free space on a remote disk.
  */
class Client final
{
public:
    Client() = default;
    Client(const Client &) = delete;
    Client & operator=(const Client &) = delete;
    size_t getFreeSpace(const InterserverIOEndpointLocation & location) const;
    void cancel() { is_cancelled = true; }

private:
    std::atomic<bool> is_cancelled{false};
};

}

}
