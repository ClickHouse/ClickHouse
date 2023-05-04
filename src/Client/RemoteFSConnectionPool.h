#pragma once

#include <Common/PoolBase.h>
#include <Client/RemoteFSConnection.h>

namespace DB
{


class RemoteFSConnectionPool : private PoolBase<RemoteFSConnection>
{
public:
    using Entry = PoolBase<RemoteFSConnection>::Entry;
    using Base = PoolBase<RemoteFSConnection>;

    RemoteFSConnectionPool(unsigned max_connections_,
            const String & host_,
            UInt16 port_,
            const String & disk_name_)
       : Base(max_connections_,
        &Poco::Logger::get("RemoteFSConnection (" + host_ + ":" + toString(port_) + ")")),
        host(host_),
        port(port_),
        disk_name(disk_name_)
    {
    }

    ~RemoteFSConnectionPool() override
    {
        LOG_TRACE(log, "Connection pool with remote disk {} destroyed", disk_name);
    }

    Entry get(const ConnectionTimeouts & timeouts, /// NOLINT
              bool force_connected = true)
    {
        Entry entry = Base::get(-1);

        if (force_connected)
            entry->forceConnected(timeouts);

        return entry;
    }

    const std::string & getHost() const
    {
        return host;
    }
    std::string getDescription() const
    {
        return host + ":" + toString(port);
    }

protected:
    /** Creates a new object to put in the pool. */
    RemoteFSConnectionPtr allocObject() override
    {
        return std::make_shared<RemoteFSConnection>(host, port, disk_name, conn_id.fetch_add(1));
    }

private:
    String host;
    UInt16 port;
    String disk_name;

    // TODO remove
    std::atomic<size_t> conn_id = 0;
};

}
