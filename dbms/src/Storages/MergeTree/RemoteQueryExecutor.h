#pragma once

#include <Interpreters/InterserverIOHandler.h>
#include <IO/WriteBuffer.h>

namespace DB
{

class Context;

namespace RemoteQueryExecutor
{

/** Service for executing SQL queries.
  */
class Service final : public InterserverIOEndpoint
{
public:
    Service(Context & context_);
    Service(const Service &) = delete;
    Service & operator=(const Service &) = delete;
    std::string getId(const std::string & node_id) const override;
    void processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out, Poco::Net::HTTPServerResponse & response) override;

private:
    Context & context;
};

/** Client for remote execution of SQL queries.
  */
class Client final
{
public:
    Client() = default;
    Client(const Client &) = delete;
    Client & operator=(const Client &) = delete;
    bool executeQuery(const InterserverIOEndpointLocation & location, const std::string & query);
    void cancel() { is_cancelled = true; }

private:
    std::atomic<bool> is_cancelled{false};
};

}

}
