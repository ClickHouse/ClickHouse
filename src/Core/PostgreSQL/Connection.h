#pragma once

#include <pqxx/pqxx> // Y_IGNORE
#include <Core/Types.h>
#include <boost/noncopyable.hpp>


namespace postgres
{
using ConnectionInfo = std::pair<String, String>;
using ConnectionPtr = std::unique_ptr<pqxx::connection>;

class Connection : private boost::noncopyable
{
public:
    Connection(const ConnectionInfo & connection_info_, bool replication_ = false, size_t num_tries = 3);

    void execWithRetry(const std::function<void(pqxx::nontransaction &)> & exec);

    pqxx::connection & getRef();

    void connect();

    const ConnectionInfo & getConnectionInfo() { return connection_info; }

private:
    ConnectionPtr connection;
    ConnectionInfo connection_info;

    bool replication;
    size_t num_tries;
};
}
