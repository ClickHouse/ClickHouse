#pragma once

#include <pqxx/pqxx> // Y_IGNORE
#include <Core/Types.h>
#include <boost/noncopyable.hpp>

/* Methods to work with PostgreSQL connection object.
 * Should only be used in case there has to be a single connection object, which
 * is long-lived and there are no concurrent connection queries.
 * Now only use case - for replication handler for replication from PostgreSQL.
 * In all other integration engine use pool with failover.
 **/

namespace Poco { class Logger; }

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

    void tryUpdateConnection();

    const ConnectionInfo & getConnectionInfo() { return connection_info; }

private:
    void updateConnection();

    ConnectionPtr connection;
    ConnectionInfo connection_info;

    bool replication;
    size_t num_tries;

    Poco::Logger * log;
};
}
