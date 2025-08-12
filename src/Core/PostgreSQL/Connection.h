#pragma once

#include "config.h"

#if USE_LIBPQXX

#include <pqxx/pqxx>
#include <Core/Types.h>
#include <Common/Logger.h>
#include <boost/noncopyable.hpp>

/** Methods to work with PostgreSQL connection object.
 * Should only be used in case there has to be a single connection object, which
 * is long-lived and there are no concurrent connection queries.
 */

namespace Poco { class Logger; }

namespace pqxx
{
    using ConnectionPtr = std::unique_ptr<pqxx::connection>;
}

namespace postgres
{

struct ConnectionInfo
{
    String connection_string;
    String host_port; /// For logs.
};

class Connection : private boost::noncopyable
{
public:
    class Lease
    {
    public:
        Lease(pqxx::connection & connection_);
        ~Lease();

        Lease(const Lease &);
        Lease & operator=(const Lease &);

        pqxx::connection & getRef() { return *connection; }

    private:
        pqxx::connection * connection;
    };

    explicit Connection(
        const ConnectionInfo & connection_info_,
        bool replication_ = false,
        size_t num_tries = 3);
    
    ~Connection();

    void execWithRetry(const std::function<void(pqxx::nontransaction &)> & exec);

    Lease getLease();

    void connect();

    void updateConnection();

    void tryUpdateConnection();

    bool isConnected() const { return connection != nullptr && connection->is_open(); }

    const ConnectionInfo & getConnectionInfo() { return connection_info; }

    String getInfoForLog() const { return connection_info.host_port; }

    void close();

private:

    pqxx::ConnectionPtr connection;
    ConnectionInfo connection_info;

    bool replication;
    size_t num_tries;

    LoggerPtr log;
};

using ConnectionPtr = std::unique_ptr<Connection>;

}

#endif
