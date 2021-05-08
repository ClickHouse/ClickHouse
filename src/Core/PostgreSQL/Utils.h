#pragma once

#include <pqxx/pqxx> // Y_IGNORE
#include <Core/Types.h>
#include "Connection.h"

namespace pqxx
{
    using ReadTransaction = pqxx::read_transaction;
    using ReplicationTransaction = pqxx::transaction<isolation_level::repeatable_read, write_policy::read_only>;
}


namespace postgres
{

ConnectionInfo formatConnectionString(String dbname, String host, UInt16 port, String user, String password);

Connection createReplicationConnection(const ConnectionInfo & connection_info);

template <typename T>
class Transaction
{
public:
    Transaction(pqxx::connection & connection) : transaction(connection) {}

    ~Transaction() { transaction.commit(); }

    T & getRef() { return transaction; }

    void exec(const String & query) { transaction.exec(query); }

private:
    T transaction;
};

}
