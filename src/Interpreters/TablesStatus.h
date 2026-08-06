#pragma once

#include <unordered_set>
#include <unordered_map>

#include <base/types.h>
#include <Core/QualifiedTableName.h>

namespace DB
{

namespace ErrorCodes
{
}

class ReadBuffer;
class WriteBuffer;


/// The following are request-response messages for TablesStatus request of the client-server protocol.
/// Client can ask for about a set of tables and the server will respond with the following information for each table:
/// - Is the table Replicated?
/// - If yes, replication delay for that table.
///
/// For nonexistent tables there will be no TableStatus entry in the response.

struct TableStatus
{
    bool is_replicated = false;
    UInt32 absolute_delay = 0;
    /// Used to filter such nodes out for INSERTs
    bool is_readonly = false;

    void write(WriteBuffer & out, UInt64 client_protocol_revision) const;
    void read(ReadBuffer & in, UInt64 server_protocol_revision);
};

/// A `TablesStatusRequest` from an interserver peer asks about the single table behind the
/// `Distributed` table being read, so this is generous. It bounds how much of the request an
/// interserver peer can make the server deserialize before the peer has proven knowledge of the
/// cluster secret (the hash covers the request body, so the body is read before it is validated).
static constexpr size_t MAX_TABLES_IN_INTERSERVER_STATUS_REQUEST = 1024;

struct TablesStatusRequest
{
    std::unordered_set<QualifiedTableName> tables;

    void write(WriteBuffer & out, UInt64 server_protocol_revision) const;
    /// `max_tables` bounds how much the peer can make us deserialize; see
    /// `MAX_TABLES_IN_INTERSERVER_STATUS_REQUEST` for the interserver limit.
    void read(ReadBuffer & in, UInt64 client_protocol_revision, size_t max_tables);

    /// Deterministic, order-independent digest of `tables` for the interserver auth hash.
    std::string getAuthDigest() const;
};

struct TablesStatusResponse
{
    std::unordered_map<QualifiedTableName, TableStatus> table_states_by_id;

    void write(WriteBuffer & out, UInt64 client_protocol_revision) const;
    void read(ReadBuffer & in, UInt64 server_protocol_revision);
};

}
