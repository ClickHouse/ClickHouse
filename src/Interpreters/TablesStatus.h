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

/// Bounds on how much of a `TablesStatusRequest` its sender can make the server deserialize.
struct TablesStatusRequestLimits
{
    /// Maximum number of tables the request may ask about.
    size_t max_tables;
    /// Maximum size of each `database`/`table` name. Needed on top of `max_tables`, because
    /// `readStringBinary` allocates the declared size of a name before reading its bytes, so a
    /// bound on the number of names alone does not bound the memory the request can ask for.
    size_t max_name_size;
};

/// A `TablesStatusRequest` from an interserver peer asks about the single table behind the
/// `Distributed` table being read, so these are generous - they bound a hostile request, not a
/// legitimate one. They are needed because the request body is deserialized before the peer has
/// proven knowledge of the cluster secret: the hash covers the body, so the body is read before the
/// hash is validated, and an older peer sends no hash at all. Worst case per request is
/// `max_tables * 2 * max_name_size` = 16 MiB, and that allocation is memory-tracked.
static constexpr TablesStatusRequestLimits INTERSERVER_TABLES_STATUS_REQUEST_LIMITS
{
    .max_tables = 1024,
    .max_name_size = 8192,
};

struct TablesStatusRequest
{
    std::unordered_set<QualifiedTableName> tables;

    void write(WriteBuffer & out, UInt64 server_protocol_revision) const;
    /// See `INTERSERVER_TABLES_STATUS_REQUEST_LIMITS` for what an interserver peer is allowed;
    /// an ordinary authenticated client keeps the generic string and array limits.
    void read(ReadBuffer & in, UInt64 client_protocol_revision, const TablesStatusRequestLimits & limits);

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
