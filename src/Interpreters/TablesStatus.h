#pragma once

#include <cstdint>
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

/// How many tables an interserver peer may ask about. The request body is deserialized before the
/// peer is authenticated, so the count needs a tighter bound than the generic one. The only producer,
/// `ConnectionEstablisher`, asks about a single table.
///
/// Name length is not bounded: names are read with `readStringBinaryGrowing`, so a declared size is
/// not allocated unless the bytes actually arrive, and ClickHouse has no general table-name limit to
/// enforce here.
static constexpr size_t MAX_TABLES_IN_INTERSERVER_STATUS_REQUEST = 64;

/// Who sent the request, which is what the bound above follows from. A source rather than the limit
/// itself, so that a call site cannot hand an interserver connection the generic bound by mistake -
/// the mapping lives in one place, `TablesStatusRequest::read`.
enum class TablesStatusRequestSource : uint8_t
{
    /// An authenticated client: the generic array bound, as before this bound existed.
    Client,
    /// An interserver peer, bounded by `MAX_TABLES_IN_INTERSERVER_STATUS_REQUEST`.
    InterserverPeer,
};

struct TablesStatusRequest
{
    std::unordered_set<QualifiedTableName> tables;

    void write(WriteBuffer & out, UInt64 server_protocol_revision) const;
    void read(ReadBuffer & in, UInt64 client_protocol_revision, TablesStatusRequestSource source);

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
