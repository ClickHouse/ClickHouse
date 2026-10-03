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

/// What an interserver peer is allowed to send. `ConnectionEstablisher` is the only producer of a
/// `TablesStatusRequest`, and it asks about exactly one table - the remote table behind the
/// `Distributed` table being read - so this leaves ample headroom (including for the "request
/// status for joined tables also" TODO there) while still bounding a hostile request.
///
/// A bound is needed because the body is deserialized before the peer has proven knowledge of the
/// cluster secret. That happens on the signed path by construction - the hash covers the body, so
/// the body has to be read to recompute the digest - and on the unsigned path whenever the request
/// is not rejected outright (`interserver_tables_status_require_auth`).
///
/// The parsed request is at most `max_tables * 2 * max_name_size` = 512 KiB. On the signed path the
/// transient peak is a small multiple of that, and not all of it is tracked: `getAuthDigest` also
/// builds a sorted vector of encoded entries and a concatenation of them in plain `std::string`s,
/// which allocate through `allocNoThrow`; only the final copy into the caller's
/// `StringWithMemoryTracking` goes through the throwing memory tracker.
static constexpr TablesStatusRequestLimits INTERSERVER_TABLES_STATUS_REQUEST_LIMITS
{
    .max_tables = 64,
    .max_name_size = 4096,
};

/// Who sent the request, which is what its bounds follow from. A source rather than the limits
/// themselves, so that a call site cannot hand an interserver connection the generous profile by
/// mistake - the mapping lives in one place, `TablesStatusRequest::read`.
enum class TablesStatusRequestSource : uint8_t
{
    /// An authenticated client: the generic string and array limits, as before these bounds existed.
    Client,
    /// An interserver peer, whose request is deserialized before it has proven knowledge of the
    /// cluster secret. Bounded by `INTERSERVER_TABLES_STATUS_REQUEST_LIMITS`.
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
