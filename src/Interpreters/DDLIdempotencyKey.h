#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

#include <memory>
#include <optional>
#include <vector>

namespace zkutil
{
class ZooKeeper;
}

namespace Coordination
{
struct Request;
using RequestPtr = std::shared_ptr<Request>;
using Requests = std::vector<RequestPtr>;
}

namespace DB
{

class IInterpreter;
struct BlockIO;
using ZooKeeperPtr = std::shared_ptr<zkutil::ZooKeeper>;

/// The `idempotency_key` of a client DDL query on a `Replicated` database. The multi that commits the query's log entry
/// also creates the key node, or replaces an expired one, so a live node means that a statement with this key has
/// committed.
struct DDLIdempotencyKey
{
    enum class State : uint8_t
    {
        Pending, /// No log entry was made yet.
        Enqueued, /// The query reached `tryEnqueueReplicatedDDL`, so it may have made a log entry.
        Answered, /// The query was answered from the key node of an earlier attempt.
    };

    String key;
    /// Identifies the statement and the user: the key of another statement or user is IDEMPOTENCY_KEY_MISMATCH.
    String digest;
    /// Only the query thread touches it.
    mutable State state = State::Pending;
};

/// Data of `<zookeeper_path>/idempotency/<hash of the key>`. Later versions may append lines, which `parse` ignores.
struct DDLIdempotencyKeyNode
{
    static constexpr UInt64 VERSION = 1;

    String entry;
    String digest;
    UInt64 ttl_sec = 0;

    String toString() const;
    static DDLIdempotencyKeyNode parse(const String & data);
};

String getDDLIdempotencyKeyNodeName(const String & key);

/// An expired node is reported as absent; then `expired_version` (if given) is set to its version. `now` is in seconds.
std::optional<DDLIdempotencyKeyNode> tryReadDDLIdempotencyKeyNode(
    const ZooKeeperPtr & zookeeper, const String & path, Int64 now, std::optional<int32_t> * expired_version = nullptr);

bool isDDLIdempotencyKeyNodeExpired(Int64 mtime_ms, UInt64 ttl_sec, Int64 now);

/// The current time of Keeper in seconds, read from a write to `keys_path`, the parent of the key nodes, so that the
/// clock of a replica does not expire keys. Empty if the parent does not exist, i.e. no key was ever committed.
std::optional<Int64> getDDLIdempotencyKeeperTime(const ZooKeeperPtr & zookeeper, const String & keys_path);

/// Adds the requests that commit the key node with the log entry: they create it, or replace the expired node of version
/// `expired_version`, and update its parent, which fences `tryRemoveExpiredDDLIdempotencyKeyNode`.
void addDDLIdempotencyKeyCommitRequests(
    Coordination::Requests & ops, const String & path, const DDLIdempotencyKeyNode & node, std::optional<int32_t> expired_version);

/// A key node that has expired, with the version of its parent read before it.
struct ExpiredDDLIdempotencyKeyNode
{
    int32_t version = 0;
    int32_t parent_version = 0;
};

std::optional<ExpiredDDLIdempotencyKeyNode> tryGetExpiredDDLIdempotencyKeyNode(const ZooKeeperPtr & zookeeper, const String & path, Int64 now);

/// Removes the node unless a key was committed since it was read: a removed node may be created again with the same
/// version. Returns whether the node was removed.
bool tryRemoveExpiredDDLIdempotencyKeyNode(const ZooKeeperPtr & zookeeper, const String & path, const ExpiredDDLIdempotencyKeyNode & node);

/// Engines whose `ALTER` changes nothing but the table's metadata, which the commit of the log entry publishes.
bool isDDLIdempotencyKeyAlterEngineSupported(const String & engine_name);

/// Throws IDEMPOTENCY_KEY_MISMATCH if the node was created by another statement or user.
void checkDDLIdempotencyKeyDigest(const DDLIdempotencyKey & key, const DDLIdempotencyKeyNode & node);

/// Executes a client query with a non-empty `idempotency_key`: a DDL statement on a `Replicated` database that has
/// already committed with the key is answered from it instead of running again. Throws IDEMPOTENCY_KEY_NOT_SUPPORTED, before
/// anything is executed, for statements it cannot honour, including any statement `nested` in another query (`PARALLEL WITH`,
/// `EXECUTE AS`).
BlockIO executeWithDDLIdempotencyKey(IInterpreter & interpreter, const ASTPtr & ast, const ContextMutablePtr & context, bool nested);

/// Called by interpreters on the branch that executes a statement locally instead of through the log of a `Replicated`
/// database: such a statement cannot be made idempotent, so a query with a key is refused before anything is executed.
void throwIfDDLIdempotencyKeyIsSet(const ContextPtr & context);

}
