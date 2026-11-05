#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

#include <memory>
#include <optional>

namespace zkutil
{
class ZooKeeper;
}

namespace DB
{

class IInterpreter;
struct BlockIO;
using ZooKeeperPtr = std::shared_ptr<zkutil::ZooKeeper>;

/// The `idempotency_key` of a client DDL query on a `Replicated` database. The multi that commits the query's log entry
/// also creates the key node, so the node exists exactly when a statement with this key has committed.
struct DDLIdempotencyKey
{
    enum class State : uint8_t
    {
        Pending, /// No log entry was made yet.
        Enqueued, /// The query made its log entry.
        Answered, /// The query was answered from the key node of an earlier attempt.
    };

    String key;
    /// Identifies the statement and the user: the key of another statement or user is IDEMPOTENCY_KEY_MISMATCH.
    String digest;
    /// Only the query thread touches it.
    mutable State state = State::Pending;
};

/// Data of `<zookeeper_path>/idempotency/<hash of the key>`.
struct DDLIdempotencyKeyNode
{
    String entry;
    String digest;
    UInt64 ttl_sec = 0;

    String toString() const;
    static DDLIdempotencyKeyNode parse(const String & data);
};

String getDDLIdempotencyKeyNodeName(const String & key);

/// An expired node is reported as absent; then `expired_version` (if given) is set to its version.
std::optional<DDLIdempotencyKeyNode> tryReadDDLIdempotencyKeyNode(
    const ZooKeeperPtr & zookeeper, const String & path, std::optional<int32_t> * expired_version = nullptr);

bool isDDLIdempotencyKeyNodeExpired(Int64 ctime_ms, UInt64 ttl_sec);

/// Throws IDEMPOTENCY_KEY_MISMATCH if the node was created by another statement or user.
void checkDDLIdempotencyKeyDigest(const DDLIdempotencyKey & key, const DDLIdempotencyKeyNode & node);

/// Executes a client query with a non-empty `idempotency_key`: a DDL statement on a `Replicated` database that has
/// already committed with the key is answered from it instead of running again. Throws for statements it cannot honour.
BlockIO executeWithDDLIdempotencyKey(IInterpreter & interpreter, const ASTPtr & ast, const ContextMutablePtr & context);

/// Called by interpreters on the branch that executes a statement locally instead of through the log of a `Replicated`
/// database: such a statement cannot be made idempotent, so a query with a key is refused before anything is executed.
void throwIfDDLIdempotencyKeyIsSet(const ContextPtr & context);

}
