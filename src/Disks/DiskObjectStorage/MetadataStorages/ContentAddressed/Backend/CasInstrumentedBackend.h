#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Common/ProfileEvents.h>

namespace DB::Cas
{

/// Per-namespace and per-operation instrumentation for the content-addressed storage seam.
///
/// Every content-addressed storage operation flows through the abstract `Backend` seam.
/// `InstrumentedBackend` is a transparent decorator: it owns an inner `BackendPtr`, delegates every
/// operation, and increments a `ProfileEvent` keyed by the key's namespace and the operation's
/// outcome. The pool wraps its backend once in `Pool::open`, which includes operations issued by
/// background writers and GC as well as foreground calls, for both object-storage and in-memory
/// backends. This backend-level chokepoint is needed because background PUTs are not attributable
/// through the foreground request that scheduled them.

/// Namespace of a CA key, classified by substring of the key path (6 classes; `Server` is currently
/// unreachable through this classifier — the per-server control subtree lives under
/// `/gc/server-roots/<server_root_id>/...` and classifies as Gc).
///   <prefix>/blobs/..        → Blob
///   <prefix>/cas/ns/..        → Root  (immutable streams and point/path-addressed namespace state)
///   <prefix>/cas/manifests/.. → Manifest
///   <prefix>/roots/..        → Root  (loose mountpoint objects)
///   <prefix>/gc/..           → Gc
///   else (e.g. _pool_meta, _probe) → Other
enum class CasNs : uint8_t
{
    Blob = 0,
    Manifest,
    Root,
    Gc,
    Server,
    Other,
};
static constexpr size_t CAS_NS_COUNT = 6;

/// Operation + outcome class, mapped from the `Backend` primitive and its result.
///   write, no expected value   → a value ⇒ Put       ; RawConflict ⇒ PutDeduplicated
///   write, an expected value   → a value ⇒ Overwrite ; RawConflict ⇒ CasConflict
///   head                       → present ⇒ Head ; absent ⇒ HeadMiss (the 404 signal)
///   read                       → Read (all calls, hit or miss)
///   stream                     → GetStream
///   remove                     → Delete (all outcomes)
///   list                       → List
///   publish                    → Put
///
/// `Cas` has no current producer: the primitive `write` cannot tell a compare-and-set from any other
/// conditional replacement, so every conditional replace counts as `Overwrite`/`CasConflict`. Kept
/// for the `CAS*CompareSwap` events it still backs.
enum class CasOp : uint8_t
{
    Put = 0,
    PutDeduplicated,
    Overwrite,
    Cas,
    CasConflict,
    Head,
    HeadMiss,
    Read,
    GetStream,
    Delete,
    List,
};
static constexpr size_t CAS_OP_COUNT = 11;

/// Classify a key into its namespace by substring. The order is significant where a more specific
/// layout such as `cas/ns/` must be recognized before a generic fallback; unknown key families
/// are intentionally counted as `Other`.
CasNs classifyCasNs(const String & key);

/// Increment the `ProfileEvent` corresponding to `(ns, op)`. The row-major table is defined in the
/// implementation and must remain aligned with the `CasNs` and `CasOp` enum values.
void incrementCasEvent(CasNs ns, CasOp op);

/// Transparent `Backend` decorator that records operation counts without changing the wrapped
/// backend's results, exceptions, or state transitions. The inner backend is owned by this object.
class InstrumentedBackend final : public Backend
{
public:
    explicit InstrumentedBackend(BackendPtr inner_) : inner(std::move(inner_)) {}

    /// Capability checks are deliberately uninstrumented: they do not represent storage operations.
    void checkPoolPreconditions() override { inner->checkPoolPreconditions(); }
    void checkSkipAccessCheckSupport() override { inner->checkSkipAccessCheckSupport(); }
    void checkConditionalWriteSingleAttemptSupport() override { inner->checkConditionalWriteSingleAttemptSupport(); }

    /// The typed sentinel probe is a diagnostic/authoritative read, not a routine storage operation —
    /// deliberately uninstrumented (no ProfileEvent), like the capability checks above.
    SentinelProbeResult probeSentinelRaw(const String & key, TransportAccess & access) override
    {
        return inner->probeSentinelRaw(key, access);
    }

    /// Delegate the read and count it after the inner call succeeds or returns absent. Exceptions
    /// propagate unchanged and therefore do not produce a separate outcome event.
    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        auto result = inner->read(key, access);
        incrementCasEvent(classifyCasNs(key), CasOp::Read);
        return result;
    }

    /// Count `Head` or `HeadMiss` from the returned presence after delegating to the backend.
    std::optional<RawMeta> head(const String & key, TransportAccess & access) override
    {
        auto result = inner->head(key, access);
        incrementCasEvent(classifyCasNs(key), result ? CasOp::Head : CasOp::HeadMiss);
        return result;
    }

    /// Delegate one paginated listing and classify the prefix used for the request.
    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
    {
        auto page = inner->list(prefix, cursor, limit, access);
        incrementCasEvent(classifyCasNs(prefix), CasOp::List);
        return page;
    }

    /// Delegate the conditional removal and count every returned outcome as `Delete`.
    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        auto outcome = inner->remove(key, expected_value, access);
        incrementCasEvent(classifyCasNs(key), CasOp::Delete);
        return outcome;
    }

    /// Delegate the batch removal, count the request once, and count each key it named as a `Delete`
    /// in its own namespace class -- the per-key counters say how many keys of each class one request
    /// carried, the request counter says how many requests it took. Out of line, like `publish`, so
    /// the header need not declare the `CASBulkDeleteRequests` extern.
    void removeManyWriteOnce(const std::vector<WriteOnceKey> & keys, TransportAccess & access) override;

    /// Count a create and a replacement separately, and each of them separately from its refusal:
    /// they cost the same one request, but a pool whose creates are mostly refused and one whose
    /// replacements mostly conflict are different problems.
    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value, TransportAccess & access) override
    {
        auto result = inner->write(key, bytes, expected_value, access);
        const CasOp op = expected_value ? (result ? CasOp::Overwrite : CasOp::CasConflict)
                                        : (result ? CasOp::Put : CasOp::PutDeduplicated);
        incrementCasEvent(classifyCasNs(key), op);
        return result;
    }

    /// Delegate a forward-only read stream and count the request after the stream is acquired.
    std::unique_ptr<ReadBuffer> stream(const String & key, TransportAccess & access) override
    {
        auto result = inner->stream(key, access);
        incrementCasEvent(classifyCasNs(key), CasOp::GetStream);
        return result;
    }

    /// Count one successful physical blob publication after delegating exactly once. The backend has
    /// no lifecycle reason to classify here; decision diagnostics remain with the writer.
    void publish(const BlobPublishRequest & request, TransportAccess & access) override;

    /// These are properties of the wrapped backend, not operations to count.
    Dialect dialect() const override { return inner->dialect(); }
    bool supportsListTokens() const override { return inner->supportsListTokens(); }
    uint64_t attemptTimeoutMs() const override { return inner->attemptTimeoutMs(); }
    uint64_t attemptEnvelopeMs() const override { return inner->attemptEnvelopeMs(); }
    bool refreshCredentials() override { return inner->refreshCredentials(); }

private:
    BackendPtr inner;
};

}
