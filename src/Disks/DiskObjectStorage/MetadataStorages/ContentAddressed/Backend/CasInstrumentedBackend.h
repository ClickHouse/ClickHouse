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

/// Operation + outcome class (11 classes), mapped from the `Backend` method and its return value.
///   putIfAbsent / putIfAbsentStream finalize → Done ⇒ Put ; PreconditionFailed ⇒ PutDeduplicated
///   putOverwrite                              → Done ⇒ Overwrite ; PreconditionFailed ⇒ CasConflict
///   casPut                                    → Committed ⇒ Cas ; Conflict ⇒ CasConflict
///   head                                      → exists ⇒ Head ; !exists ⇒ HeadMiss (the 404 signal)
///   get                                       → Get (all calls, hit or miss)
///   getStream                                 → GetStream (all calls, hit or miss)
///   deleteExact                               → Delete (all outcomes)
///   list                                      → List
enum class CasOp : uint8_t
{
    Put = 0,
    PutDeduplicated,
    Overwrite,
    Cas,
    CasConflict,
    Head,
    HeadMiss,
    Get,
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
/// For streaming creates, namespace classification happens when the sink is created and the
/// `Put`/`PutDeduplicated` event is emitted only when `finalize` returns, because the outcome is unavailable
/// earlier.
class InstrumentedBackend final : public Backend
{
public:
    /// Unhide the base convenience overloads (omitted Range/ObjectMeta/expected-token forms): the
    /// overrides below would otherwise shadow them for callers holding a concrete backend type.
    using Backend::get;
    using Backend::getStream;
    using Backend::putIfAbsent;
    using Backend::putIfAbsentStream;
    using Backend::putOverwrite;
    using Backend::casPut;

    explicit InstrumentedBackend(BackendPtr inner_) : inner(std::move(inner_)) {}

    /// Capability checks are deliberately uninstrumented: they do not represent storage operations.
    void checkPoolPreconditions() override { inner->checkPoolPreconditions(); }
    void checkSkipAccessCheckSupport() override { inner->checkSkipAccessCheckSupport(); }
    void checkConditionalWriteSingleAttemptSupport() override { inner->checkConditionalWriteSingleAttemptSupport(); }

    /// The typed sentinel probe is a diagnostic/authoritative read, not a routine storage operation —
    /// deliberately uninstrumented (no ProfileEvent), like the capability checks above. MUST still be
    /// forwarded explicitly: `Backend::probeSentinelRaw`'s generic default derives its classification from
    /// THIS object's own `head`/`get` (virtual dispatch would otherwise resolve back to
    /// `InstrumentedBackend`'s plain, non-typed overrides above), silently discarding whatever sharper
    /// container/permission evidence the wrapped `inner` backend (e.g. `ObjectStorageBackend`'s S3/Local
    /// classification) is able to provide.
    SentinelProbeResult probeSentinelRaw(const String & key) override { return inner->probeSentinelRaw(key); }

    /// Delegate the read and count it after the inner call succeeds or returns absent. Exceptions
    /// propagate unchanged and therefore do not produce a separate outcome event.
    std::optional<GetResult> get(const String & key, Range range) override
    {
        auto result = inner->get(key, range);
        incrementCasEvent(classifyCasNs(key), CasOp::Get);
        return result;
    }

    /// Delegate a forward-only read stream and count the request after the stream is acquired.
    std::optional<GetStreamResult> getStream(const String & key, Range range) override
    {
        auto result = inner->getStream(key, range);
        incrementCasEvent(classifyCasNs(key), CasOp::GetStream);
        return result;
    }

    /// Count `Head` or `HeadMiss` from the returned presence flag after delegating to the backend.
    HeadResult head(const String & key) override
    {
        HeadResult result = inner->head(key);
        incrementCasEvent(classifyCasNs(key), result.exists ? CasOp::Head : CasOp::HeadMiss);
        return result;
    }

    /// Count a successful create as `Put` and an existing-key precondition result as `PutDeduplicated`.
    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        PutResult result = inner->putIfAbsent(key, bytes, meta);
        incrementCasEvent(classifyCasNs(key), result.outcome == PutOutcome::Done ? CasOp::Put : CasOp::PutDeduplicated);
        return result;
    }

    /// Return a sink that records the create outcome when its `finalize` is called.
    WriteSinkPtr putIfAbsentStream(const String & key, const ObjectMeta & meta) override;

    /// Count a successful token-conditional overwrite as `Overwrite`; a precondition conflict is
    /// counted as `CasConflict`.
    PutResult putOverwrite(const String & key, const String & bytes, const Token & expected,
                           const ObjectMeta & meta) override
    {
        PutResult result = inner->putOverwrite(key, bytes, expected, meta);
        incrementCasEvent(classifyCasNs(key), result.outcome == PutOutcome::Done ? CasOp::Overwrite : CasOp::CasConflict);
        return result;
    }

    /// Count a committed compare-and-swap as `Cas`; conflicts are counted as `CasConflict`.
    CasResult casPut(const String & key, const String & bytes, const std::optional<Token> & expected,
                     const ObjectMeta & meta) override
    {
        CasResult result = inner->casPut(key, bytes, expected, meta);
        incrementCasEvent(classifyCasNs(key), result.outcome == CasOutcome::Committed ? CasOp::Cas : CasOp::CasConflict);
        return result;
    }

    /// Delegate token-exact deletion and count every returned deletion outcome as `Delete`.
    DeleteOutcome deleteExact(const String & key, const Token & token) override
    {
        DeleteOutcome outcome = inner->deleteExact(key, token);
        incrementCasEvent(classifyCasNs(key), CasOp::Delete);
        return outcome;
    }

    /// Delegate one paginated listing and classify the prefix used for the request.
    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        ListPage page = inner->list(prefix, cursor, limit);
        incrementCasEvent(classifyCasNs(prefix), CasOp::List);
        return page;
    }

    /// This capability is a property of the wrapped backend, not an operation to count.
    bool supportsListTokens() const override { return inner->supportsListTokens(); }

    /// Count a successful staged promotion as a create of the destination blob; an existing
    /// destination is the same deduplication outcome as `putIfAbsent`.
    PutResult promoteStaged(const String & staging_key, const String & blob_key) override
    {
        PutResult result = inner->promoteStaged(staging_key, blob_key);
        /// A write-once server-side copy is a create attempt on the BLOB key: Done ⇒ Put, 412 ⇒ PutDeduplicated.
        incrementCasEvent(classifyCasNs(blob_key), result.outcome == PutOutcome::Done ? CasOp::Put : CasOp::PutDeduplicated);
        return result;
    }

    /// Count a staged resurrection as an unconditional overwrite of the destination blob. The
    /// wrapped backend remains responsible for its fresh-header and condemned-token guarantees.
    Token resurrect(ReadBuffer & payload, uint64_t payload_size, const String & blob_key, const String & fresh_header) override
    {
        Token token = inner->resurrect(payload, payload_size, blob_key, fresh_header);
        /// An unconditional resurrect re-upload overwrites the (condemned) BLOB key.
        incrementCasEvent(classifyCasNs(blob_key), CasOp::Overwrite);
        return token;
    }

private:
    BackendPtr inner;
};

}
