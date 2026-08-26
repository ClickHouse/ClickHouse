#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <map>
#include <mutex>
#include <set>
#include <vector>

namespace DB::Cas
{

/// Thread-safe, token-enforcing in-memory `Backend` implementation used by CAS tests.
///
/// All successful writes mint a monotonically increasing token (`TokenType::Emulated`).
/// Tokens NEVER repeat across the lifetime of a backend instance.
///
/// The backend also exposes fault-injection controls for probe tests and CAS correctness tests:
///   - `setHoldDeletes` / `landPendingDelete`: simulate async/delayed conditional deletes
///   - `failNextCasPut`:                      inject a one-shot conflict
///   - `setEnforceTokens(false)`:             mimic a "dumb" backend that ignores token checks
///   - `setSimulateDeleteMarkers`:            mimic S3 versioning-enabled buckets
///
/// Not `final`: tests subclass it to distort single behaviors (e.g. clamp list page size to force
/// pagination) while delegating everything else to this base.
class InMemoryBackend : public Backend
{
public:
    InMemoryBackend() = default;

    /// Unhide the base convenience overloads (omitted Range/ObjectMeta/expected-token forms): the
    /// overrides below would otherwise shadow them for callers holding a concrete backend type.
    using Backend::get;
    using Backend::getStream;
    using Backend::putIfAbsent;
    using Backend::putOverwrite;
    using Backend::casPut;

    // ---- Backend interface ----

    /// Returns the requested byte window, current token, and metadata, or `nullopt` when the key is absent.
    std::optional<GetResult> get(const String & key, Range range) override;

    /// Returns a forward-only stream over the requested byte window, or `nullopt` when the key is absent.
    /// The in-memory implementation copies the window into an owning read buffer while holding the
    /// backend lock, so the returned stream remains independent of later backend mutations.
    std::optional<GetStreamResult> getStream(const String & key, Range range) override;

    /// Returns the current existence, size, token, and metadata without materializing the body.
    HeadResult head(const String & key) override;

    /// The in-memory backend mints a monotonic token it surfaces through `list` — TRUE.
    bool supportsListTokens() const override { return true; }

    /// Creates `key` only when it is absent. On success stores `bytes` and `meta` under a new token;
    /// on a precondition failure leaves the existing object untouched.
    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override;

    /// Publishes either `[fresh_envelope][payload]` or the complete staged bytes as one atomic
    /// in-memory replacement. Streaming sources are fully validated before the destination changes.
    void publishBlob(const BlobPublishRequest & request) override;

    /// Replaces the existing object only when `expected` is its current token. Token enforcement can
    /// be disabled with `setEnforceTokens` to model a backend that incorrectly ignores this condition.
    PutResult putOverwrite(const String & key, const String & bytes, const Token & expected,
                           const ObjectMeta & meta) override;

    /// Performs create-if-absent when `expected` is empty, or replace-if-current-token otherwise.
    /// Conflicts leave the store unchanged and are returned as an outcome rather than an exception.
    CasResult casPut(const String & key, const String & bytes, const std::optional<Token> & expected,
                     const ObjectMeta & meta) override;

    /// Removes exactly the incarnation named by `token`, or queues that token check for a later
    /// `landPendingDelete` when delete holding is enabled. A queued delete is reported as accepted,
    /// but its token is rechecked when it is landed.
    DeleteOutcome deleteExact(const String & key, const Token & token) override;

    /// Lists up to `limit` keys under `prefix` in map order. `cursor` is the last key from the previous
    /// page; returned tokens identify the listed incarnations and `next_cursor` is set only when more
    /// matching keys remain.
    ListPage list(const String & prefix, const String & cursor, size_t limit) override;

    // ---- Fault-injection controls ----

    /// When true, `deleteExact` validates and enqueues deletes rather than applying them immediately.
    /// The caller sees `Deleted` (the send was accepted), but the object remains until
    /// `landPendingDelete`, where the token is checked again.
    void setHoldDeletes(bool hold);

    /// Returns the number of currently held deletes.
    size_t pendingDeletes() const;

    /// Applies and removes the held delete at index `i`. The token is evaluated against the current
    /// object at land time; the queue entry is removed whether the result is `TokenMismatch` or
    /// `Deleted`. An invalid index returns `NotFound`.
    DeleteOutcome landPendingDelete(size_t i);

    /// Injects a one-shot artificial `Conflict` on the next `casPut` for `key`.
    void failNextCasPut(const String & key);

    /// Injects a one-shot AMBIGUOUS outcome on the next `putIfAbsent` for `key`: instead of attempting
    /// the write, that call throws a plain (non-`DB::Exception`) exception -- classified `Unresolved`,
    /// never `DefiniteFailure`, by `classifyConditionalWriteResult` regardless of build flags -- and the
    /// store is left exactly as it was. Models a request whose own HTTP attempt outcome is lost (a
    /// timeout, a dropped connection) rather than a clean `PreconditionFailed`, for tests of controlled
    /// ops (`CasRequestController::slotOccupy` and its callers) that must exercise the "ambiguous
    /// attempt, resolve before deciding" path without a live network. One-shot, mirroring
    /// `failNextCasPut`'s contract: consumed by the first matching `putIfAbsent` call, whether the key
    /// was already present or not.
    void injectAmbiguousPutIfAbsent(const String & key);

    /// Enables or disables token checks for delete, overwrite, and CAS operations. Disabling checks
    /// models a backend that reports every expected token as matching.
    void setEnforceTokens(bool enforce);

    /// When true, successful deletes report `created_delete_marker = true`, modelling a versioned S3
    /// bucket whose delete creates a marker instead of reclaiming the current object.
    void setSimulateDeleteMarkers(bool simulate);

private:
    /// Complete in-memory incarnation state for one key. All fields are read or modified while
    /// `mutex_` is held; replacing `token` marks a new incarnation even when the bytes are unchanged.
    struct Object
    {
        String bytes;
        Token token;
        ObjectMeta meta;
    };

    /// Token captured when a held delete is queued. It is intentionally checked again at land time so
    /// a replacement between send and land produces `TokenMismatch` rather than deleting the new object.
    struct PendingDelete
    {
        String key;
        Token token;
    };

    /// Mints the next process-local token. Tokens are strictly increasing and never reused by this
    /// backend instance, which also makes token equality a safe content-cache identity check in tests.
    Token mintToken();

    /// Applies an exact-token delete while `mutex_` is already held. Used by immediate deletes and by
    /// `landPendingDelete` after its queue entry has been removed.
    DeleteOutcome applyDelete(const String & key, const Token & token);

    mutable std::mutex mutex_;
    std::map<String, Object> store_;
    uint64_t token_seq_ = 0;

    // Fault-injection state. These fields are protected by `mutex_` just like `store_`.
    bool hold_deletes_ = false;
    std::vector<PendingDelete> pending_deletes_;
    std::set<String> fail_next_cas_;
    std::set<String> ambiguous_put_keys_;
    bool enforce_tokens_ = true;
    bool simulate_delete_markers_ = false;
};

}
