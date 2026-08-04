#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event CASGCRetiredSparedByReref;
    extern const Event CASGCUnmatchedRemoveDeltas;
}
#include <city.h>
#include <algorithm>
#include <cstring>
#include <memory>
#include <optional>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::Cas
{

namespace
{

const UInt128 kZeroSourceId{0};

/// Streams a shard's prior source-edge run at O(one block) resident memory: chains the run SEGMENTS the
/// caller resolved from the parent seal (`blob_target_runs` filtered to one shard) and exposes a one-row
/// lookahead for the fold merge. The prior run carries
/// BOTH surviving edges (`kEdgeActive`) AND the retired `kCondemned` sentinel rows at the zero source id,
/// so the cursor stops at edges AND at condemned rows (exposing the type via `rowType`), while zero-marker
/// sentinels are dropped on carry (per-generation, never carried forward). Row/key invariants are enforced
/// while streaming: `kEdgeActive` never at `source_id = 0`; sentinel rows (`kZeroMarker` /
/// `kCondemned`) ONLY at `source_id = 0`; at most one sentinel per blob; an unknown value byte or an empty
/// payload is `CORRUPTED_DATA`. Resolution uses the exact object references supplied by the caller, so a run
/// sealed for generation G that physically lives under an older generation's key is reached
/// without key construction. An empty `segments` is the fresh-pool / empty baseline. The row stream is
/// globally sorted by (blob_hash, source_id), so `key()` values are non-decreasing.
class PriorEdgeCursor
{
public:
    /// The key codec is stateless and self-describing, so a run may freely mix supported hash algorithms.
    PriorEdgeCursor(Backend & backend_, const std::vector<RunRef> & segments_)
        : backend(backend_), segments(segments_)
    {
        advance();
    }

    bool valid() const { return has_current; }
    const String & key() const { return current_key; }
    /// The value byte of the current row: `kEdgeActive` (a surviving edge) or `kCondemned` (a retired
    /// sentinel row). Zero markers are never surfaced (dropped on carry).
    char rowType() const { return current_type; }
    /// The decoded retired sentinel for the current row (only valid when `rowType() == kCondemned`).
    const CondemnedRow & condemnedRow() const { return current_condemned; }

    /// Advance to the next surviving edge OR retired sentinel, dropping zero markers, enforcing the
    /// row/key invariants, and crossing segment boundaries.
    void advance()
    {
        while (true)
        {
            /// Pull rows from the open segment until a surviving edge, retired sentinel, or the segment ends.
            if (reader)
            {
                String k;
                String p;
                while (reader->next(k, p))
                {
                    BlobRef bh;
                    UInt128 sid;
                    /// `parse` throws CORRUPTED_DATA on a malformed size / NOT_IMPLEMENTED on an
                    /// unknown algo byte (fail-closed).
                    SourceEdgeKeyCodec::parse(k, bh, sid);
                    if (p.empty())
                        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS source-edge run: empty row payload");
                    const char v = p[0];
                    const bool sentinel_key = (sid == kZeroSourceId);

                    if (sentinel_key)
                    {
                        /// A sentinel key carries exactly one row per blob and never an edge.
                        if (v == kEdgeActive)
                            throw Exception(ErrorCodes::CORRUPTED_DATA,
                                "CAS source-edge run: active edge at the reserved sentinel source_id 0");
                        if (v != kZeroMarker && v != kCondemned)
                            throw Exception(ErrorCodes::CORRUPTED_DATA,
                                "CAS source-edge run: unknown sentinel row type 0x{:02x}", static_cast<uint8_t>(v));
                        if (have_sentinel_blob && sentinel_blob == bh)
                            throw Exception(ErrorCodes::CORRUPTED_DATA,
                                "CAS source-edge run: duplicate sentinel row for one blob");
                        have_sentinel_blob = true;
                        sentinel_blob = bh;
                        if (v == kZeroMarker)
                            continue;   // A zero marker is per-generation and is dropped on carry.
                        /// A retired sentinel: decode and surface it (settled at close-out, not an edge).
                        current_condemned = decodeCondemnedRow(p);
                        current_key = k;
                        current_type = kCondemned;
                        has_current = true;
                        return;
                    }

                    /// A non-sentinel key must carry a surviving edge and nothing else.
                    if (v != kEdgeActive)
                        throw Exception(ErrorCodes::CORRUPTED_DATA,
                            "CAS source-edge run: sentinel row type 0x{:02x} at a non-sentinel key",
                            static_cast<uint8_t>(v));
                    current_key = k;
                    current_type = kEdgeActive;
                    has_current = true;
                    return;
                }
                /// Segment fully drained: verify its whole-file checksum against
                /// the seal's RunRef.checksum BEFORE the fold acts on any of its rows. This is the fold —
                /// the checksum's most important consumer (it decides deletions). Fail-closed on mismatch.
                reader->verifyAgainst(segments[seg_idx].checksum);
                reader.reset();
                ++seg_idx;
            }

            /// Open the next resolved segment; the segment list is exhausted => the chain is done.
            if (seg_idx >= segments.size())
            {
                has_current = false;
                return;
            }
            /// Typed open validates the NDJSON header before any row is consumed. Each row carries its
            /// own algorithm byte, so no separate width gate is needed.
            reader = openSourceEdgeRun(backend, segments[seg_idx].key);
        }
    }

private:
    Backend & backend;
    const std::vector<RunRef> & segments;

    size_t seg_idx = 0;
    std::optional<SourceEdgeRunView> reader;
    String current_key;
    char current_type = kEdgeActive;
    CondemnedRow current_condemned;
    bool has_current = false;

    /// Duplicate-sentinel guard: the last blob for which a sentinel row was seen (across skipped zero
    /// markers too). Rows are globally sorted, so two sentinels for one blob are adjacent.
    bool have_sentinel_blob = false;
    BlobRef sentinel_blob{};
};

}

UInt128 sourceEdgeId(const ManifestId & id, const String & path)
{
    String canon;
    canon += id.root_namespace.string();
    canon += '\0';
    auto beU64 = [&](uint64_t v) { for (int i = 7; i >= 0; --i) canon += static_cast<char>((v >> (8 * i)) & 0xFF); };
    auto beU32 = [&](uint32_t v) { for (int i = 3; i >= 0; --i) canon += static_cast<char>((v >> (8 * i)) & 0xFF); };
    beU64(id.ref.writer_epoch); beU64(id.ref.build_sequence); beU32(id.ref.manifest_ordinal);
    canon += '\0';
    canon += path;
    const auto h = CityHash_v1_0_2::CityHash128(canon.data(), canon.size());
    const UInt128 result = (static_cast<UInt128>(h.high64) << 64) | static_cast<UInt128>(h.low64);
    if (result == UInt128{0})
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CAS source edge: hash collided with the reserved sentinel id 0");
    return result;
}

void assertValidSourceEdgeId(const UInt128 & source_id)
{
    if (source_id == UInt128{0})
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CAS source edge: source_id 0 is the reserved sentinel key");
}

String encodeCondemnedRow(const CondemnedRow & row)
{
    String out;
    out.push_back(kCondemned);
    out.push_back(static_cast<char>((row.delete_pending ? 1 : 0) | (row.marker_confirmed ? 2 : 0)));
    out.push_back(static_cast<char>(row.token.type));
    auto beU64 = [&](uint64_t v) { for (int i = 7; i >= 0; --i) out += static_cast<char>((v >> (8 * i)) & 0xFF); };
    beU64(row.condemn_round);
    beU64(row.size);
    if (row.token.value.size() > 0xFFFF)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS condemned row: token too long ({})", row.token.value.size());
    out += static_cast<char>((row.token.value.size() >> 8) & 0xFF);
    out += static_cast<char>(row.token.value.size() & 0xFF);
    out += row.token.value;
    return out;
}

CondemnedRow decodeCondemnedRow(std::string_view p)
{
    /// [0]=0x02 [1]=flags [2]=token_type [3..10]=round [11..18]=size [19..20]=len [21..]=value
    constexpr size_t kFixed = 21;
    if (p.size() < kFixed || p[0] != kCondemned)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS condemned row: malformed header");
    CondemnedRow row;
    const uint8_t flags = static_cast<uint8_t>(p[1]);
    if (flags > 3)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS condemned row: unknown flags 0x{:02x}", flags);
    row.delete_pending = flags & 1;
    row.marker_confirmed = flags & 2;
    const uint8_t type = static_cast<uint8_t>(p[2]);
    if (type < 1 || type > 3)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS condemned row: unknown token_type {}", type);
    row.token.type = static_cast<TokenType>(type);
    auto beU64 = [&](size_t off) { uint64_t v = 0; for (int i = 0; i < 8; ++i) v = (v << 8) | static_cast<uint8_t>(p[off + i]); return v; };
    row.condemn_round = beU64(3);
    row.size = beU64(11);
    const size_t len = (static_cast<uint8_t>(p[19]) << 8) | static_cast<uint8_t>(p[20]);
    if (p.size() != kFixed + len)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS condemned row: declared token_len {} vs payload {}", len, p.size() - kFixed);
    row.token.value = String(p.substr(kFixed, len));
    return row;
}

SourceEdgeRunView::SourceEdgeRunView(std::unique_ptr<ReadBuffer> stream_)
    : stream(std::move(stream_))
    , reader(std::make_unique<SourceEdgeRunReader>(*stream))   /// the reader borrows *stream; both are members
{
}

bool SourceEdgeRunView::next(String & key, String & payload)
{
    SourceEdgeRecord rec;
    if (!reader->next(rec))
        return false;
    /// Reconstruct the packed SourceEdgeKeyCodec key + the ORIGINAL payload bytes (a single marker byte,
    /// or the encoded condemned row) so the fold / zeroInDegree / previewDeletes / fsck consumers keep
    /// their exact parse/compare logic against the NDJSON codec.
    key = SourceEdgeKeyCodec::key(rec.ref, rec.source_id);
    switch (rec.marker)
    {
        case kEdgeActive:
        case kZeroMarker:
            payload = String(1, rec.marker);
            break;
        case kCondemned:
            payload = encodeCondemnedRow(CondemnedRow{.delete_pending = rec.delete_pending,
                                                      .token = rec.token, .size = rec.size,
                                                      .condemn_round = rec.condemn_round,
                                                      .marker_confirmed = rec.marker_confirmed});
            break;
        default:
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS source-edge run: unknown row marker 0x{:02x}", static_cast<uint8_t>(rec.marker));
    }
    return true;
}

void SourceEdgeRunView::verifyAgainst(const UInt128 & expected)
{
    reader->verifyAgainst(expected);
}

UInt128 SourceEdgeRunView::accumulatedChecksum()
{
    return reader->accumulatedChecksum();
}

SourceEdgeRunView openSourceEdgeRun(std::string_view bytes)
{
    return SourceEdgeRunView(std::make_unique<ReadBufferFromMemory>(bytes.data(), bytes.size()));
}

SourceEdgeRunView openSourceEdgeRun(Backend & backend, const String & key)
{
    /// Streaming: `getStream` is a forward-only read of the write-once run — nothing is
    /// materialized whole (cas_run is object_cap = 0). Absent object => fail-closed.
    auto sr = backend.getStream(key);
    if (!sr)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS source-edge run: object {} is absent", key);
    return SourceEdgeRunView(std::move(sr->stream));
}

namespace
{
/// len-drift guard (mirrors DigestCodec::checkZeroTail, CasBlobDigest.h): a caller passing a digest
/// wider than the algo's own width is a programming bug, not corrupted on-disk data — chassert, not
/// throw.
void checkZeroTailForAlgo(const BlobDigest & d, uint8_t digest_len, [[maybe_unused]] const char * what)
{
    for (size_t i = digest_len; i < d.bytes.size(); ++i)
        chassert(d.bytes[i] == 0, fmt::format("SourceEdgeKeyCodec::{}: non-zero byte at tail position {} (digest_len={})", what, i, digest_len));
}
}

String SourceEdgeKeyCodec::key(const BlobRef & ref, const UInt128 & source_id)
{
    const uint8_t digest_len = static_cast<uint8_t>(blobHashLenFor(ref.algo));
    checkZeroTailForAlgo(ref.digest, digest_len, "key");
    String out;
    out.push_back(static_cast<char>(static_cast<uint8_t>(ref.algo)));
    out += String(reinterpret_cast<const char *>(ref.digest.bytes.data()), digest_len);
    out += u128ToBytesBE(source_id);
    return out;
}

void SourceEdgeKeyCodec::parse(std::string_view key, BlobRef & ref, UInt128 & source_id)
{
    if (key.empty())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS source-edge run: empty key");
    const uint8_t algo_byte = static_cast<uint8_t>(key[0]);
    BlobHashAlgo algo{};
    switch (algo_byte)
    {
        case static_cast<uint8_t>(BlobHashAlgo::CityHash128): algo = BlobHashAlgo::CityHash128; break;
        case static_cast<uint8_t>(BlobHashAlgo::XXH3_128):    algo = BlobHashAlgo::XXH3_128; break;
        case static_cast<uint8_t>(BlobHashAlgo::Sha256):      algo = BlobHashAlgo::Sha256; break;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "CAS source-edge run: unknown algo byte {} in key", algo_byte);
    }
    const uint8_t digest_len = static_cast<uint8_t>(blobHashLenFor(algo));
    if (key.size() != 1 + static_cast<size_t>(digest_len) + 16)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS source-edge run: malformed key ({} bytes, expected {})", key.size(), 1 + digest_len + 16);
    ref = BlobRef{};
    ref.algo = algo;
    memcpy(ref.digest.bytes.data(), key.data() + 1, digest_len);
    source_id = u128FromBytesBE(String(key.substr(1 + digest_len, 16)), "src-edge run key source_id");
}

void putDeterministicArtifact(Backend & backend, const String & key, const String & bytes)
{
    if (backend.putIfAbsent(key, bytes).outcome == PutOutcome::PreconditionFailed)
    {
        const auto existing = backend.get(key);
        if (!existing || existing->bytes != bytes)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS gc: deterministic artifact at {} occupied by divergent bytes (impossible under "
                "correct operation; refusing to proceed)", key);
        /// byte-equal => our own deterministic replay; adopt (no-op).
    }
}

void foldDeltasIntoGeneration(Backend & backend, const Layout & layout,
                              const std::vector<RunRef> & prior_runs,
                              uint64_t new_generation, uint64_t attempt,
                              uint64_t shard,
                              std::vector<BlobDelta> scattered, std::vector<RunRef> & out_runs,
                              uint64_t current_round, uint64_t condemn_round,
                              const std::function<std::optional<HeadResult>(const BlobRef &)> & head_blob,
                              const std::function<std::optional<HeadResult>(const BlobRef &)> & peek_head,
                              const std::function<bool(const RetiredEntry &)> & confirm_condemned_marker,
                              RetiredMergeResult * out_retired,
                              bool suppress_destructive,
                              std::vector<uint8_t> * out_applied_by_txn_ordinal,
                              std::vector<BlobSourceRetirement> source_retirements,
                              GcRoundWorkBudget * work_budget)
{
    RetiredMergeResult sink;
    RetiredMergeResult & rmr = out_retired ? *out_retired : sink;

    // Deterministic input ordering produces a byte-reproducible run for safe retry and adoption.
    // MUST be stable: for the same (ref, source_id) the journal ordering is
    // activation-before-removal; "last wins" then correctly resolves to removal (edge absent).
    // An unstable sort can put removal before activation => last=activation => false positive.
    // The comparator is exactly (ref.algo, ref.digest, source_id) == BlobRef::operator< then
    // source_id — the same order the raw keys sort in (`SourceEdgeKeyCodec::key`'s algo
    // byte decides before any digest byte can), so the merge below stays a plain key comparison.
    std::stable_sort(scattered.begin(), scattered.end(),
        [](const BlobDelta & a, const BlobDelta & b)
        {
            if (a.ref != b.ref) return a.ref < b.ref;
            return a.source_id < b.source_id;
        });
    std::sort(source_retirements.begin(), source_retirements.end(),
        [](const BlobSourceRetirement & a, const BlobSourceRetirement & b)
        {
            if (a.ref != b.ref) return a.ref < b.ref;
            return a.source_id < b.source_id;
        });

    PriorEdgeCursor cursor(backend, prior_runs);

    DB::WriteBufferFromOwnString out;
    SourceEdgeRunWriter writer(out);   // sorted NDJSON; byte-deterministic for write-once adoption

    // Streaming two-cursor merge over the prior run (surviving edges AND retired kCondemned
    // sentinel rows at the zero source id) and this round's edge deltas (by (blob_hash, source_id)). All
    // rows for one blob are adjacent in both inputs; the sentinel key (source_id 0) sorts first. We resolve
    // final presence per edge locally (idempotent: prior present + activate => present; any remove =>
    // absent), settle each blob's carried retired row against its post-merge in-degree at close-out, and
    // re-emit the surviving retired rows / zero-transition markers. O(block) IO + O(1) per current blob.
    size_t di = 0;
    size_t ri = 0;
    BlobRef cur_blob{};
    bool have_blob = false;
    uint64_t cur_edges = 0;              // surviving edges of cur_blob so far
    bool cur_touched = false;            // cur_blob had prior edges or deltas this generation
    std::optional<CondemnedRow> cur_condemned;   // the retired sentinel carried on the prior run for cur_blob

    auto toRetiredEntry = [](const BlobRef & ref, const CondemnedRow & r) -> RetiredEntry
    {
        RetiredEntry e;
        e.kind = ObjectKind::Blob;
        e.ref = ref;
        e.token = r.token;
        e.size = r.size;
        e.condemn_round = r.condemn_round;
        e.delete_pending = r.delete_pending;
        e.marker_confirmed = r.marker_confirmed;
        return e;
    };
    /// THE DELETE-SITE IN-DEGREE RE-READ IS NORMATIVE (spec §5, third arm). It is not an optimization
    /// and not defense-in-depth: it is the last of the three things that keep a delete from racing a
    /// live edge, and the only one that acts on THIS pass's freshly merged view.
    ///
    ///   1. the round-paced floor: a blob condemned in round R cannot graduate before R+1, so a `+1`
    ///      that lands in the same round as the condemnation is always folded before any delete;
    ///   2. the exact-token delete: a writer that resurrected the blob replaced its incarnation, so a
    ///      stale token's delete finds a TokenMismatch and removes nothing;
    ///   3. THIS: the entry is settled against `indeg` recomputed by the merge that just ran, so an edge
    ///      folded after the condemnation but before the delete pass spares the blob outright --
    ///      `indeg > 0` wins over `delete_pending`, unconditionally and past the floor.
    ///
    /// Arms 1 and 2 bound WHEN and WHAT a delete may remove; only this one asks whether the blob is
    /// still referenced at the moment the pass decides. Removing it -- or reordering the branches so
    /// that `delete_pending` is checked first -- silently deletes re-referenced blobs on exactly the
    /// interleaving the other two arms do not cover. Any change here needs a test that fails without it.
    auto settleEntry = [&](const RetiredEntry & e, uint64_t indeg)
    {
        chassert(e.kind == ObjectKind::Blob);   /// the in-degree merge settles Blob entries only
        if (indeg > 0)
        {
            /// A delete_pending entry recovering in-degree is the expected shape of a dedup-adopt vs
            /// condemn race, not an ack-floor violation: a graduated blob carries NO surviving prior
            /// edges (see the comment on the sentinel emission below), so any edge that resurrects its
            /// in-degree is necessarily a FRESH this-generation edge -- a writer's `observeAndAdmit`
            /// point-read of the per-hash meta raced GC's `Condemned` write and adopted the (about to
            /// be deleted) token instead of resurrecting from source. Spare it LOUDLY (never a
            /// fail-closed abort, never a delete of a re-referenced blob), but at Debug: this is a
            /// routine, safely-handled race, not something to page on.
            if (e.delete_pending)
            {
                /// No LoggerPtr is threaded this deep (foldDeltasIntoGeneration is a free function
                /// shared by the non-sharded fold and CasGcShardPlan's per-shard reduce); scope the
                /// message with the pool's own key prefix instead so a multi-disk process's logs can
                /// still be attributed.
                LOG_DEBUG(getLogger("CasGcFold"),
                    "CAS gc fold ({}): delete_pending blob {} (condemned at round {}, observed at round {}) "
                    "recovered in-degree {} -- a fresh dedup-adopt raced the condemn; sparing (never a "
                    "fail-closed delete)", layout.poolPrefix(), blobIdOf(e.ref), e.condemn_round, current_round, indeg);
                ProfileEvents::increment(ProfileEvents::CASGCRetiredSparedByReref);
            }
            rmr.spared.push_back(e);            /// recovery wins, even past the floor
        }
        else if (e.delete_pending)
        {
            /// Excess past the round's redelete budget is carried unchanged (still `delete_pending`) —
            /// exactly the suppressed-pass shape below — rather than skipped ahead in `scattered`, so a
            /// budget-exhausted round retries the same entry next round instead of losing it.
            if (suppress_destructive || (work_budget && !work_budget->redeleteAvailable()))
                rmr.still_retired.push_back(e); /// clamp-suppressed or budget-exhausted: carry UNCHANGED
            else
            {
                if (work_budget)
                    ++work_budget->redeletes_used;
                rmr.redelete.push_back(e);      /// published pending by a PRIOR pass — execute + drop
            }
        }
        else if (!suppress_destructive && e.condemn_round < current_round)
        {
            /// Graduation gate (triage 2026-07-17 §3.4): publishing delete_pending is the one edge that
            /// authorizes an irreversible delete, and it requires CONFIRMED durable Condemned evidence
            /// for this exact (hash, token) — the marker is the writer's adopt gate, so an entry whose
            /// marker write was swallowed could be same-token adopted invisibly to this fold's cut.
            /// Unconfirmed => carry unchanged (fail-safe delay; the gate callback retries the marker so a
            /// later pass can confirm). This gates a DELETE on missing evidence; it never throws.
            if (e.marker_confirmed || !confirm_condemned_marker || confirm_condemned_marker(e))
            {
                /// Excess past the round's graduation budget carries the floor-passed entry unchanged
                /// (still condemned, not yet delete_pending) — it re-evaluates the floor next round and
                /// graduates then; nothing is lost, only delayed.
                if (work_budget && !work_budget->graduationAvailable())
                    rmr.still_retired.push_back(e);
                else
                {
                    if (work_budget)
                        ++work_budget->graduations_used;
                    RetiredEntry pending = e;       /// newly floor-passed: publish pending; delete NEXT pass
                    pending.delete_pending = true;
                    pending.marker_confirmed = true;
                    rmr.graduated.push_back(pending);
                    rmr.still_retired.push_back(std::move(pending));
                }
            }
            else
                rmr.still_retired.push_back(e); /// no durable condemn-marker evidence yet — carried
        }
        else
            rmr.still_retired.push_back(e);     /// carried unchanged until the floor passes it
    };

    auto closeBlob = [&]()
    {
        if (!have_blob)
            return;
        const size_t retired_before = rmr.still_retired.size();

        /// Settle the retired row carried on the prior run for the blob being closed, against its
        /// post-merge in-degree...
        if (cur_condemned)
        {
            /// The retired row already identifies the blob with the native `BlobRef` used by the run.
            const RetiredEntry stale = toRetiredEntry(cur_blob, *cur_condemned);
            /// On a re-reference cycle (touched this window, net in-degree 0),
            /// re-observe the CURRENT token. If it differs from the retired row's token, a resurrect
            /// replaced the incarnation at this key — supersede the stale entry with a fresh condemn of the
            /// current token so the replacement enters the pipeline (the stale token's exact-token delete
            /// would only find the new token and no-op). Keyed on (hash, current token), matching GRetire.
            /// `peek_head` is deliberately side-effect-free and is not `head_blob` —
            /// `head_blob` is the fresh-condemn hook (emits `BlobRetire` + increments
            /// `CASGCRetiredCondemned`); calling it here would double-emit `blob_retire` alongside the
            /// `blob_retire_replaced` this supersede already produces below, and double-count the
            /// condemned counter for one physical condemnation.
            bool superseded = false;
            if (cur_edges == 0 && cur_touched && peek_head)
            {
                if (const auto hr = peek_head(cur_blob);
                    hr && hr->exists && hr->token != stale.token)
                {
                    RetiredEntry fresh;
                    fresh.kind = ObjectKind::Blob;
                    fresh.ref = cur_blob;
                    fresh.token = hr->token;
                    fresh.size = hr->size;
                    fresh.condemn_round = condemn_round;
                    ReplacedEntry re;
                    re.old_token = stale.token;                 /// the stale token this supersede replaces
                    re.fresh = fresh;
                    rmr.replaced.push_back(std::move(re));       /// caller emits blob_retire_replaced
                    rmr.still_retired.push_back(std::move(fresh));
                    superseded = true;
                }
            }
            if (!superseded)
                settleEntry(stale, cur_edges);
        }
        /// ...or condemn a fresh transition-to-zero (no carried row). `head_blob` captures the exact
        /// incarnation token for the later exact-token delete; an absent object needs no entry.
        else if (cur_edges == 0 && cur_touched && head_blob)
        {
            if (const auto hr = head_blob(cur_blob); hr && hr->exists)
            {
                RetiredEntry fresh;
                fresh.kind = ObjectKind::Blob;
                fresh.ref = cur_blob;
                fresh.token = hr->token;
                fresh.size = hr->size;
                fresh.condemn_round = condemn_round;
                rmr.still_retired.push_back(std::move(fresh));
            }
        }

        /// Emit at most one sentinel row per blob: the `kCondemned` row when the
        /// blob is condemned/carried/graduated this pass (still_retired grew for it), else a per-generation
        /// `kZeroMarker` when it transitioned to zero this pass but was not condemned (redelete-dropped or
        /// absent-at-condemn). A blob with surviving edges (cur_edges > 0) emits neither — its edge rows
        /// were appended inline, and a condemned/zeroed blob has NO surviving edges, so appending the
        /// sentinel now (its key sorts first for the blob, and no edge rows precede it) keeps the run
        /// sorted. `still_retired` therefore mirrors exactly the emitted `kCondemned` rows, in order.
        if (rmr.still_retired.size() > retired_before)
        {
            const RetiredEntry & e = rmr.still_retired.back();
            writer.append(SourceEdgeRecord{.ref = cur_blob, .source_id = kZeroSourceId, .marker = kCondemned,
                                           .delete_pending = e.delete_pending, .token = e.token,
                                           .size = e.size, .condemn_round = e.condemn_round,
                                           .marker_confirmed = e.marker_confirmed});
        }
        else if (cur_edges == 0 && cur_touched)
            writer.append(SourceEdgeRecord{.ref = cur_blob, .source_id = kZeroSourceId, .marker = kZeroMarker});
    };
    auto openBlobIfNeeded = [&](const BlobRef & b)
    {
        if (!have_blob || b != cur_blob)
        {
            closeBlob();
            cur_blob = b; have_blob = true; cur_edges = 0; cur_touched = false; cur_condemned.reset();
        }
    };

    while (cursor.valid() || di < scattered.size() || ri < source_retirements.size())
    {
        // Pick the smallest row key across the prior-run cursor and this round's deltas.
        String key;
        bool from_prior = false;
        if (cursor.valid()) { key = cursor.key(); from_prior = true; }
        if (di < scattered.size())
        {
            const String dk = SourceEdgeKeyCodec::key(scattered[di].ref, scattered[di].source_id);
            if (!from_prior || dk < key) { key = dk; from_prior = false; }
        }
        if (ri < source_retirements.size())
        {
            const String rk = SourceEdgeKeyCodec::key(source_retirements[ri].ref, source_retirements[ri].source_id);
            if ((!from_prior && key.empty()) || rk < key) { key = rk; from_prior = false; }
        }

        BlobRef blob_ref;
        UInt128 source_id;
        SourceEdgeKeyCodec::parse(key, blob_ref, source_id);   // throws CORRUPTED_DATA on a malformed key (fail-closed)
        openBlobIfNeeded(blob_ref);

        /// A retired sentinel row from the prior run: stash it for close-out settlement. It is not an edge
        /// and NEVER a touch — a carried kCondemned row must not force a zero-marker or a peek_head HEAD
        /// and never a touch. Deltas never key the zero source id, so no delta merges at this key.
        if (from_prior && cursor.rowType() == kCondemned)
        {
            cur_condemned = cursor.condemnedRow();
            cursor.advance();
            continue;
        }

        bool present = false;
        if (from_prior && cursor.key() == key) { present = true; cursor.advance(); cur_touched = true; }
        while (di < scattered.size()
               && scattered[di].ref == blob_ref && scattered[di].source_id == source_id)
        {
            /// An unmatched remove: `present` was false immediately before this remove delta is
            /// applied, meaning neither the prior run nor an earlier delta in this same scattered run
            /// for this key had activated it. The set semantics make this a harmless per-key no-op
            /// (never a false deletion), but a persistent nonzero rate is a correctness signal — count
            /// it and hand ONE example back to the caller, who logs once per round (never from this
            /// hot inner loop; it runs over potentially millions of rows).
            if (scattered[di].remove && !present)
            {
                ++rmr.unmatched_removes;
                ProfileEvents::increment(ProfileEvents::CASGCUnmatchedRemoveDeltas);
                if (!rmr.unmatched_remove_example)
                    rmr.unmatched_remove_example = UnmatchedRemoveExample{blob_ref, source_id};
            }
            /// PROBE B2: this delta reached a reducer and is being CONSUMED. Marked here rather than
            /// at run flush because the in-degree model is a SET — an unmatched `-1` and a duplicate
            /// `+1` legitimately vanish inside the merge, so a flush-side mark would fire on healthy
            /// rounds. See `Cas::TxnApplyLedger`.
            if (out_applied_by_txn_ordinal)
                (*out_applied_by_txn_ordinal)[scattered[di].txn_ordinal] = 1;
            present = scattered[di].remove ? false : true;   // apply in order; last wins
            cur_touched = true;
            ++di;
        }

        /// Orphan nomination retires this exact manifest-source identity after ordinary ref deltas at
        /// the same key. It is accounting-neutral: absence is an idempotent no-op, not an unmatched
        /// ref removal, and there is no transaction ordinal to mark in B2's apply ledger.
        while (ri < source_retirements.size()
               && source_retirements[ri].ref == blob_ref && source_retirements[ri].source_id == source_id)
        {
            present = false;
            cur_touched = true;
            ++ri;
        }

        if (present)
        {
            writer.append(SourceEdgeRecord{.ref = blob_ref, .source_id = source_id, .marker = kEdgeActive});
            ++cur_edges;
        }
    }
    closeBlob();

    writer.finish();
    out.finalize();
    const String run_bytes = out.str();
    /// Whole-object streaming checksum: the same chained CityHash128 the reader
    /// accumulates on the read path, replacing the retired one-shot cityHash128. Carried by the fold
    /// seal's RunRef.checksum and verified before any consumer acts on the run.
    const UInt128 run_checksum = sourceEdgeRunChecksum(run_bytes);
    const String run_key = layout.blobTargetRunKey(new_generation, attempt, shard, 0);
    putDeterministicArtifact(backend, run_key, run_bytes);
    out_runs.push_back(RunRef{.key = run_key, .checksum = run_checksum,
                              .shard = shard, .generation = new_generation});
}

std::vector<BlobCandidate> zeroInDegree(Backend & backend, const std::vector<RunRef> & runs)
{
    std::vector<BlobCandidate> result;
    for (const RunRef & run : runs)
    {
        /// The caller passes the exact object key, so a run sealed
        /// for a later generation but physically living under an older key is reached directly. The run is
        /// streamed at O(one block) resident memory, never materialized whole. `openSourceEdgeRun` enforces
        /// the run kind + key schema; `kCondemned` sentinel rows are skipped (only `kZeroMarker` counts).
        SourceEdgeRunView r = openSourceEdgeRun(backend, run.key);
        String k;
        String p;
        while (r.next(k, p))
            if (!p.empty() && p[0] == kZeroMarker)
            {
                BlobRef bh;
                UInt128 sid;
                /// `parse` throws `CORRUPTED_DATA` on a malformed key; malformed rows must not be silently
                /// treated as absent candidates.
                SourceEdgeKeyCodec::parse(k, bh, sid);
                result.push_back(BlobCandidate{.ref = bh});
            }
        /// Whole-file checksum: verify the drained run against the seal's
        /// RunRef.checksum BEFORE its candidates feed a GC delete decision. Fail-closed on mismatch.
        r.verifyAgainst(run.checksum);
    }
    return result;
}

}
