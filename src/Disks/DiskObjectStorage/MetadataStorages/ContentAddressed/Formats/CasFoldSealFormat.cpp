#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasByteBudget.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEnumWireTableAsserts.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <fmt/format.h>
#include <algorithm>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}
}

namespace DB::Cas
{

namespace
{

namespace FoldSealWire
{
    constexpr WireKey generation{"generation"};
    constexpr WireKey parent_generation{"parent_generation"};
    constexpr WireKey kind{"kind"};
    constexpr WireKey run_key{"key"};
    constexpr WireKey checksum{"checksum"};
    constexpr WireKey shard{"shard"};
    constexpr WireKey key_generation{"key_generation"};
    constexpr WireKey life{"life"};
    constexpr WireKey classification{"class"};
    constexpr WireKey fold_epoch{"fold_epoch"};
    constexpr WireKey fold_seq{"fold_seq"};
    constexpr WireKey hold_reason{"hold_reason"};
    constexpr WireKey hold_epoch{"hold_epoch"};
    constexpr WireKey hold_seq{"hold_seq"};
    constexpr WireKey retries{"retries"};
    constexpr WireKey retry_round{"retry_round"};
    constexpr WireKey remove_epoch{"remove_epoch"};
    constexpr WireKey remove_seq{"remove_seq"};
    constexpr WireKey condemned_total{"condemned"};
    constexpr WireKey pending_total{"pending"};
    constexpr WireKey oldest_round{"oldest_round"};
}

constexpr std::string_view kRefLifeTag = "ref_life";
constexpr std::string_view kBlobRunTag = "blob_run";
constexpr std::string_view kCondemnedTag = "condemned";

constexpr EnumWireTable<HoldReason, 6> kHoldReasonWords{{{
    {HoldReason::GapBelowWitness, "gap_below_witness"},
    {HoldReason::UnconsumedSealCrossing, "unconsumed_seal_crossing"},
    {HoldReason::WitnessDisappeared, "witness_disappeared"},
    {HoldReason::BodyUndecodable, "body_undecodable"},
    {HoldReason::ManifestBodyMissing, "manifest_body_missing"},
    {HoldReason::CheckpointUndecodable, "checkpoint_undecodable"},
}}};

static_assert(casEnumTableCoversEnum<kHoldReasonWords, HoldReason>());

constexpr EnumWireTable<CoverageClass, 4> kCoverageClassWords{{{
    {CoverageClass::Absent, "absent"},
    {CoverageClass::Unchanged, "unchanged"},
    {CoverageClass::Folded, "folded"},
    {CoverageClass::Clamped, "clamped"},
}}};

static_assert(casEnumTableCoversEnum<kCoverageClassWords, CoverageClass>());

/// A hold names a position the fold must resolve, and both components of that id are nonzero (the
/// canonical `RefTxnId` rule `renderRefTxnId` enforces for every id that becomes a key). A zero
/// component is not a weaker hold, it is a self-erasing one: no position sorts below `{0, 0}`, so the
/// carry rule clears it on the first round that folds anything, and the durable evidence that the
/// namespace ever stopped disappears without a single record having been resolved.
bool isCanonicalHoldPosition(const RefTxnId & at)
{
    return at.writer_epoch != 0 && at.ref_sequence != 0;
}

/// Insert a decoded record under a key that must appear at most ONCE in the object. Plain
/// `map[key] = value` is last-wins, and last-wins is not a lossy nicety here: a repeated coverage key
/// lets a later, clean row overwrite the held one that a whole namespace's retention rests on, which is
/// exactly how a forged or mis-merged seal would erase the only durable record of an unresolved
/// position. One record per key, or the object is corrupt.
template <typename Map, typename Key, typename Value>
void insertRecordOnce(Map & map, const Key & key, Value && value, std::string_view what)
{
    if (!map.emplace(key, std::forward<Value>(value)).second)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS fold seal: a second {} record for '{}' — a key appears at most once, and accepting the "
            "duplicate would silently overwrite the record already read",
            what, key);
}

/// Emit one run record (`kind` = `blob_run`) WITHOUT its line terminator; the caller closes (and measures) the
/// line, and sorts the vector by key first.
void writeRun(CasJsonWriter & out, std::string_view kind, const RunRef & r)
{
    bool first = true;
    writeWordField(out, FoldSealWire::kind, kind, first);
    writeStringField(out, FoldSealWire::run_key, r.key, first);
    writeHex128Field(out, FoldSealWire::checksum, r.checksum, first);
    writeNumberField(out, FoldSealWire::shard, r.shard, first);
    writeU64StringField(out, FoldSealWire::key_generation, r.key_generation, first);
    closeObject(out, first);
}

void validateFoldSealStructure(
    const CasFoldSeal & seal, const Layout & layout, uint64_t gc_shards,
    int error_code, std::string_view source)
{
    if (gc_shards == 0)
        throw Exception(error_code, "CAS fold seal {}: gc_shards must be nonzero", source);

    std::vector<bool> run_seen(gc_shards, false);
    for (const RunRef & run : seal.blob_target_runs)
    {
        if (run.key.empty() || run.key_generation == 0)
            throw Exception(error_code,
                "CAS fold seal {}: blob-target run requires a nonempty key and nonzero physical generation",
                source);
        if (run.shard >= gc_shards)
            throw Exception(error_code,
                "CAS fold seal {}: blob-target shard {} is outside [0, {})",
                source, run.shard, gc_shards);
        if (run_seen[run.shard])
            throw Exception(error_code,
                "CAS fold seal {}: duplicate blob-target shard {} -- at most one run per shard is allowed",
                source, run.shard);
        run_seen[run.shard] = true;

        const auto parsed = layout.parseBlobTargetRunKey(run.key);
        if (!parsed || parsed->generation != run.key_generation || parsed->shard != run.shard || parsed->seq != 0)
            throw Exception(error_code,
                "CAS fold seal {}: blob-target run key '{}' is not canonical for generation {}, shard {}, sequence 0",
                source, run.key, run.key_generation, run.shard);
    }

    if (seal.condemned_summary.size() != gc_shards)
        throw Exception(error_code,
            "CAS fold seal {}: condemned summary has {} rows, but exactly {} shards are required",
            source, seal.condemned_summary.size(), gc_shards);
    for (uint64_t shard = 0; shard < gc_shards; ++shard)
    {
        const auto it = seal.condemned_summary.find(shard);
        if (it == seal.condemned_summary.end())
            throw Exception(error_code,
                "CAS fold seal {}: condemned summary is missing shard {} from [0, {})",
                source, shard, gc_shards);
        const CondemnedSummary & summary = it->second;
        if (summary.pending_total > summary.condemned_total)
            throw Exception(error_code,
                "CAS fold seal {}: shard {} has pending_total {} greater than condemned_total {}",
                source, shard, summary.pending_total, summary.condemned_total);
        const bool has_nonpending = summary.pending_total < summary.condemned_total;
        const bool has_real_oldest = summary.oldest_nonpending_condemn_round != UINT64_MAX;
        if (has_nonpending != has_real_oldest)
            throw Exception(error_code,
                "CAS fold seal {}: shard {} must carry a real oldest non-pending condemn round exactly when non-pending rows exist",
                source, shard);
    }
}

}

std::string_view holdReasonToWord(HoldReason r)
{
    return kHoldReasonWords.toWord(r, "CAS fold seal hold reason");
}

HoldReason holdReasonFromWord(std::string_view w)
{
    return kHoldReasonWords.fromWord(w, "CAS fold seal hold reason");
}

std::string_view coverageClassToWord(CoverageClass c)
{
    return kCoverageClassWords.toWord(c, "CAS fold seal classification");
}

CoverageClass coverageClassFromWord(std::string_view w)
{
    return kCoverageClassWords.fromWord(w, "CAS fold seal classification");
}

namespace
{
/// A seal can carry thousands of `ref_life` rows, so a rejected word names the row that carries it --
/// "unknown word" alone leaves an operator scanning the object by hand.
CoverageClass coverageClassInRow(std::string_view w, std::string_view life_hex)
{
    return kCoverageClassWords.fromWord(w, fmt::format("CAS fold seal: ref_life '{}' classification", life_hex));
}

HoldReason holdReasonInRow(std::string_view w, std::string_view life_hex)
{
    return kHoldReasonWords.fromWord(w, fmt::format("CAS fold seal: ref_life '{}' hold_reason", life_hex));
}
}

FoldSealCaps foldSealCaps()
{
    const FormatTraits & t = traitsFor(FormatId::FoldSeal);
    return FoldSealCaps{.line_cap = t.line_cap, .object_cap = t.object_cap};
}

void checkFoldSealObjectBytes(uint64_t encoded_bytes)
{
    const uint64_t object_cap = foldSealCaps().object_cap;
    if (!fitsObjectCap(encoded_bytes, /*entries_reservation*/0, object_cap))
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "CAS fold seal: the seal encodes to {} bytes, over the {}-byte object cap. Writing it "
            "would leave a durable seal no later round can read; the round is refused before the PUT "
            "and retries.",
            encoded_bytes, object_cap);
}

String encodeFoldSeal(const CasFoldSeal & seal)
{
    const FoldSealCaps caps = foldSealCaps();
    CasJsonWriter out(256);

    /// EVERY line this encoder emits is measured against the LINE cap, on the bytes actually emitted --
    /// escaping, framing and all -- rather than on an estimate of them. A line that does not fit is not
    /// a large line, it is an UNREADABLE one: `readLine` refuses it, so the whole object is lost.
    /// Refuse here, where nothing is durable yet. The header and trailer are measured too, even though
    /// their lengths are bounded by construction — a gate with an unstated exception is one that a
    /// later edit widens without noticing.
    const auto checkLineBytes = [&](uint64_t bytes, std::string_view what)
    {
        if (!fitsLineCap(bytes, caps.line_cap))
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "CAS fold seal: the {} line encodes to {} bytes, over the {}-byte line cap; a longer "
                "line cannot be read back, so the seal is refused before it is written",
                what, bytes, caps.line_cap);
    };

    writeHeaderLine(out, FormatId::FoldSeal);   /// emits its own terminator
    checkLineBytes(out.size() - 1, "header");

    size_t line_start = out.size();
    const auto closeLine = [&](std::string_view what)
    {
        checkLineBytes(out.size() - line_start, what);
        writeChar('\n', out);
        line_start = out.size();
    };

    /// meta line
    {
        bool first = true;
        writeU64StringField(out, FoldSealWire::generation, seal.generation, first);
        writeU64StringField(out, FoldSealWire::parent_generation, seal.parent_generation, first);
        closeObject(out, first);
        closeLine("meta");
    }

    uint64_t n = 0;

    /// Ref-life rows (`std::map<UInt128, ...>` => opaque-id-sorted). This is the sole serialized
    /// producer of ref coverage and removal evidence.
    for (const auto & [life_id, life_state] : seal.ref_lives)
    {
        const RefCoverage & cov = life_state.coverage;
        const String life_hex = u128ToHex(life_id);
        if (life_id == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS fold seal: a ref-life row has a zero life id -- 0 never names a life");
        /// THE STRICT GRAMMAR, enforced where the bytes are produced. Every refusal below is
        /// `LOGICAL_ERROR`: this row came from our own fold, so an ill-formed one is a bug in this
        /// process, not corruption arriving from a store — and none of these shapes is repairable once
        /// durable, so none is ever written.
        ///
        /// A classification outside the four named values first, because the two checks after it are
        /// stated in terms of those names and a row they cannot classify makes their answers meaningless.
        /// `coverageClassToWord` IS that range check (it throws `LOGICAL_ERROR` for a value the table
        /// does not index), so capturing its result here also gives the record its wire value below.
        const std::string_view classification_word = coverageClassToWord(cov.classification);
        /// A clamped row whose hold was dropped is indistinguishable, once durable, from a namespace that
        /// stopped for no reason — and a hold on any other classification claims a stop that did not
        /// happen.
        if ((cov.classification == CoverageClass::Clamped) != cov.hold.has_value())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS fold seal: coverage '{}' has classification {} and {} hold — the hold fields are "
                "required for classification clamped and forbidden otherwise",
                life_hex, classification_word, cov.hold ? "a" : "no");
        /// A hold that names no position resolves itself on the next round (nothing sorts below
        /// `{0, 0}`) and cannot be rendered where the sweep reports why it retained a manifest.
        if (cov.hold && !isCanonicalHoldPosition(cov.hold->offending_position))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS fold seal: coverage '{}' is held at {}-{} — a hold's offending position has both "
                "components nonzero; a zero one would be cleared by the first record the next round "
                "folds, erasing the only durable evidence that the namespace stopped",
                life_hex, cov.hold->offending_position.writer_epoch, cov.hold->offending_position.ref_sequence);

        if (life_state.cleanup_evidence
            && (life_state.cleanup_evidence->remove_txn_id.writer_epoch == 0
                || life_state.cleanup_evidence->remove_txn_id.ref_sequence == 0))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS fold seal: ref life '{}' carries cleanup evidence with non-canonical removal "
                "transaction {}-{} -- both components are required and nonzero",
                life_hex,
                life_state.cleanup_evidence->remove_txn_id.writer_epoch,
                life_state.cleanup_evidence->remove_txn_id.ref_sequence);

        bool first = true;
        writeWordField(out, FoldSealWire::kind, kRefLifeTag, first);
        writeHex128Field(out, FoldSealWire::life, life_id, first);
        writeWordField(out, FoldSealWire::classification, classification_word, first);
        writeU64StringField(out, FoldSealWire::fold_epoch, cov.last_folded_ref_id.writer_epoch, first);
        writeU64StringField(out, FoldSealWire::fold_seq, cov.last_folded_ref_id.ref_sequence, first);
        if (cov.hold)
        {
            writeWordField(out, FoldSealWire::hold_reason, holdReasonToWord(cov.hold->reason), first);
            writeU64StringField(out, FoldSealWire::hold_epoch, cov.hold->offending_position.writer_epoch, first);
            writeU64StringField(out, FoldSealWire::hold_seq, cov.hold->offending_position.ref_sequence, first);
            writeNumberField(out, FoldSealWire::retries, cov.hold->retry_count, first);
            writeU64StringField(out, FoldSealWire::retry_round, cov.hold->next_retry_round, first);
        }
        if (life_state.cleanup_evidence)
        {
            writeU64StringField(out, FoldSealWire::remove_epoch, life_state.cleanup_evidence->remove_txn_id.writer_epoch, first);
            writeU64StringField(out, FoldSealWire::remove_seq, life_state.cleanup_evidence->remove_txn_id.ref_sequence, first);
        }
        closeObject(out, first);
        closeLine(kRefLifeTag);
        ++n;
    }

    {
        std::vector<RunRef> runs = seal.blob_target_runs;
        std::sort(runs.begin(), runs.end(), [](const RunRef & a, const RunRef & b) { return a.key < b.key; });
        for (const RunRef & r : runs)
        {
            writeRun(out, kBlobRunTag, r);
            closeLine(kBlobRunTag);
        }
    }
    n += seal.blob_target_runs.size();

    /// condemned summary (std::map<uint64> => shard-sorted)
    for (const auto & [shard, s] : seal.condemned_summary)
    {
        bool first = true;
        writeWordField(out, FoldSealWire::kind, kCondemnedTag, first);
        writeNumberField(out, FoldSealWire::shard, shard, first);
        writeNumberField(out, FoldSealWire::condemned_total, s.condemned_total, first);
        writeNumberField(out, FoldSealWire::pending_total, s.pending_total, first);
        writeU64StringField(out, FoldSealWire::oldest_round, s.oldest_nonpending_condemn_round, first);
        closeObject(out, first);
        closeLine(kCondemnedTag);
        ++n;
    }

    const size_t trailer_start = out.size();
    writeTrailerLine(out, n);   /// emits its own terminator
    checkLineBytes(out.size() - trailer_start - 1, "trailer");

    String text = std::move(out).take();
    checkFoldSealObjectBytes(text.size());
    return text;
}

CasFoldSeal decodeFoldSeal(std::string_view data, std::optional<uint64_t> expected_generation)
{
    ReadBufferFromMemory in(data.data(), data.size());
    expectHeaderLine(in, FormatId::FoldSeal);
    const uint64_t line_cap = traitsFor(FormatId::FoldSeal).line_cap;

    CasFoldSeal seal;

    /// meta line
    {
        const String meta = readLine(in, line_cap, "fold seal");
        ReadBufferFromMemory m(meta.data(), meta.size());
        JsonObjectReader r(m, KeyStrictness::Strict, "fold seal");
        String key;
        while (r.nextKey(key))
        {
            if (key == FoldSealWire::generation) seal.generation = r.readU64String();
            else if (key == FoldSealWire::parent_generation) seal.parent_generation = r.readU64String();
            else r.skipUnknown(key);   /// Strict => any unknown key is CORRUPTED_DATA
        }
    }

    uint64_t seen = 0;
    /// One line scratch and one reader for the whole loop: a decoder that rebuilds them per
    /// row pays an allocation per row for the seen-key store and the line, which profiling put
    /// at about a fifth of the instructions executed inside a row.
    String row_line;
    JsonObjectReader row_reader;
    while (true)
    {
        readLineInto(in, row_line, line_cap, "fold seal");
        ReadBufferFromMemory l(row_line.data(), row_line.size());
        row_reader.reset(l, KeyStrictness::Strict, "fold seal");
        JsonObjectReader & r = row_reader;
        String key;
        if (!r.nextKey(key))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS fold seal: empty line");

        if (key == "n")
        {
            const uint64_t n = r.readU64Number();
            if (r.nextKey(key))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS fold seal: trailer has extra keys");
            if (!l.eof() || !in.eof())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS fold seal: bytes after trailer");
            if (n != seen)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS fold seal: trailer count {} != {} records", n, seen);
            if (expected_generation && seal.generation != *expected_generation)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS fold seal: body generation {} does not match the requested generation {}",
                    seal.generation, *expected_generation);
            return seal;
        }
        if (key != FoldSealWire::kind)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS fold seal: record must start with \"kind\"");
        const String kind = r.readString();

        if (kind == kRefLifeTag)
        {
            std::optional<UInt128> life_id;
            RefCoverage cov;
            /// The hold fields are read individually so the grammar can be checked on WHICH of them
            /// arrived, not merely on how many. `JsonObjectReader` already rejects a duplicate key, so
            /// a second `hold_reason` can never quietly rewrite the reason.
            std::optional<String> classification_word;
            std::optional<String> hold_reason_word;
            std::optional<HoldReason> hold_reason;
            std::optional<uint64_t> hold_epoch;
            std::optional<uint64_t> hold_sequence;
            std::optional<uint32_t> hold_retry_count;
            std::optional<uint64_t> hold_next_retry_round;
            std::optional<uint64_t> remove_txn_epoch;
            std::optional<uint64_t> remove_txn_sequence;
            while (r.nextKey(key))
            {
                if (key == FoldSealWire::life) life_id = r.readHex128();
                /// The two word-valued fields are collected as words and converted BELOW, once the row's
                /// life id is known: a seal can carry thousands of rows, so an unknown word has to say
                /// WHICH row carries it.
                else if (key == FoldSealWire::classification) classification_word = r.readString();
                else if (key == FoldSealWire::fold_epoch) cov.last_folded_ref_id.writer_epoch = r.readU64String();
                else if (key == FoldSealWire::fold_seq) cov.last_folded_ref_id.ref_sequence = r.readU64String();
                else if (key == FoldSealWire::hold_reason) hold_reason_word = r.readString();
                else if (key == FoldSealWire::hold_epoch) hold_epoch = r.readU64String();
                else if (key == FoldSealWire::hold_seq) hold_sequence = r.readU64String();
                else if (key == FoldSealWire::retries) hold_retry_count = r.readU32Number();
                else if (key == FoldSealWire::retry_round) hold_next_retry_round = r.readU64String();
                else if (key == FoldSealWire::remove_epoch) remove_txn_epoch = r.readU64String();
                else if (key == FoldSealWire::remove_seq) remove_txn_sequence = r.readU64String();
                else throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS fold seal: unknown ref_life key '{}'", key);
            }

            if (!life_id || *life_id == 0)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS fold seal: a ref-life row is missing a nonzero opaque life id");
            const String life_hex = u128ToHex(*life_id);

            /// `class` is required, not defaulted: an absent one would read as `absent` ("no round
            /// folded this namespace"), which is a claim about a fold, not the absence of one.
            if (!classification_word)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS fold seal: ref_life '{}' missing class", life_hex);
            if (hold_reason_word)
                hold_reason = holdReasonInRow(*hold_reason_word, life_hex);
            cov.classification = coverageClassInRow(*classification_word, life_hex);

            /// The same strict grammar the encoder enforces, applied to bytes we did not write. A
            /// PARTIAL hold is corruption, never a hold with defaults: a hold whose offending position
            /// defaulted to `{0,0}` would be cleared by the very first record the next round folds.
            const bool any_hold_field = hold_reason || hold_epoch || hold_sequence
                || hold_retry_count || hold_next_retry_round;
            const bool every_hold_field = hold_reason && hold_epoch && hold_sequence
                && hold_retry_count && hold_next_retry_round;
            if (cov.classification == CoverageClass::Clamped)
            {
                if (!every_hold_field)
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "CAS fold seal: coverage '{}' is held (classification clamped) but its hold is "
                        "incomplete — reason, offending position, retry count and next retry round are "
                        "all required", life_hex);
                /// PRESENT is not enough: the position must be one a fold can actually retry. `{0, 0}`
                /// (or either component zero) is the shape that quietly deletes the hold — the carry
                /// rule keeps a hold only while the walk stops BELOW it, and nothing is below zero — and
                /// it cannot be rendered where the sweep names the position it retained a manifest for.
                if (!isCanonicalHoldPosition(RefTxnId{*hold_epoch, *hold_sequence}))
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "CAS fold seal: coverage '{}' is held at {}-{} — a hold's offending position has "
                        "both components nonzero; a zero one clears itself on the next round",
                        life_hex, *hold_epoch, *hold_sequence);
                cov.hold = RefHold{.reason = *hold_reason,
                                   .offending_position = RefTxnId{*hold_epoch, *hold_sequence},
                                   .retry_count = *hold_retry_count,
                                   .next_retry_round = *hold_next_retry_round};
            }
            else if (any_hold_field)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS fold seal: coverage '{}' carries hold fields at classification {} — they are "
                    "forbidden on anything but a held (classification clamped) row",
                    life_hex, coverageClassToWord(cov.classification));

            const bool any_cleanup_field = remove_txn_epoch || remove_txn_sequence;
            const bool every_cleanup_field = remove_txn_epoch && remove_txn_sequence;
            std::optional<RefCleanupEvidence> cleanup_evidence;
            if (any_cleanup_field)
            {
                if (!every_cleanup_field || *remove_txn_epoch == 0 || *remove_txn_sequence == 0)
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "CAS fold seal: ref life '{}' carries incomplete or zero cleanup evidence -- "
                        "both removal transaction components are required and nonzero",
                        life_hex);
                cleanup_evidence = RefCleanupEvidence{
                    .remove_txn_id = RefTxnId{*remove_txn_epoch, *remove_txn_sequence}};
            }
            if (!seal.ref_lives.emplace(
                    *life_id, RefLifeFoldState{.coverage = cov, .cleanup_evidence = cleanup_evidence}).second)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS fold seal: a second ref-life record for '{}' -- a life id appears at most once",
                    life_hex);
        }
        else if (kind == kBlobRunTag)
        {
            std::optional<String> run_key;
            std::optional<UInt128> checksum;
            std::optional<uint64_t> shard;
            std::optional<uint64_t> generation;
            while (r.nextKey(key))
            {
                if (key == FoldSealWire::run_key) run_key = r.readString();
                else if (key == FoldSealWire::checksum) checksum = r.readHex128();
                else if (key == FoldSealWire::shard) shard = r.readU64Number();
                else if (key == FoldSealWire::key_generation) generation = r.readU64String();
                else throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS fold seal: unknown run key '{}'", key);
            }
            if (!run_key || !checksum || !shard || !generation)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS fold seal: blob_run requires key, checksum, shard, and key_generation");
            seal.blob_target_runs.push_back(RunRef{
                .key = std::move(*run_key), .checksum = *checksum, .shard = *shard, .key_generation = *generation});
        }
        else if (kind == kCondemnedTag)
        {
            std::optional<uint64_t> shard;
            std::optional<uint64_t> condemned_total;
            std::optional<uint64_t> pending_total;
            std::optional<uint64_t> oldest_nonpending_condemn_round;
            while (r.nextKey(key))
            {
                if (key == FoldSealWire::shard) shard = r.readU64Number();
                else if (key == FoldSealWire::condemned_total) condemned_total = r.readU64Number();
                else if (key == FoldSealWire::pending_total) pending_total = r.readU64Number();
                else if (key == FoldSealWire::oldest_round) oldest_nonpending_condemn_round = r.readU64String();
                else throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS fold seal: unknown condemned key '{}'", key);
            }
            if (!shard || !condemned_total || !pending_total || !oldest_nonpending_condemn_round)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS fold seal: condemned requires shard, condemned, pending, and oldest_round");
            insertRecordOnce(seal.condemned_summary, *shard, CondemnedSummary{
                .condemned_total = *condemned_total,
                .pending_total = *pending_total,
                .oldest_nonpending_condemn_round = *oldest_nonpending_condemn_round}, "condemned-summary");
        }
        else
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS fold seal: unknown record kind '{}'", kind);

        if (!l.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS fold seal: junk after record");
        ++seen;
    }
}

CasFoldSeal decodeFoldSeal(
    std::string_view data, const Layout & layout, uint64_t gc_shards,
    std::optional<uint64_t> expected_generation)
{
    CasFoldSeal seal = decodeFoldSeal(data, expected_generation);
    validateFoldSealStructure(seal, layout, gc_shards, ErrorCodes::CORRUPTED_DATA, "adoption");
    return seal;
}

void validateFoldSealForWrite(const CasFoldSeal & seal, const Layout & layout, uint64_t gc_shards)
{
    validateFoldSealStructure(seal, layout, gc_shards, ErrorCodes::LOGICAL_ERROR, "producer");
}

}
