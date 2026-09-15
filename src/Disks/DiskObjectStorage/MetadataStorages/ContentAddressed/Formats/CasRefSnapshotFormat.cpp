#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <optional>
#include <tuple>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}
}

namespace DB::Cas
{

namespace
{

namespace RefSnapWire
{
    constexpr WireKey ns{"namespace"};
    constexpr WireKey snapshot_epoch{"snapshot_epoch"};
    constexpr WireKey snapshot_seq{"snapshot_seq"};
    constexpr WireKey lifecycle{"lifecycle"};
    constexpr WireKey kind{"kind"};
    constexpr WireKey ref{"ref"};
    constexpr WireKey published_ms{"published_ms"};
}

constexpr std::string_view kLiveLifecycleWord = "live";

void checkCommittedSorted(const std::vector<RefCommittedRow> & rows)
{
    for (size_t i = 1; i < rows.size(); ++i)
        if (!(rows[i - 1].ref_name < rows[i].ref_name))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "RefTableSnapshot: committed rows are not strictly ascending by ref_name at '{}' -> '{}'",
                rows[i - 1].ref_name, rows[i].ref_name);
}

void checkPrecommitsSorted(const std::vector<RefOwnerBinding> & rows)
{
    for (size_t i = 1; i < rows.size(); ++i)
    {
        const auto prev_key = std::tie(rows[i - 1].ref_name, rows[i - 1].manifest_ref);
        const auto cur_key = std::tie(rows[i].ref_name, rows[i].manifest_ref);
        if (!(prev_key < cur_key))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "RefTableSnapshot: precommit rows are not strictly ascending by (ref_name, manifest_ref) at '{}' -> '{}'",
                rows[i - 1].ref_name, rows[i].ref_name);
    }
}

/// Whole-object validation: transaction IDs must be nonzero and both row vectors must be strictly
/// sorted. Applying the same
/// checks before encoding and after decoding keeps malformed caller state and malformed stored data
/// subject to the same contract.
void checkSnapshotInvariants(const RefTableSnapshot & snapshot)
{
    checkRefTxnIdNonzero(snapshot.snapshot_id, "RefTableSnapshot", "snapshot_id");

    checkCommittedSorted(snapshot.committed);
    checkPrecommitsSorted(snapshot.precommits);
}

void writeCommittedRow(CasJsonWriter & out, const RefCommittedRow & row)
{
    checkCanonicalRefName(row.ref_name, "RefTableSnapshot", "committed ref_name");
    checkManifestRef(row.manifest_ref, "RefTableSnapshot", "committed");
    bool first = true;
    writeWordField(out, RefSnapWire::kind, refOwnerKindToWord(RefOwnerKind::Committed), first);
    writeStringField(out, RefSnapWire::ref, row.ref_name, first);
    writeManifestRefFields(out, first, kBareManifestRefKeys, row.manifest_ref);
    writeNumberField(out, RefSnapWire::published_ms, row.published_at_ms, first);
    closeObject(out, first);
    writeChar('\n', out);
}

void writePrecommitRow(CasJsonWriter & out, const RefOwnerBinding & row)
{
    if (row.kind != RefOwnerKind::Precommit)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefTableSnapshot: precommits entry '{}' has kind {}, expected Precommit",
            row.ref_name, static_cast<uint8_t>(row.kind));
    checkCanonicalRefName(row.ref_name, "RefTableSnapshot", "precommit ref_name");
    checkManifestRef(row.manifest_ref, "RefTableSnapshot", "precommit");
    bool first = true;
    writeWordField(out, RefSnapWire::kind, refOwnerKindToWord(RefOwnerKind::Precommit), first);
    writeStringField(out, RefSnapWire::ref, row.ref_name, first);
    writeManifestRefFields(out, first, kBareManifestRefKeys, row.manifest_ref);
    closeObject(out, first);
    writeChar('\n', out);
}

/// The snapshot's header-object meta line (`namespace`, `snapshot_id`, and the required
/// `lifecycle:"live"` constant). Shared by
/// `encodeRefTableSnapshot` and `snapshotFramingSize` so the two never disagree by a
/// byte. Assumes the caller has already validated the snapshot (or is measuring framing only).
void writeSnapshotMeta(CasJsonWriter & out, const RefTableSnapshot & snapshot)
{
    bool first = true;
    writeStringField(out, RefSnapWire::ns, snapshot.ns, first);
    writeRefTxnIdFields(out, first, RefSnapWire::snapshot_epoch, RefSnapWire::snapshot_seq, snapshot.snapshot_id);
    /// A snapshot object exists only for a live namespace -- `RefLifecycle::Removed` has no snapshot
    /// representation -- so the wire carries exactly one lifecycle word. The reader keeps the
    /// fail-closed half: any other word, or none, is rejected there.
    writeWordField(out, RefSnapWire::lifecycle, kLiveLifecycleWord, first);
    closeObject(out, first);
    writeChar('\n', out);
}

}

String encodeRefTableSnapshot(const RefTableSnapshot & snapshot)
{
    checkSnapshotInvariants(snapshot);

    CasJsonWriter out(256 + 128 * (snapshot.committed.size() + snapshot.precommits.size()));
    writeHeaderLine(out, FormatId::RefSnapshot);

    writeSnapshotMeta(out, snapshot);

    for (const RefCommittedRow & row : snapshot.committed)
        writeCommittedRow(out, row);
    for (const RefOwnerBinding & row : snapshot.precommits)
        writePrecommitRow(out, row);

    writeTrailerLine(out, snapshot.committed.size() + snapshot.precommits.size());
    String text = std::move(out).take();
    if (text.size() > ref_snapshot_max_bytes)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefTableSnapshot: encoded size {} exceeds the snapshot byte limit {}", text.size(), ref_snapshot_max_bytes);
    return text;
}

RefTableSnapshot decodeRefTableSnapshot(
    std::string_view data, const String & expected_ns, const RefTxnId & expected_snapshot_id)
{
    ReadBufferFromMemory in(data.data(), data.size());
    expectHeaderLine(in, FormatId::RefSnapshot);
    const uint64_t line_cap = traitsFor(FormatId::RefSnapshot).line_cap;

    RefTableSnapshot snapshot;

    {
        const String line = readLine(in, line_cap, "cas_ref_snap");
        ReadBufferFromMemory meta_buf(line.data(), line.size());
        JsonObjectReader r(meta_buf, KeyStrictness::Tolerant, "cas_ref_snap");
        bool saw_ns = false;
        bool saw_snapshot_epoch = false;
        bool saw_snapshot_seq = false;
        RefLifecycle lifecycle = RefLifecycle::Removed;
        String key;
        while (r.nextKey(key))
        {
            if (key == RefSnapWire::ns) { snapshot.ns = r.readString(); saw_ns = true; }
            else if (key == RefSnapWire::snapshot_epoch) { snapshot.snapshot_id.writer_epoch = r.readU64String(); saw_snapshot_epoch = true; }
            else if (key == RefSnapWire::snapshot_seq) { snapshot.snapshot_id.ref_sequence = r.readU64String(); saw_snapshot_seq = true; }
            else if (key == RefSnapWire::lifecycle)
            {
                const String lifecycle_word = r.readString();
                if (lifecycle_word != kLiveLifecycleWord)
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "RefTableSnapshot: lifecycle must be exactly '{}', got '{}'", kLiveLifecycleWord, lifecycle_word);
                lifecycle = RefLifecycle::Live;
            }
            else if (key == "rte" || key == "rts")
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "RefTableSnapshot: meta carries retired terminal field '{}'", key);
            else r.skipUnknown(key);
        }
        if (!saw_ns || !saw_snapshot_epoch || !saw_snapshot_seq || lifecycle != RefLifecycle::Live)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: meta line missing namespace/snapshot_epoch/snapshot_seq/lifecycle");
        if (!meta_buf.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: junk after meta line");
    }

    /// record lines (committed then precommit), until the trailer
    /// One line scratch and one reader for the whole loop: a decoder that rebuilds them per
    /// row pays an allocation per row for the seen-key store and the line, which profiling put
    /// at about a fifth of the instructions executed inside a row.
    String row_line;
    JsonObjectReader row_reader;
    while (true)
    {
        readLineInto(in, row_line, line_cap, "cas_ref_snap");
        ReadBufferFromMemory l(row_line.data(), row_line.size());
        row_reader.reset(l, KeyStrictness::Tolerant, "cas_ref_snap");
        JsonObjectReader & r = row_reader;
        String key;
        if (!r.nextKey(key))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: empty line");

        if (key == "n")
        {
            const uint64_t n = r.readU64Number();
            while (r.nextKey(key))
                r.skipUnknown(key);
            if (!l.eof() || !in.eof())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: bytes after trailer");
            if (n != snapshot.committed.size() + snapshot.precommits.size())
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "RefTableSnapshot: trailer count {} != {} rows", n, snapshot.committed.size() + snapshot.precommits.size());
            break;
        }
        if (key != RefSnapWire::kind)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: record must start with \"kind\"");
        const RefOwnerKind kind = refOwnerKindFromWord(r.readString(), "RefTableSnapshot row kind");

        std::optional<String> ref;
        ManifestRefFields mf;
        std::optional<uint64_t> published_ms;
        while (r.nextKey(key))
        {
            if (key == RefSnapWire::ref) ref = r.readString();
            else if (matchManifestRefFields(key, r, kBareManifestRefKeys, mf))
            {
            }
            else if (key == RefSnapWire::published_ms) published_ms = r.readU64Number();
            else if (key == "pl")
                /// `"pl"` (payload) was removed from the row wire. It is a KNOWN-removed
                /// field, not a genuinely-unknown future one the tolerant reader may skip -- silently
                /// discarding a persisted payload would lose data -- so reject it explicitly.
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "RefTableSnapshot: record carries the removed \"pl\" (payload) field");
            else r.skipUnknown(key);
        }
        if (!l.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: junk after record");

        /// A `switch` rather than an if/else-if chain: the row kinds partition the enum, and a future
        /// enumerator must not be able to arrive here, pass the word lookup, and then fall out of the
        /// chain as a silently dropped row. With no default arm, adding one is a build error.
        switch (kind)
        {
        case RefOwnerKind::Committed:
        {
            if (!ref || !published_ms)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: committed row missing ref/published_ms");
            RefCommittedRow row;
            row.ref_name = *ref;
            checkCanonicalRefName(row.ref_name, "RefTableSnapshot", "committed ref_name");
            row.manifest_ref = mf.buildRef("RefTableSnapshot", "committed");
            row.published_at_ms = *published_ms;
            snapshot.committed.push_back(std::move(row));
            break;
        }
        case RefOwnerKind::Precommit:
        {
            if (!ref)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: precommit row missing ref");
            RefOwnerBinding row;
            row.kind = kind;
            row.ref_name = *ref;
            checkCanonicalRefName(row.ref_name, "RefTableSnapshot", "precommit ref_name");
            row.manifest_ref = mf.buildRef("RefTableSnapshot", "precommit");
            snapshot.precommits.push_back(std::move(row));
            break;
        }
        }
    }

    /// The object key is supplied separately from the body. Check the binding before accepting any
    /// decoded state so bytes stored under one namespace or snapshot ID cannot be interpreted as
    /// another object.
    if (snapshot.ns != expected_ns || snapshot.snapshot_id != expected_snapshot_id)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefTableSnapshot: body (ns='{}', snapshot_id={}-{}) does not match the key it was read from "
            "(ns='{}', snapshot_id={}-{})",
            snapshot.ns, snapshot.snapshot_id.writer_epoch, snapshot.snapshot_id.ref_sequence,
            expected_ns, expected_snapshot_id.writer_epoch, expected_snapshot_id.ref_sequence);

    checkSnapshotInvariants(snapshot);
    return snapshot;
}

size_t committedRowEncodedSize(const RefCommittedRow & row)
{
    CasJsonWriter out(256);
    writeCommittedRow(out, row);
    return out.size();
}

size_t precommitRowEncodedSize(const RefOwnerBinding & binding)
{
    CasJsonWriter out(256);
    writePrecommitRow(out, binding);
    return out.size();
}

size_t snapshotFramingSize(const String & ns, const RefTxnId & snapshot_id, uint64_t row_count)
{
    RefTableSnapshot meta_only;
    meta_only.ns = ns;
    meta_only.snapshot_id = snapshot_id;

    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::RefSnapshot);
    writeSnapshotMeta(out, meta_only);
    writeTrailerLine(out, row_count);
    return out.size();
}

}
