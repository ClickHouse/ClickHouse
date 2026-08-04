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
    writeKey(out, "k", first);
    writeStringValue(out, "c");
    writeKey(out, "rn", first);
    writeStringValue(out, row.ref_name);
    writeManifestRefFields(out, first, "", row.manifest_ref);
    writeKey(out, "ts", first);
    writeIntText(row.published_at_ms, out);
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
    writeKey(out, "k", first);
    writeStringValue(out, "p");
    writeKey(out, "rn", first);
    writeStringValue(out, row.ref_name);
    writeManifestRefFields(out, first, "", row.manifest_ref);
    closeObject(out, first);
    writeChar('\n', out);
}

/// The snapshot's header-object meta line (`ns`, `snapshot_id`, and the required generation-8
/// `lc:"live"` constant). Shared by
/// `encodeRefTableSnapshot` and `snapshotFramingSize` so the two never disagree by a
/// byte. Assumes the caller has already validated the snapshot (or is measuring framing only).
void writeSnapshotMeta(CasJsonWriter & out, const RefTableSnapshot & snapshot)
{
    bool first = true;
    writeKey(out, "ns", first);
    writeStringValue(out, snapshot.ns);
    writeRefTxnIdFields(out, first, "we", "rs", snapshot.snapshot_id);
    writeKey(out, "lc", first);
    writeStringValue(out, "live");
    closeObject(out, first);
    writeChar('\n', out);
}

/// Collector for a ManifestRef's three flat fields (bare "me"/"mb"/"mo").
struct ManifestFields
{
    std::optional<uint64_t> me;
    std::optional<uint64_t> mb;
    std::optional<uint64_t> mo;

    /// Reconstruct a manifest reference after the tolerant reader has collected all three flat
    /// fields. Missing fields are malformed input; `manifestRefFromFields` performs the remaining
    /// range checks and reports the same corruption context as the row decoder.
    ManifestRef build(std::string_view what) const
    {
        if (!me || !mb || !mo)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: {} manifest_ref missing me/mb/mo", what);
        return manifestRefFromFields(*me, *mb, *mo, "RefTableSnapshot", what);
    }
};

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
        bool saw_we = false;
        bool saw_rs = false;
        bool saw_lc = false;
        String key;
        while (r.nextKey(key))
        {
            if (key == "ns") { snapshot.ns = r.readString(); saw_ns = true; }
            else if (key == "we") { snapshot.snapshot_id.writer_epoch = r.readU64String(); saw_we = true; }
            else if (key == "rs") { snapshot.snapshot_id.ref_sequence = r.readU64String(); saw_rs = true; }
            else if (key == "lc")
            {
                const String lifecycle = r.readString();
                if (lifecycle != "live")
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "RefTableSnapshot: lifecycle must be exactly 'live', got '{}'", lifecycle);
                saw_lc = true;
            }
            else if (key == "rte" || key == "rts")
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "RefTableSnapshot: meta carries retired terminal field '{}'", key);
            else r.skipUnknown(key);
        }
        if (!saw_ns || !saw_we || !saw_rs || !saw_lc)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: meta line missing ns/we/rs/lc");
        if (!meta_buf.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: junk after meta line");
    }

    /// record lines (committed then precommit), until the trailer
    while (true)
    {
        const String line = readLine(in, line_cap, "cas_ref_snap");
        ReadBufferFromMemory l(line.data(), line.size());
        JsonObjectReader r(l, KeyStrictness::Tolerant, "cas_ref_snap");
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
        if (key != "k")
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: record must start with \"k\"");
        const String k = r.readString();

        std::optional<String> rn;
        ManifestFields mf;
        std::optional<uint64_t> ts;
        while (r.nextKey(key))
        {
            if (key == "rn") rn = r.readString();
            else if (key == "me") mf.me = r.readU64String();
            else if (key == "mb") mf.mb = r.readU64String();
            else if (key == "mo") mf.mo = r.readU64Number();
            else if (key == "ts") ts = r.readU64Number();
            else if (key == "pl")
                /// `"pl"` (payload) was removed from the row wire in stage-1 T12. It is a KNOWN-removed
                /// field, not a genuinely-unknown future one the tolerant reader may skip -- silently
                /// discarding a persisted payload would lose data -- so reject it explicitly.
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "RefTableSnapshot: record carries the removed \"pl\" (payload) field");
            else r.skipUnknown(key);
        }
        if (!l.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: junk after record");

        if (k == "c")
        {
            if (!rn || !ts)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: committed row missing rn/ts");
            RefCommittedRow row;
            row.ref_name = *rn;
            checkCanonicalRefName(row.ref_name, "RefTableSnapshot", "committed ref_name");
            row.manifest_ref = mf.build("committed");
            row.published_at_ms = *ts;
            snapshot.committed.push_back(std::move(row));
        }
        else if (k == "p")
        {
            if (!rn)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: precommit row missing rn");
            RefOwnerBinding row;
            row.kind = RefOwnerKind::Precommit;
            row.ref_name = *rn;
            checkCanonicalRefName(row.ref_name, "RefTableSnapshot", "precommit ref_name");
            row.manifest_ref = mf.build("precommit");
            snapshot.precommits.push_back(std::move(row));
        }
        else
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefTableSnapshot: unknown row kind '{}'", k);
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
