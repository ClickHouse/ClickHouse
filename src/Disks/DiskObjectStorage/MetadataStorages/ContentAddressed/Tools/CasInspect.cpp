#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasInspect.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobMeta.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefWireVocab.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasWireVocab.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Common/Exception.h>
#include <fmt/format.h>
#include <cstdint>
#include <set>
#include <vector>

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

/// Escapes `s` as a JSON string LITERAL (including the surrounding quotes). Handles the standard
/// two-char escapes plus a `\uXXXX` fallback for any other control byte; everything else (including
/// raw multi-byte UTF-8) passes through unchanged. This is a debug/inspection rendering, not a wire
/// format, so it deliberately does not attempt full Unicode validation.
String jsonEscape(std::string_view s)
{
    String out;
    out.reserve(s.size() + 2);
    out += '"';
    for (unsigned char c : s)
    {
        switch (c)
        {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20)
                    out += fmt::format("\\u{:04x}", c);
                else
                    out += static_cast<char>(c);
        }
    }
    out += '"';
    return out;
}

/// u128 fields (hashes, ids, tokens-as-u128) render as a lowercase-hex JSON string, matching
/// `u128ToHex` — never as a nested {high,low} object or a decimal number.
String jsonHex(const UInt128 & v) { return jsonEscape(u128ToHex(v)); }
String jsonUInt(uint64_t v) { return std::to_string(v); }
String jsonBool(bool b) { return b ? "true" : "false"; }

/// A minimal JSON object builder: each `add` takes a key and an already-rendered JSON fragment
/// (a quoted string, a number, `true`/`false`/`null`, or a nested `{...}`/`[...]`) and joins them
/// with commas. No pretty-printing — this is a debug/inspection tool, not a wire format.
class JsonObj
{
public:
    JsonObj & add(std::string_view key, const String & raw_value)
    {
        if (!first)
            out += ",";
        first = false;
        out += jsonEscape(key);
        out += ":";
        out += raw_value;
        return *this;
    }

    String str() const { return "{" + out + "}"; }

private:
    String out;
    bool first = true;
};

String jsonArray(const std::vector<String> & items)
{
    String out = "[";
    for (size_t i = 0; i < items.size(); ++i)
    {
        if (i)
            out += ",";
        out += items[i];
    }
    out += "]";
    return out;
}

String renderManifestRef(const ManifestRef & r)
{
    return JsonObj()
        .add("writer_epoch", jsonUInt(r.writer_epoch))
        .add("build_sequence", jsonUInt(r.build_sequence))
        .add("manifest_ordinal", jsonUInt(r.manifest_ordinal))
        .str();
}

/// Snapshot and log ref objects use `RefTxnId` values with `writer_epoch` and `ref_sequence` fields.
/// `renderRefTxnIdObj` renders those raw numeric fields rather than the canonical hex form, which
/// rejects a zero field, so inspection can dump any object, including a malformed one, without
/// failing while rendering its identifiers.
String renderRefTxnIdObj(const RefTxnId & id)
{
    return JsonObj()
        .add("writer_epoch", jsonUInt(id.writer_epoch))
        .add("ref_sequence", jsonUInt(id.ref_sequence))
        .str();
}

String renderRefOwnerBinding(const RefOwnerBinding & b)
{
    return JsonObj()
        .add("kind", jsonEscape(refOwnerKindToWord(b.kind)))
        .add("ref_name", jsonEscape(b.ref_name))
        .add("manifest_ref", renderManifestRef(b.manifest_ref))
        .str();
}

String renderRefCommittedRow(const RefCommittedRow & r)
{
    return JsonObj()
        .add("ref_name", jsonEscape(r.ref_name))
        .add("manifest_ref", renderManifestRef(r.manifest_ref))
        .add("published_at_ms", jsonUInt(r.published_at_ms))
        .str();
}

String renderRefTableSnapshot(const RefTableSnapshot & s)
{
    std::vector<String> committed;
    committed.reserve(s.committed.size());
    for (const auto & row : s.committed)
        committed.push_back(renderRefCommittedRow(row));

    std::vector<String> precommits;
    precommits.reserve(s.precommits.size());
    for (const auto & b : s.precommits)
        precommits.push_back(renderRefOwnerBinding(b));

    return JsonObj()
        .add("object", jsonEscape("ref_snapshot"))
        .add("namespace", jsonEscape(s.ns))
        .add("snapshot_id", renderRefTxnIdObj(s.snapshot_id))
        .add("committed", jsonArray(committed))
        .add("precommits", jsonArray(precommits))
        .str();
}

/// The namespace's checkpoint. Every field is optional and each absence means something
/// different an operator needs to see: no `life_epoch` means no writer that knew this namespace's
/// genesis epoch has written here yet, no `committed_through` means the life has no committed
/// transaction, no `checkpoint_snapshot_id` means recovery has no snapshot base, and no
/// `last_epoch_seal` means no epoch of this namespace has been closed. They are rendered as explicit
/// `null`s rather than omitted keys so all four cases are visible.
/// `ns` comes from the KEY -- unlike the log and snapshot objects, a `_ckpt` body does not name its
/// namespace, so there is no key-to-body binding to cross-check here.
String renderRefCkpt(const RootNamespace & ns, const RefCkpt & c)
{
    return JsonObj()
        .add("object", jsonEscape("ref_ckpt"))
        .add("namespace", jsonEscape(ns.string()))
        .add("life_epoch", c.life_epoch ? jsonUInt(*c.life_epoch) : "null")
        .add("committed_through", c.committed_through ? renderRefTxnIdObj(*c.committed_through) : "null")
        .add("checkpoint_snapshot_id",
             c.checkpoint_snapshot_id ? renderRefTxnIdObj(*c.checkpoint_snapshot_id) : "null")
        .add("last_epoch_seal", c.last_epoch_seal ? renderRefTxnIdObj(*c.last_epoch_seal) : "null")
        .str();
}

String renderRefOp(const RefOp & op)
{
    return JsonObj()
        .add("kind", jsonEscape(refOpKindToWireWord(op.kind)))
        .add("old_binding", op.old_binding ? renderRefOwnerBinding(*op.old_binding) : "null")
        .add("new_binding", op.new_binding ? renderRefOwnerBinding(*op.new_binding) : "null")
        .add("ref_name", jsonEscape(op.ref_name))
        .add("expected_manifest_ref", renderManifestRef(op.expected_manifest_ref))
        .add("published_at_ms", jsonUInt(op.published_at_ms))
        .str();
}

String renderRefLogTxn(const RefLogTxn & t)
{
    std::vector<String> ops;
    ops.reserve(t.ops.size());
    for (const auto & op : t.ops)
        ops.push_back(renderRefOp(op));

    return JsonObj()
        .add("object", jsonEscape("ref_log"))
        .add("namespace", jsonEscape(t.ns))
        .add("txn_id", renderRefTxnIdObj(t.txn_id))
        .add("ops", jsonArray(ops))
        .add("prev_epoch_seal", t.prev_epoch_seal ? renderRefTxnIdObj(*t.prev_epoch_seal) : "null")
        .str();
}

/// `inline_bytes` renders as its LENGTH only, not its content — an inline file's bytes are payload
/// data, not part-manifest identity, and may be arbitrarily large / non-UTF8.
String renderManifestEntry(const ManifestEntry & e)
{
    /// Render `blobIdOf(e.ref)` ("<algoName>:<hex>"). The algorithm must remain part of the
    /// rendered identity: a bare digest is ambiguous in a pool containing algorithms with different
    /// digest widths, and each entry's own `ref.algo` determines its width.
    return JsonObj()
        .add("path", jsonEscape(e.path))
        .add("placement", jsonEscape(entryPlacementToWireWord(e.placement)))
        .add("blob", jsonEscape(blobIdOf(e.ref)))
        .add("blob_size", jsonUInt(e.blob_size))
        .add("inline_bytes_size", jsonUInt(e.inline_bytes.size()))
        .str();
}

String renderPartManifest(const PartManifest & m)
{
    std::vector<String> entries;
    entries.reserve(m.entries.size());
    for (const auto & e : m.entries)
        entries.push_back(renderManifestEntry(e));

    return JsonObj()
        .add("ref", renderManifestRef(m.ref))
        .add("root_namespace_id", jsonEscape(m.root_namespace_id.string()))
        .add("payload_digest", jsonHex(m.payload_digest))
        .add("entries", jsonArray(entries))
        .str();
}

String renderMountLease(const MountLease & m)
{
    return JsonObj()
        .add("server_uuid", jsonHex(m.server_uuid))
        .add("writer_epoch", jsonUInt(m.writer_epoch))
        .add("hostname", jsonEscape(m.hostname))
        .add("pid", jsonUInt(m.pid))
        .add("started_at_ms", jsonUInt(m.started_at_ms))
        .add("seq", jsonUInt(m.seq))
        .add("expires_at_ms", jsonUInt(m.expires_at_ms))
        .add("min_active_build_sequence", jsonUInt(m.min_active_build_sequence))
        .add("gc_fenced", jsonBool(m.gc_fenced))
        .add("write_attempt_id", jsonHex(m.write_attempt_id))
        .str();
}

String renderGcLease(const GcLease & l)
{
    return JsonObj()
        .add("owner", jsonHex(l.owner))
        .add("seq", jsonUInt(l.seq))
        .str();
}

String renderGcState(const GcState & s)
{
    return JsonObj()
        .add("round", jsonUInt(s.round))
        .add("gc_shards", jsonUInt(s.gc_shards))
        .add("snap_generation", jsonUInt(s.snap_generation))
        .add("snap_pruned_through", jsonUInt(s.snap_pruned_through))
        .add("snap_attempt", jsonUInt(s.snap_attempt))
        .add("manifest_sweep_cursor", jsonEscape(s.manifest_sweep_cursor))
        .add("lease", renderGcLease(s.lease))
        .str();
}

/// A recorded incarnation's value is an opaque backend-native string (e.g. an S3 ETag) — NOT a
/// 128-bit hash — so it renders verbatim (escaped), not hex-converted; `type` is the dialect word,
/// naming which backend family minted it.
String renderPersistedEtag(const PersistedEtag & inc)
{
    return JsonObj()
        .add("value", jsonEscape(inc.value))
        .add("type", jsonEscape(inc.dialect))
        .str();
}

String renderRunRef(const RunRef & r)
{
    return JsonObj()
        .add("key", jsonEscape(r.key))
        .add("checksum", jsonHex(r.checksum))
        .add("shard", jsonUInt(r.shard))
        .add("generation", jsonUInt(r.key_generation))
        .str();
}

String renderRefCoverage(const RefCoverage & c)
{
    return JsonObj()
        .add("classification", jsonEscape(coverageClassToWord(c.classification)))
        .add("last_folded_ref_id", renderRefTxnIdObj(c.last_folded_ref_id))
        .str();
}

String renderFoldSeal(const CasFoldSeal & seal)
{
    JsonObj ref_lives;
    for (const auto & [life_id, state] : seal.ref_lives)
        ref_lives.add(renderIncarnation(life_id), JsonObj()
            .add("coverage", renderRefCoverage(state.coverage))
            .add("cleanup_evidence", state.cleanup_evidence
                ? JsonObj().add("remove_txn_id", renderRefTxnIdObj(state.cleanup_evidence->remove_txn_id)).str()
                : "null")
            .str());

    std::vector<String> blob_target_runs;
    blob_target_runs.reserve(seal.blob_target_runs.size());
    for (const auto & r : seal.blob_target_runs)
        blob_target_runs.push_back(renderRunRef(r));

    /// A fold seal carries per-GC-shard totals for `RunMarker::Condemned` rows in its source runs. Render the
    /// summary from the seal itself; the older separate retired-reference object is no longer part
    /// of the current layout.
    JsonObj condemned_summary;
    for (const auto & [shard, cs] : seal.condemned_summary)
        condemned_summary.add(std::to_string(shard), JsonObj()
            .add("condemned_total", jsonUInt(cs.condemned_total))
            .add("pending_total", jsonUInt(cs.pending_total))
            .add("oldest_nonpending_condemn_round", jsonUInt(cs.oldest_nonpending_condemn_round))
            .str());

    return JsonObj()
        .add("generation", jsonUInt(seal.generation))
        .add("parent_generation", jsonUInt(seal.parent_generation))
        .add("ref_lives", ref_lives.str())
        .add("blob_target_runs", jsonArray(blob_target_runs))
        .add("condemned_summary", condemned_summary.str())
        .str();
}

String renderProvenance(const Provenance & p)
{
    return JsonObj()
        .add("created_at_ms", jsonUInt(p.created_at_ms))
        .add("creator_server_id", jsonHex(p.creator_server_id))
        .add("ch_version", jsonUInt(p.ch_version))
        .add("op", jsonEscape(provenanceOpToWireWord(p.op)))
        .str();
}

/// The per-hash `.meta` descriptor is the blob body's sibling and records its freshness state
/// (`Clean` or `Condemned`), not its payload. It is rendered separately from `renderEnvelopeHeader`:
/// the body remains an enveloped object, while the descriptor has its own format.
String renderBlobMeta(const BlobMeta & m)
{
    return JsonObj()
        .add("object", jsonEscape("blob_meta"))
        .add("version", jsonUInt(m.version))
        .add("state", jsonEscape(metaStateToWireWord(m.state)))
        .add("condemn_round", jsonUInt(m.condemn_round))
        .add("size", jsonUInt(m.size))
        .str();
}

String renderEnvelopeHeader(const EnvelopeHeader & h)
{
    return JsonObj()
        .add("kind", jsonEscape(objectKindToWord(h.kind)))
        /// The blob identity is carried by the object key, so the envelope keeps only the provenance
        /// fields needed for forensics (`chver` and `build`) together with its compatibility version.
        .add("compatibility_version", jsonUInt(h.compatibility_version))
        .add("incarnation_tag", jsonHex(h.incarnation_tag))
        .add("build_id", jsonHex(h.build_id))
        .add("header_len", jsonUInt(h.header_len))
        .add("provenance", h.provenance ? renderProvenance(*h.provenance) : "null")
        .add("intended_ref", h.intended_ref ? jsonEscape(*h.intended_ref) : "null")
        .str();
}

/// The word vocabulary a row's marker byte renders as, matching the `cas_run` NDJSON's own `mark` field
/// words (`runMarkerToWireWord`) so cas-inspect speaks the same vocabulary
/// as the on-disk format rather than inventing a second one.
String sourceEdgeRowKindName(RunMarker marker)
{
    return String(runMarkerToWireWord(marker));
}

String renderCondemnedRow(const CondemnedRow & r)
{
    return JsonObj()
        .add("delete_pending", jsonBool(r.delete_pending))
        .add("token", renderPersistedEtag(r.token))
        .add("size", jsonUInt(r.size))
        .add("condemn_round", jsonUInt(r.condemn_round))
        .add("marker_confirmed", jsonBool(r.marker_confirmed))
        .str();
}

/// Renders one blob-target source-edge run segment (`Layout::blobTargetRunKey`): every row (edge,
/// zero-marker, or condemned sentinel), plus a summary. `parsed` carries the run's own coordinates
/// recovered from the key; `bytes` is decoded with the same typed `SourceEdgeRunView` reader the fold /
/// `zeroInDegree` / `fsck` consumers use (the memory overload, since `caInspectToJson` is a pure
/// function of (key, bytes) with no backend access here). A malformed key or payload propagates the
/// codec's own `CORRUPTED_DATA` (`SourceEdgeKeyCodec::parse`, `decodeCondemnedRow`) -- rows are never
/// silently skipped.
String renderBlobTargetRun(const ParsedBlobTargetRunKey & parsed, std::string_view bytes)
{
    SourceEdgeRunView reader = openSourceEdgeRun(bytes);

    std::vector<String> rows;
    std::set<BlobRef> distinct_blobs;
    uint64_t edge_count = 0;
    uint64_t condemned_count = 0;
    uint64_t zero_marker_count = 0;

    String key;
    String payload;
    while (reader.next(key, payload))
    {
        BlobRef ref;
        UInt128 source_id;
        SourceEdgeKeyCodec::parse(key, ref, source_id);   // throws CORRUPTED_DATA on a malformed key (fail-closed)
        if (payload.empty())
            throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
                "cas-inspect: source-edge run row for blob {} has an empty payload", blobIdOf(ref));
        const RunMarker marker = runMarkerFromByte(payload[0], "cas-inspect: source-edge run row");

        distinct_blobs.insert(ref);
        JsonObj row;
        row.add("blob", jsonEscape(blobIdOf(ref)))
           /// `source_id` is a `CityHash128` of (namespace, writer_epoch, build_sequence,
           /// manifest_ordinal, path) -- not invertible here, so it renders as plain hex, exactly like
           /// every other opaque u128 identifier in this file.
           .add("source_id", jsonHex(source_id))
           .add("kind", jsonEscape(sourceEdgeRowKindName(marker)));

        switch (marker)
        {
            case RunMarker::Edge:
                ++edge_count;
                break;
            case RunMarker::Zero:
                ++zero_marker_count;
                break;
            case RunMarker::Condemned:
                ++condemned_count;
                row.add("condemned", renderCondemnedRow(decodeCondemnedRow(payload)));   // CORRUPTED_DATA on malformed (fail-closed)
                break;
        }
        rows.push_back(row.str());
    }

    return JsonObj()
        .add("object", jsonEscape("blob_target_run"))
        .add("generation", jsonUInt(parsed.generation))
        .add("attempt", jsonUInt(parsed.attempt))
        .add("shard", jsonUInt(parsed.shard))
        .add("seq", jsonUInt(parsed.seq))
        .add("rows", jsonArray(rows))
        .add("summary", JsonObj()
            .add("rows", jsonUInt(rows.size()))
            .add("distinct_blobs", jsonUInt(distinct_blobs.size()))
            .add("edges", jsonUInt(edge_count))
            .add("condemned", jsonUInt(condemned_count))
            .add("zero_markers", jsonUInt(zero_marker_count))
            .str())
        .str();
}

}

String caInspectToJson(const Layout & layout, const String & key, std::string_view bytes,
                       const std::optional<NamespaceLifeId> & resolved_life)
{
    /// Most-specific first: `cas/manifests/.../NNNNNN.zst` before the pool-wide `cas/ns/stream/`
    /// prefix, the `/mount` and `/fold_seal` suffixes before the pool-wide `gc/state` exact match,
    /// and the `.meta` sibling suffix before the bare `blobs/` prefix it also matches.
    if (key.starts_with(layout.casManifestsPrefix()) && key.ends_with(storedSuffix(FormatId::PartManifest)))
        return renderPartManifest(decodePartManifest(openObject(FormatId::PartManifest, bytes)));

    const auto requireResolvedLife = [&](NamespaceLifePhysicalId life_id) -> const NamespaceLifeId &
    {
        if (!resolved_life || resolved_life->incarnation != life_id)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                "cas-inspect: life_id {} has no unique resolution in the supplied catalog cut",
                renderIncarnation(life_id));
        return *resolved_life;
    };

    if (key.starts_with(layout.namespaceStateRootPrefix()))
    {
        if (const auto life_id = layout.parseRefCkptKey(key))
            return renderRefCkpt(requireResolvedLife(*life_id).ns, decodeRefCkpt(bytes));
    }

    if (key.starts_with(layout.casRefsPrefix()))
    {

        const auto parsed = layout.parseRefObjectKey(key);
        if (!parsed)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                "cas-inspect: key under cas/ns/stream is not a recognized ref-object key '{}'", key);
        const NamespaceLifeId & life = requireResolvedLife(parsed->life_id);
        if (parsed->kind == RefObjectKind::Snap)
            return renderRefTableSnapshot(decodeRefTableSnapshot(
                openObject(FormatId::RefSnapshot, bytes), life.ns.string(), parsed->txn_id));
        if (parsed->kind == RefObjectKind::Log)
            return renderRefLogTxn(decodeRefLogTxn(
                openObject(FormatId::RefLog, bytes), life.ns.string(), parsed->txn_id));
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "cas-inspect: unhandled ref-object kind for key '{}'", key);
    }

    if (key == layout.gcStateKey())
        return renderGcState(decodeGcState(bytes));

    if (key.ends_with("/mount"))
        return renderMountLease(decodeMountLease(bytes));

    if (key.ends_with("/fold_seal"))
        return renderFoldSeal(decodeFoldSeal(bytes));

    /// Blob-target source-edge run segments (`Layout::blobTargetRunKey`) are the ground truth for
    /// every in-degree question, so they get a typed decode too, not just the fold seal that names
    /// them. Checked before the pool-wide `blobs/` prefix below (disjoint anyway -- these keys live
    /// under `gc/gen/`, never `blobs/` -- but most-specific-first stays the dispatch's rule).
    if (const auto parsed = layout.parseBlobTargetRunKey(key))
        return renderBlobTargetRun(*parsed, bytes);

    /// `blobMetaKey(id) == blobKey(id) + ".meta"`, so a meta descriptor also matches
    /// `blobsPrefix()` below. Check it first or it would be decoded incorrectly as an envelope. A
    /// non-`.meta` blob body still carries its envelope.
    if (key.starts_with(layout.blobsPrefix()) && key.ends_with(".meta"))
        return renderBlobMeta(decodeBlobMeta(bytes));

    if (key.starts_with(layout.blobsPrefix()))
        return renderEnvelopeHeader(decodeEnvelopeHeader(bytes, bytes.size(), ObjectKind::Blob));

    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
        "cas-inspect: unrecognized key layout '{}' (recognized: cas/ns/stream, cas/ns/state, cas/manifests, "
        "gc/server-roots/*/mount, gc/state, gc/gen/*/fold_seal, gc/gen/*/attempt/*/blob_target/*/*, "
        "retired, blobs, blobs/*.meta)", key);
}

}
