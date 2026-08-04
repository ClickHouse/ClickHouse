#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasInspect.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <IO/WriteBufferFromString.h>
#include "cas_test_helpers.h"

using namespace DB::Cas;

namespace
{

ManifestRef manifestRef(uint64_t epoch, uint64_t seq, uint32_t ordinal)
{
    return ManifestRef{epoch, seq, ordinal};
}

BlobRef bh(uint64_t n)
{
    return BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(UInt128(n))};
}

}

/// Stage-1 T12 (spec §4 "RefOp payload removal"): `cas-inspect` renders the renamed `SetPublishedAt`
/// op kind, and neither the ref-log nor the ref-snapshot rendering carries a `payload_size` key --
/// `RefOp`/`RefCommittedRow` no longer have a `payload` field to size.

TEST(CASInspect, RendersSetPublishedAtOpWithNoPayloadSizeKey)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/db/tbl"};
    const RefTxnId id{7, 9};

    RefLogTxn txn;
    txn.ns = ns.string();
    txn.txn_id = id;
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "all_1_1_0";
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    op.published_at_ms = 42;
    txn.ops.push_back(op);

    const String key = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), id);
    const String bytes = sealObject(FormatId::RefLog, encodeRefLogTxn(txn));

    const String json = caInspectToJson(layout, key, bytes, DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_NE(json.find(R"("kind":"SetPublishedAt")"), String::npos) << json;
    EXPECT_EQ(json.find("payload"), String::npos) << json;
}

/// Task-1 review finding M5: `cas inspect` renders the new `EpochSeal` op kind and the txn-level
/// `prev_epoch_seal` chain field, needed to debug INV-2 seal chains without a raw byte dump.
TEST(CASInspect, RendersEpochSealTxnWithPrevEpochSeal)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/db/tbl"};
    const RefTxnId id{3, 1};

    RefLogTxn txn;
    txn.ns = ns.string();
    txn.txn_id = id;
    txn.prev_epoch_seal = RefTxnId{2, 9};
    RefOp op;
    op.kind = RefOpKind::EpochSeal;
    txn.ops.push_back(op);

    const String key = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), id);
    const String bytes = sealObject(FormatId::RefLog, encodeRefLogTxn(txn));

    const String json = caInspectToJson(layout, key, bytes, DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_NE(json.find(R"("kind":"EpochSeal")"), String::npos) << json;
    EXPECT_NE(json.find(R"("prev_epoch_seal":{"writer_epoch":2,"ref_sequence":9})"), String::npos) << json;
}

TEST(CASInspect, RendersCommittedRowWithNoPayloadSizeKey)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/db/tbl"};
    const RefTxnId id{7, 9};

    RefTableSnapshot snap;
    snap.ns = ns.string();
    snap.snapshot_id = id;
    RefCommittedRow row;
    row.ref_name = "all_1_1_0";
    row.manifest_ref = manifestRef(1, 1, 1);
    row.published_at_ms = 42;
    snap.committed.push_back(row);

    const String key = layout.refSnapshotKey(DB::Cas::tests::fixture::fixtureLife(ns), id);
    const String bytes = sealObject(FormatId::RefSnapshot, encodeRefTableSnapshot(snap));

    const String json = caInspectToJson(layout, key, bytes, DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_EQ(json.find("payload"), String::npos) << json;
    EXPECT_EQ(json.find("lifecycle"), String::npos) << json;
    EXPECT_EQ(json.find("remove_txn_id"), String::npos) << json;
    EXPECT_NE(json.find(R"("published_at_ms":42)"), String::npos) << json;
}

/// A blob-target source-edge run segment (`Layout::blobTargetRunKey`) is the ground truth for every
/// in-degree question; `cas-inspect` decodes it with the typed `SourceEdgeRunView` reader (not by hand)
/// and must distinguish an active edge from a condemned sentinel row, decoding the latter's fields.
TEST(CASInspect, RendersBlobTargetRunEdgeAndCondemnedRows)
{
    const Layout layout("p");

    /// `SourceEdgeRunWriter::append` requires non-decreasing `(ref, source_id)` order; the condemned
    /// sentinel sorts first for its blob (source_id 0), and `bh(1) < bh(2)`, so appending in this
    /// order already satisfies it.
    SourceEdgeRecord condemned_rec;
    condemned_rec.ref = bh(1);
    condemned_rec.source_id = UInt128{0};
    condemned_rec.marker = kCondemned;
    condemned_rec.delete_pending = true;
    condemned_rec.token = Token{.value = "etag-1", .type = TokenType::Emulated};
    condemned_rec.size = 123;
    condemned_rec.condemn_round = 7;
    condemned_rec.marker_confirmed = true;

    SourceEdgeRecord edge_rec;
    edge_rec.ref = bh(2);
    edge_rec.source_id = UInt128(9);
    edge_rec.marker = kEdgeActive;

    DB::WriteBufferFromOwnString out;
    SourceEdgeRunWriter writer(out);
    writer.append(condemned_rec);
    writer.append(edge_rec);
    writer.finish();
    out.finalize();
    const String bytes = out.str();

    const String key = layout.blobTargetRunKey(/*generation*/2, /*attempt*/0, /*shard*/0, /*seq*/0);

    const String json = caInspectToJson(layout, key, bytes);
    EXPECT_NE(json.find(R"("object":"blob_target_run")"), String::npos) << json;
    EXPECT_NE(json.find(R"("generation":2)"), String::npos) << json;
    EXPECT_NE(json.find(R"("kind":"edge")"), String::npos) << json;
    EXPECT_NE(json.find(R"("kind":"condemned")"), String::npos) << json;
    EXPECT_NE(json.find(R"("delete_pending":true)"), String::npos) << json;
    EXPECT_NE(json.find(R"("condemn_round":7)"), String::npos) << json;
    EXPECT_NE(json.find(R"("value":"etag-1")"), String::npos) << json;
    EXPECT_NE(json.find(R"("rows":2)"), String::npos) << json;
    EXPECT_NE(json.find(R"("distinct_blobs":2)"), String::npos) << json;
    EXPECT_NE(json.find(R"("edges":1)"), String::npos) << json;
    EXPECT_NE(json.find(R"("condemned":1)"), String::npos) << json;
    EXPECT_NE(json.find(R"("zero_markers":0)"), String::npos) << json;
}

/// Stage A task 5 (spec INV-4): the `_ckpt` renders as its own object kind. It is point-addressed in
/// `cas/ns/state/` with no transaction id, so it has a separate dispatch from stream objects and once
/// fell through to
/// `BAD_ARGUMENTS` for it -- and it is precisely the object an operator reaches for when asking "what
/// is recovery's base" or "why is cleanup not reclaiming anything".
TEST(CASInspect, RendersRefCkptWithEveryFieldPresent)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/db/tbl"};

    const RefCkpt ckpt{.life_epoch = std::optional<uint64_t>{7},
                       .committed_through = RefTxnId{7, 9},
                       .checkpoint_snapshot_id = RefTxnId{7, 9},
                       .last_epoch_seal = RefTxnId{6, 4}};

    const String json = caInspectToJson(
        layout, layout.refCkptKey(DB::Cas::tests::fixture::fixtureLife(ns)), encodeRefCkpt(ckpt),
        DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_NE(json.find(R"("object":"ref_ckpt")"), String::npos) << json;
    /// The namespace comes from the KEY: a `_ckpt` body does not name it.
    EXPECT_NE(json.find(R"("namespace":"srv1/db/tbl")"), String::npos) << json;
    EXPECT_NE(json.find(R"("life_epoch":7)"), String::npos) << json;
    EXPECT_NE(json.find(R"("committed_through":{"writer_epoch":7,"ref_sequence":9})"), String::npos) << json;
    EXPECT_NE(json.find(R"("writer_epoch":7,"ref_sequence":9)"), String::npos) << json;
    EXPECT_NE(json.find(R"("writer_epoch":6,"ref_sequence":4)"), String::npos) << json;
}

/// The absences are the interesting readings, so they render as explicit `null`s rather than missing
/// keys: no checkpoint means recovery has no base AND nothing is deletable, which is a very different
/// report from "the key is there and I could not tell you what is in it".
TEST(CASInspect, RendersRefCkptAbsencesAsExplicitNulls)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/db/fresh"};

    const String json = caInspectToJson(
        layout, layout.refCkptKey(DB::Cas::tests::fixture::fixtureLife(ns)), encodeRefCkpt(RefCkpt{}),
        DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_NE(json.find(R"("object":"ref_ckpt")"), String::npos) << json;
    EXPECT_NE(json.find(R"("life_epoch":null)"), String::npos) << json;
    EXPECT_NE(json.find(R"("checkpoint_snapshot_id":null)"), String::npos) << json;
    EXPECT_NE(json.find(R"("last_epoch_seal":null)"), String::npos) << json;
}

/// A listed physical id cannot supply a namespace. Inspect must receive the unique catalog join, and
/// a different logical spelling at the same id is rejected by the decoded object's own namespace.
TEST(CASInspect, RefObjectRequiresTheExactCatalogResolution)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/db/tbl"};
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(ns, UInt128{91});
    RefLogTxn txn;
    txn.ns = ns.string();
    txn.txn_id = RefTxnId{1, 1};
    RefOp birth;
    birth.kind = RefOpKind::NamespaceBirth;
    txn.ops = {birth};
    const String key = layout.refLogKey(life, txn.txn_id);
    const String bytes = sealObject(FormatId::RefLog, encodeRefLogTxn(txn));

    EXPECT_THROW(caInspectToJson(layout, key, bytes), DB::Exception);
    EXPECT_THROW(caInspectToJson(
        layout, key, bytes, NamespaceLifeId::fromCatalogEntry(RootNamespace{"redirected"}, life.incarnation)),
        DB::Exception);
    EXPECT_NO_THROW(caInspectToJson(layout, key, bytes, life));
}
