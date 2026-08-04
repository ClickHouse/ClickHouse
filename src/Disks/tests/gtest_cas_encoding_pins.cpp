#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRecordStreamFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <IO/WriteBufferFromString.h>
#include <fmt/format.h>

using namespace DB;
using namespace DB::Cas;

/// These literals pin the CANONICAL BYTES of the CAS text encoders as of the commit that
/// introduced this file. The CasJsonWriter migration (2026-07-20 spec) must keep every one of
/// them green UNMODIFIED: canonical text is byte-compared on retries and deterministic adoption,
/// and the incremental ref budget counters assume these exact sizes. Never edit an expected
/// string here to make a test pass — that means the encoder's bytes drifted, which is the bug.

TEST(CASEncodingPins, RefLogTxnAllOpKinds)
{
    RefLogTxn txn;
    txn.ns = "roots/pin";
    txn.txn_id = RefTxnId{7, 9};

    RefOp birth;
    birth.kind = RefOpKind::NamespaceBirth;
    txn.ops.push_back(birth);

    RefOp transition;
    transition.kind = RefOpKind::OwnerTransition;
    transition.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, "20260101_0_1_1_1", ManifestRef{1, 2, 3}};
    transition.new_binding = RefOwnerBinding{RefOwnerKind::Committed, "20260101_0_1_1_1", ManifestRef{1, 2, 3}};
    txn.ops.push_back(transition);

    RefOp set_published_at;
    set_published_at.kind = RefOpKind::SetPublishedAt;
    /// NOTE the split literals: "\x01" "e" (else the hex escape would swallow the 'e') and
    /// "\xA8" "f" (else it would swallow the 'f'). `checkCanonicalRefName` forbids '\\' and NUL but
    /// not quote/newline/control bytes/U+2028, so `ref_name` -- the only free-form string `RefOp`
    /// still carries now that `payload` is gone -- exercises quote, newline, a bare control byte,
    /// and the three-byte U+2028 sequence. Backslash escaping is pinned separately, over an
    /// unrestricted string, by `gtest_cas_json_writer.cpp`'s `CASJsonWriterEscaping` suite.
    set_published_at.ref_name = String("20260101_0_1_1_1\"c\nd") + "\x01" "e" + "\xE2\x80\xA8" "f";
    set_published_at.expected_manifest_ref = ManifestRef{1, 2, 3};
    set_published_at.published_at_ms = 1234;
    txn.ops.push_back(set_published_at);

    RefOp removal;
    removal.kind = RefOpKind::RemoveNamespace;
    txn.ops.push_back(removal);

    const String expected = fmt::format("{{\"type\":\"cas_ref_log\",\"v\":{}}}\n", currentCompatibilityVersion()) +
        "{\"ns\":\"roots/pin\",\"we\":\"7\",\"rs\":\"9\"}\n"
        "{\"op\":\"namespace_birth\"}\n"
        "{\"op\":\"owner_transition\",\"obk\":\"precommit\",\"orn\":\"20260101_0_1_1_1\","
        "\"ome\":\"1\",\"omb\":\"2\",\"omo\":3,\"nbk\":\"committed\",\"nrn\":\"20260101_0_1_1_1\","
        "\"nme\":\"1\",\"nmb\":\"2\",\"nmo\":3}\n"
        "{\"op\":\"set_published_at\",\"rn\":\"20260101_0_1_1_1\\\"c\\nd\\u0001e\\u2028f\","
        "\"me\":\"1\",\"mb\":\"2\",\"mo\":3,\"ts\":1234}\n"
        "{\"op\":\"remove_namespace\"}\n"
        "{\"n\":4}\n";
    EXPECT_EQ(encodeRefLogTxn(txn), expected);
}

TEST(CASEncodingPins, RefSnapshotLive)
{
    RefTableSnapshot snap;
    snap.ns = "roots/pin";
    snap.snapshot_id = RefTxnId{7, 9};

    RefCommittedRow row;
    row.ref_name = "20260101_0_1_1_1";
    row.manifest_ref = ManifestRef{1, 2, 3};
    row.published_at_ms = 5;
    snap.committed.push_back(row);

    snap.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "20260102_0_2_2_2", ManifestRef{4, 5, 6}});

    const String expected = fmt::format("{{\"type\":\"cas_ref_snap\",\"v\":{}}}\n", currentCompatibilityVersion()) +
        "{\"ns\":\"roots/pin\",\"we\":\"7\",\"rs\":\"9\",\"lc\":\"live\"}\n"
        "{\"k\":\"c\",\"rn\":\"20260101_0_1_1_1\",\"me\":\"1\",\"mb\":\"2\",\"mo\":3,\"ts\":5}\n"
        "{\"k\":\"p\",\"rn\":\"20260102_0_2_2_2\",\"me\":\"4\",\"mb\":\"5\",\"mo\":6}\n"
        "{\"n\":2}\n";
    EXPECT_EQ(encodeRefTableSnapshot(snap), expected);
}

TEST(CASEncodingPins, SourceEdgeRunLines)
{
    WriteBufferFromOwnString out;
    SourceEdgeRunWriter writer(out);

    SourceEdgeRecord active;
    active.ref = BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(UInt128(2))};
    active.source_id = UInt128(5);
    active.marker = kEdgeActive;
    writer.append(active);

    writer.finish();
    out.finalize();

    /// The exact "b" rendering (algo byte + digest hex) is pinned as a whole line; the point is
    /// that Task 8's line-scratch rewrite must reproduce it byte-for-byte.
    const String text = out.str();
    const String header = fmt::format("{{\"type\":\"cas_run\",\"v\":{},\"kind\":\"source_edge\"}}\n", currentCompatibilityVersion());
    const String expected_record =
        "{\"b\":\"0100000000000000000000000000000002\",\"s\":\"00000000000000000000000000000005\",\"m\":\"edge\"}\n";
    const String trailer = "{\"n\":1}\n";
    /// There is exactly one record, so the whole buffer must be byte-identical to header + record + trailer.
    const String expected_full = header + expected_record + trailer;
    EXPECT_EQ(text, expected_full) << text;
}
