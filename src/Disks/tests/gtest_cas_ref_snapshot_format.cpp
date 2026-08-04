#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <limits>

/// v3 text codec tests for `cas_ref_snap` (codecs-v3 phase 3). Split out of the retired
/// `gtest_cas_ref_codecs.cpp` and re-pointed at the TEXT codec. The encoder-side validation tests are
/// format-agnostic and carry over verbatim; the old binary-offset byte-patch decode tests
/// (`bytes[k] = 99`) are gone -- the shape-level corruption classes (truncation, `v`+1 forward-gate,
/// wrong type, leading garbage) are covered by the `CASFormatBattery.RefSnapshot` row below, which also
/// subsumes the old `DecodeRejectsFutureFormatVersion`/`DecodeRejectsFormatVersionOne` pair (there is
/// no `format_version` byte any more -- the header `v` gate is the single forward-compat mechanism).

using namespace DB::Cas;
using DB::Cas::tests::expectThrowsCode;

namespace
{

ManifestRef manifestRef(uint64_t epoch, uint64_t seq, uint32_t ordinal)
{
    return ManifestRef{epoch, seq, ordinal};
}

RefTableSnapshot makeLiveSnapshot()
{
    RefTableSnapshot s;
    s.ns = "srv1/db/table@cas@";
    s.snapshot_id = RefTxnId{5, 200};

    RefCommittedRow c1;
    c1.ref_name = "all_1_1_0";
    c1.manifest_ref = manifestRef(5, 10, 1);
    c1.published_at_ms = 1717000000000ULL;
    s.committed.push_back(c1);

    RefCommittedRow c2;
    c2.ref_name = "all_2_2_0";
    c2.manifest_ref = manifestRef(5, 11, 1);
    c2.published_at_ms = 1717000000001ULL;
    s.committed.push_back(c2);

    RefOwnerBinding p1{RefOwnerKind::Precommit, "all_3_3_0", manifestRef(5, 12, 1)};
    s.precommits.push_back(p1);

    return s;
}

}

/// ===================================================================================
/// RefTableSnapshot: round trip
/// ===================================================================================

TEST(CASRefSnapshotCodec, RoundTripLive)
{
    const RefTableSnapshot s = makeLiveSnapshot();
    const String bytes = encodeRefTableSnapshot(s);
    const RefTableSnapshot decoded = decodeRefTableSnapshot(bytes, s.ns, s.snapshot_id);
    EXPECT_EQ(decoded, s);
}

TEST(CASRefSnapshotCodec, DecodeRequiresLifecycleField)
{
    const RefTableSnapshot s = makeLiveSnapshot();
    String bytes = encodeRefTableSnapshot(s);
    const String field = R"(,"lc":"live")";
    const size_t at = bytes.find(field);
    ASSERT_NE(at, String::npos);
    bytes.erase(at, field.size());

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeRefTableSnapshot(bytes, s.ns, s.snapshot_id); });
}

TEST(CASRefSnapshotCodec, DecodeRejectsTerminalLifecycleWord)
{
    const RefTableSnapshot s = makeLiveSnapshot();
    String bytes = encodeRefTableSnapshot(s);
    const String live = R"("lc":"live")";
    const size_t at = bytes.find(live);
    ASSERT_NE(at, String::npos);
    bytes.replace(at, live.size(), R"("lc":"removed")");

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeRefTableSnapshot(bytes, s.ns, s.snapshot_id); });
}

TEST(CASRefSnapshotCodec, DecodeRejectsRetiredRemoveTxnEpochField)
{
    const RefTableSnapshot s = makeLiveSnapshot();
    String bytes = encodeRefTableSnapshot(s);
    const String live = R"("lc":"live")";
    const size_t at = bytes.find(live);
    ASSERT_NE(at, String::npos);
    bytes.replace(at, live.size(), live + R"(,"rte":"7")");

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeRefTableSnapshot(bytes, s.ns, s.snapshot_id); });
}

TEST(CASRefSnapshotCodec, DecodeRejectsRetiredRemoveTxnSequenceField)
{
    const RefTableSnapshot s = makeLiveSnapshot();
    String bytes = encodeRefTableSnapshot(s);
    const String live = R"("lc":"live")";
    const size_t at = bytes.find(live);
    ASSERT_NE(at, String::npos);
    bytes.replace(at, live.size(), live + R"(,"rts":"9")");

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeRefTableSnapshot(bytes, s.ns, s.snapshot_id); });
}

TEST(CASRefSnapshotCodec, DecodeRejectsRetiredRemoveTxnFieldPair)
{
    const RefTableSnapshot s = makeLiveSnapshot();
    String bytes = encodeRefTableSnapshot(s);
    const String live = R"("lc":"live")";
    const size_t at = bytes.find(live);
    ASSERT_NE(at, String::npos);
    bytes.replace(at, live.size(), live + R"(,"rte":"7","rts":"9")");

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeRefTableSnapshot(bytes, s.ns, s.snapshot_id); });
}

/// No-tolerance decode pin (codex round-2, finding 3): the `"pl"` (payload) field was removed from the
/// committed-row wire in stage-1 T12. It is NOT a genuinely-unknown future field the tolerant reader may
/// skip -- silently discarding a persisted payload would lose data -- so decoding a committed row that
/// still carries `"pl"` must FAIL with `CORRUPTED_DATA` naming the removed field, not `skipUnknown` it.
TEST(CASRefSnapshotCodec, DecodeRejectsRemovedPayloadFieldInCommittedRow)
{
    RefTableSnapshot s;
    s.ns = "ns";
    s.snapshot_id = RefTxnId{1, 1};
    RefCommittedRow c;
    c.ref_name = "all_1_1_0";
    c.manifest_ref = manifestRef(5, 10, 1);
    c.published_at_ms = 1717000000000ULL;
    s.committed.push_back(c);

    const String bytes = encodeRefTableSnapshot(s);
    /// Splice the retired `"pl"` field back into the committed record, just before its `"ts"` field.
    const String needle = ",\"ts\":";
    const auto pos = bytes.find(needle);
    ASSERT_NE(pos, String::npos);
    const String tampered = bytes.substr(0, pos) + R"(,"pl":"deadbeef")" + bytes.substr(pos);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeRefTableSnapshot(tampered, s.ns, s.snapshot_id); });
}

TEST(CASRefSnapshotCodec, RoundTripLiveEmpty)
{
    RefTableSnapshot s;
    s.ns = "ns";
    s.snapshot_id = RefTxnId{1, 1};

    const String bytes = encodeRefTableSnapshot(s);
    const RefTableSnapshot decoded = decodeRefTableSnapshot(bytes, s.ns, s.snapshot_id);
    EXPECT_EQ(decoded, s);
    EXPECT_TRUE(decoded.committed.empty());
    EXPECT_TRUE(decoded.precommits.empty());
}

TEST(CASRefSnapshotCodec, ByteIdenticalReencode)
{
    const RefTableSnapshot s = makeLiveSnapshot();
    const String bytes1 = encodeRefTableSnapshot(s);
    const RefTableSnapshot decoded = decodeRefTableSnapshot(bytes1, s.ns, s.snapshot_id);
    const String bytes2 = encodeRefTableSnapshot(decoded);
    EXPECT_EQ(bytes1, bytes2);
}

TEST(CASRefSnapshotCodec, RoundTripPrecommitsSameNameDifferentManifest)
{
    /// Two builds racing for the same final ref name: same ref_name, different manifest_ref, sorted
    /// by manifest_ref as the tiebreak.
    RefTableSnapshot s;
    s.ns = "ns";
    s.snapshot_id = RefTxnId{1, 1};
    s.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "same", manifestRef(1, 1, 1)});
    s.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "same", manifestRef(1, 2, 1)});

    const String bytes = encodeRefTableSnapshot(s);
    const RefTableSnapshot decoded = decodeRefTableSnapshot(bytes, s.ns, s.snapshot_id);
    EXPECT_EQ(decoded, s);
    EXPECT_EQ(decoded.precommits.size(), 2u);
}

/// ===================================================================================
/// Large ids
/// ===================================================================================

/// `ref_sequence` is a 64-bit counter and the codec writes it as a decimal STRING, so the top of the
/// range survives a round trip without JSON's number semantics getting involved. This used to be pinned
/// through the retired sentinel seal, whose synthetic `{E-1, UINT64_MAX}` id was the only place such a
/// value arose; the representation guarantee is what actually mattered and it is pinned directly here.
TEST(CASRefSnapshotFormat, MaximalRefSequenceRoundTripsAsADecimalString)
{
    RefTableSnapshot m;
    m.ns = "ns";
    m.snapshot_id = RefTxnId{5, std::numeric_limits<uint64_t>::max()};

    const String text = encodeRefTableSnapshot(m);
    const RefTableSnapshot back = decodeRefTableSnapshot(text, m.ns, m.snapshot_id);
    EXPECT_EQ(back.snapshot_id.ref_sequence, std::numeric_limits<uint64_t>::max());
    EXPECT_NE(text.find("\"rs\":\"18446744073709551615\""), String::npos);
}

/// ===================================================================================
/// RefTableSnapshot: validation rejections (encoder-side + key/body binding + truncation)
/// ===================================================================================

TEST(CASRefSnapshotCodec, EncodeRejectsZeroSnapshotId)
{
    RefTableSnapshot s;
    s.ns = "ns";
    s.snapshot_id = RefTxnId{0, 1};
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefTableSnapshot(s); });
}

TEST(CASRefSnapshotCodec, EncodeRejectsUnsortedCommitted)
{
    RefTableSnapshot s;
    s.ns = "ns";
    s.snapshot_id = RefTxnId{1, 1};
    RefCommittedRow a;
    a.ref_name = "b";
    a.manifest_ref = manifestRef(1, 1, 1);
    RefCommittedRow b;
    b.ref_name = "a";
    b.manifest_ref = manifestRef(1, 2, 1);
    s.committed.push_back(a);
    s.committed.push_back(b);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefTableSnapshot(s); });
}

TEST(CASRefSnapshotCodec, EncodeRejectsDuplicateCommittedRefName)
{
    RefTableSnapshot s;
    s.ns = "ns";
    s.snapshot_id = RefTxnId{1, 1};
    RefCommittedRow a;
    a.ref_name = "same";
    a.manifest_ref = manifestRef(1, 1, 1);
    RefCommittedRow b;
    b.ref_name = "same";
    b.manifest_ref = manifestRef(1, 2, 1);
    s.committed.push_back(a);
    s.committed.push_back(b);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefTableSnapshot(s); });
}

TEST(CASRefSnapshotCodec, EncodeRejectsUnsortedPrecommits)
{
    RefTableSnapshot s;
    s.ns = "ns";
    s.snapshot_id = RefTxnId{1, 1};
    s.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "b", manifestRef(1, 1, 1)});
    s.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "a", manifestRef(1, 2, 1)});
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefTableSnapshot(s); });
}

TEST(CASRefSnapshotCodec, EncodeRejectsPrecommitsSameNameWrongManifestOrder)
{
    /// Same ref_name but the manifest_ref tiebreak is descending -- must be rejected even though the
    /// names alone look sorted.
    RefTableSnapshot s;
    s.ns = "ns";
    s.snapshot_id = RefTxnId{1, 1};
    s.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "same", manifestRef(1, 2, 1)});
    s.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "same", manifestRef(1, 1, 1)});
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefTableSnapshot(s); });
}

TEST(CASRefSnapshotCodec, EncodeRejectsDuplicatePrecommitBinding)
{
    RefTableSnapshot s;
    s.ns = "ns";
    s.snapshot_id = RefTxnId{1, 1};
    s.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "same", manifestRef(1, 1, 1)});
    s.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "same", manifestRef(1, 1, 1)});
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefTableSnapshot(s); });
}

TEST(CASRefSnapshotCodec, EncodeRejectsNonCanonicalCommittedRefName)
{
    RefTableSnapshot s;
    s.ns = "ns";
    s.snapshot_id = RefTxnId{1, 1};
    RefCommittedRow row;
    row.ref_name = "a/../b";
    row.manifest_ref = manifestRef(1, 1, 1);
    s.committed.push_back(row);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefTableSnapshot(s); });
}

TEST(CASRefSnapshotCodec, EncodeRejectsNonCanonicalPrecommitRefName)
{
    RefTableSnapshot s;
    s.ns = "ns";
    s.snapshot_id = RefTxnId{1, 1};
    s.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "", manifestRef(1, 1, 1)});
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefTableSnapshot(s); });
}

TEST(CASRefSnapshotCodec, EncodeRejectsPrecommitWrongKind)
{
    RefTableSnapshot s;
    s.ns = "ns";
    s.snapshot_id = RefTxnId{1, 1};
    s.precommits.push_back(RefOwnerBinding{RefOwnerKind::Committed, "r", manifestRef(1, 1, 1)});
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefTableSnapshot(s); });
}

TEST(CASRefSnapshotCodec, EncodeRejectsZeroManifestRefFields)
{
    {
        RefTableSnapshot s;
        s.ns = "ns";
        s.snapshot_id = RefTxnId{1, 1};
        RefCommittedRow row;
        row.ref_name = "r";
        row.manifest_ref = manifestRef(0, 1, 1);
        s.committed.push_back(row);
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefTableSnapshot(s); });
    }
    {
        RefTableSnapshot s;
        s.ns = "ns";
        s.snapshot_id = RefTxnId{1, 1};
        RefCommittedRow row;
        row.ref_name = "r";
        row.manifest_ref = manifestRef(1, 1, 0);   /// ordinal 0 is out of range
        s.committed.push_back(row);
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefTableSnapshot(s); });
    }
}

TEST(CASRefSnapshotCodec, EncodeRejectsOversizedSnapshot)
{
    RefTableSnapshot s;
    s.ns = "ns";
    s.snapshot_id = RefTxnId{1, 1};
    RefCommittedRow row;
    /// `ref_name` has no length limit (`checkCanonicalRefName`), so it is the padding field now that
    /// `payload` is gone: a run of un-escaped 'x' bytes inflates the encoded row one-for-one.
    row.ref_name = String(ref_snapshot_max_bytes + 1, 'x');
    row.manifest_ref = manifestRef(1, 1, 1);
    s.committed.push_back(row);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefTableSnapshot(s); });
}

TEST(CASRefSnapshotCodec, DecodeRejectsTruncatedBuffer)
{
    const RefTableSnapshot s = makeLiveSnapshot();
    const String bytes = encodeRefTableSnapshot(s);
    /// Dropping the trailing bytes leaves the final line without its '\n' terminator -> fail closed.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeRefTableSnapshot(bytes.substr(0, bytes.size() - 3), s.ns, s.snapshot_id); });
}

TEST(CASRefSnapshotCodec, DecodeRejectsNamespaceMismatch)
{
    RefTableSnapshot s;
    s.ns = "ns-a";
    s.snapshot_id = RefTxnId{1, 1};
    const String bytes = encodeRefTableSnapshot(s);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeRefTableSnapshot(bytes, "ns-b", s.snapshot_id); });
}

TEST(CASRefSnapshotCodec, DecodeRejectsSnapshotIdMismatch)
{
    RefTableSnapshot s;
    s.ns = "ns";
    s.snapshot_id = RefTxnId{1, 1};
    const String bytes = encodeRefTableSnapshot(s);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeRefTableSnapshot(bytes, s.ns, RefTxnId{1, 2}); });
}

TEST(CASRefSnapshotCodec, EncodeAllowsExactlySnapshotMaxBytes)
{
    RefTableSnapshot s;
    s.ns = "ns";
    s.snapshot_id = RefTxnId{1, 1};
    RefCommittedRow row;
    row.ref_name = "r";
    row.manifest_ref = manifestRef(1, 1, 1);
    s.committed.push_back(row);

    const size_t base_size = encodeRefTableSnapshot(s).size();
    ASSERT_LE(base_size, ref_snapshot_max_bytes);
    /// Every added 'x' is one un-escaped byte inside the JSON ref_name string, so the encoded size
    /// grows one-for-one to exactly the cap; +1 accounts for the base row's own 1-byte ref_name "r"
    /// already counted in base_size.
    s.committed[0].ref_name = String(ref_snapshot_max_bytes - base_size + 1, 'x');

    const String bytes = encodeRefTableSnapshot(s);
    EXPECT_EQ(bytes.size(), ref_snapshot_max_bytes);
    const RefTableSnapshot decoded = decodeRefTableSnapshot(bytes, s.ns, s.snapshot_id);
    EXPECT_EQ(decoded, s);
}

TEST(CASRefSnapshotCodec, DecodeRejectsOversizedBufferDirectly)
{
    /// A body with no line terminator inside the first `line_cap` bytes fails closed before any field
    /// parsing (the text `readLine` line-cap guard, the text-codec analogue of the old early size guard).
    const String oversized(ref_snapshot_max_bytes + 1, 'x');
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeRefTableSnapshot(oversized, "ns", RefTxnId{1, 1}); });
}

/// ===================================================================================
/// Shape-level failure-mode battery (truncation / v+1 gate / wrong type / leading garbage)
/// ===================================================================================

TEST(CASFormatBattery, RefSnapshot)
{
    const RefTableSnapshot s = makeLiveSnapshot();
    const String ns = s.ns;
    const RefTxnId id = s.snapshot_id;
    runFormatBattery({FormatId::RefSnapshot,
        [s] { return sealObject(FormatId::RefSnapshot, encodeRefTableSnapshot(s)); },
        [ns, id](std::string_view d) { decodeRefTableSnapshot(openObject(FormatId::RefSnapshot, d), ns, id); },
        currentFormatHeader("cas_ref_snap") +
        "{\"ns\":\"srv1/db/table@cas@\",\"we\":\"5\",\"rs\":\"200\",\"lc\":\"live\"}\n"
        "{\"k\":\"c\",\"rn\":\"all_1_1_0\",\"me\":\"5\",\"mb\":\"10\",\"mo\":1,\"ts\":1717000000000}\n"
        "{\"k\":\"c\",\"rn\":\"all_2_2_0\",\"me\":\"5\",\"mb\":\"11\",\"mo\":1,\"ts\":1717000000001}\n"
        "{\"k\":\"p\",\"rn\":\"all_3_3_0\",\"me\":\"5\",\"mb\":\"12\",\"mo\":1}\n"
        "{\"n\":3}\n"});
}
