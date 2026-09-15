#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <random>
#include <vector>

#include <magic_enum.hpp>

/// v3 text codec tests for `cas_ref_log` (codecs-v3 phase 3). Split out of the retired
/// `gtest_cas_ref_codecs.cpp` and re-pointed at the TEXT codec: the encoder-side validation tests are
/// format-agnostic (they only assert `encodeRefLogTxn` throws) and carry over verbatim; the old
/// binary-offset byte-patch decode tests (`bytes[k] = 99`) are gone — the shape-level corruption
/// classes (truncation, `v`+1 forward-gate, wrong type, leading garbage) are now covered by the
/// `CASFormatBattery.RefLog` row below. `RefTxnId` render/parse coverage lives here too (it rode in
/// the same suite and is independent of either ref codec).

using namespace DB::Cas;
using DB::Cas::tests::expectThrowsCode;

namespace
{

ManifestRef manifestRef(uint64_t epoch, uint64_t seq, uint32_t ordinal)
{
    return ManifestRef{epoch, seq, ordinal};
}

}

/// ===================================================================================
/// RefTxnId: render / parse
/// ===================================================================================

TEST(CASRefCodec, RenderCanonicalForm)
{
    EXPECT_EQ(renderRefTxnId(RefTxnId{7, 0x8e}), "0000000000000007-000000000000008e");
    EXPECT_EQ(renderRefTxnId(RefTxnId{1, 1}), "0000000000000001-0000000000000001");
    EXPECT_EQ(renderRefTxnId(RefTxnId{0xffffffffffffffffULL, 0xffffffffffffffffULL}),
        "ffffffffffffffff-ffffffffffffffff");
}

TEST(CASRefCodec, RenderRejectsZeroComponent)
{
    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            renderRefTxnId(RefTxnId{0, 1});
        },
        "RefTxnId: writer_epoch and ref_sequence must both be nonzero");
    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            renderRefTxnId(RefTxnId{1, 0});
        },
        "RefTxnId: writer_epoch and ref_sequence must both be nonzero");
    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            renderRefTxnId(RefTxnId{0, 0});
        },
        "RefTxnId: writer_epoch and ref_sequence must both be nonzero");
}

TEST(CASRefCodec, ParseRoundTrip)
{
    for (const RefTxnId id : {RefTxnId{7, 0x8e}, RefTxnId{1, 1}, RefTxnId{255, 2}, RefTxnId{0x100000000ULL, 3},
                               RefTxnId{0x8000000000000000ULL, 0x8000000000000000ULL},
                               RefTxnId{0xffffffffffffffffULL, 0xffffffffffffffffULL}})
    {
        const String rendered = renderRefTxnId(id);
        const auto parsed = parseRefTxnId(rendered);
        ASSERT_TRUE(parsed.has_value());
        EXPECT_EQ(*parsed, id);
    }
}

TEST(CASRefCodec, ParseRejectsShort)
{
    EXPECT_FALSE(parseRefTxnId("000000000000007-000000000000008e").has_value());   /// 32 chars, one short
    EXPECT_FALSE(parseRefTxnId("7-8e").has_value());
    EXPECT_FALSE(parseRefTxnId("").has_value());
}

TEST(CASRefCodec, ParseRejectsLong)
{
    EXPECT_FALSE(parseRefTxnId("00000000000000007-000000000000008e").has_value());  /// 34 chars, one long
    EXPECT_FALSE(parseRefTxnId("0000000000000007-000000000000008e0").has_value());
}

TEST(CASRefCodec, ParseRejectsUppercase)
{
    EXPECT_FALSE(parseRefTxnId("0000000000000007-00000000000000AE").has_value());
    EXPECT_FALSE(parseRefTxnId("0000000000000007-000000000000008E").has_value());
    EXPECT_FALSE(parseRefTxnId("0000000000000007-00000000000000Ae").has_value());  /// mixed case
}

TEST(CASRefCodec, ParseRejectsZeroComponent)
{
    EXPECT_FALSE(parseRefTxnId("0000000000000000-000000000000008e").has_value());
    EXPECT_FALSE(parseRefTxnId("0000000000000007-0000000000000000").has_value());
    EXPECT_FALSE(parseRefTxnId("0000000000000000-0000000000000000").has_value());
}

TEST(CASRefCodec, ParseRejectsNonHexGarbage)
{
    EXPECT_FALSE(parseRefTxnId("000000000000000g-000000000000008e").has_value());
    EXPECT_FALSE(parseRefTxnId("!!!!!!!!!!!!!!!!-000000000000008e").has_value());
    EXPECT_FALSE(parseRefTxnId("0000000000000007_000000000000008e").has_value());  /// wrong separator
}

TEST(CASRefCodec, ParseRejectsMisplacedSeparator)
{
    /// 17 hex digits then '-' then 15: same total length (33), dash at the wrong index -- the kind of
    /// shape that, read naively without a fixed dash position, could be mistaken for an in-range but
    /// overflowing first component.
    EXPECT_FALSE(parseRefTxnId("00000000000000078-00000000000000e").has_value());
}

TEST(CASRefCodec, OrderMatchesLexicalOrderOfRender)
{
    const std::vector<uint64_t> values{1, 2, 255, 1ULL << 32, 1ULL << 63};
    std::vector<RefTxnId> ids;
    for (uint64_t epoch : values)
        for (uint64_t seq : values)
            ids.push_back(RefTxnId{epoch, seq});

    std::mt19937 rng(42); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed is required for reproducible property coverage.
    for (int iter = 0; iter < 200; ++iter)
    {
        const RefTxnId & a = ids[rng() % ids.size()];
        const RefTxnId & b = ids[rng() % ids.size()];
        const String ra = renderRefTxnId(a);
        const String rb = renderRefTxnId(b);
        EXPECT_EQ(a < b, ra < rb) << ra << " vs " << rb;
        EXPECT_EQ(a == b, ra == rb);
    }
}

/// ===================================================================================
/// RefLogTxn: round trip
/// ===================================================================================

/// Closed-set pin: the five `RefOpKind` words, walked through `magic_enum::enum_values`, which is
/// what proves the renderer and the parser consult the SAME table: a table entry missing altogether is already a
/// build error at the coverage assert, but two delegates drifting onto different tables is not.
TEST(CASRefCodec, ClosedSetPinsRefOpKindWords)
{
    EXPECT_EQ(refOpKindToWireWord(RefOpKind::NamespaceBirth), "namespace_birth");
    EXPECT_EQ(refOpKindToWireWord(RefOpKind::OwnerTransition), "owner_transition");
    EXPECT_EQ(refOpKindToWireWord(RefOpKind::SetPublishedAt), "set_published_at");
    EXPECT_EQ(refOpKindToWireWord(RefOpKind::RemoveNamespace), "remove_namespace");
    EXPECT_EQ(refOpKindToWireWord(RefOpKind::EpochSeal), "epoch_seal");
    for (const auto k : magic_enum::enum_values<RefOpKind>())
        EXPECT_EQ(refOpKindFromWireWord(refOpKindToWireWord(k)), k);
}

TEST(CASRefCodec, RoundTripNamespaceBirth)
{
    RefLogTxn txn;
    txn.ns = "srv1/db/table@cas@";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::NamespaceBirth;
    txn.ops.push_back(op);

    const String bytes = encodeRefLogTxn(txn);
    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded, txn);
}

TEST(CASRefCodec, RoundTripRemoveNamespace)
{
    RefLogTxn txn;
    txn.ns = "srv1/db/table@cas@";
    txn.txn_id = RefTxnId{1, 2};
    RefOp op;
    op.kind = RefOpKind::RemoveNamespace;
    txn.ops.push_back(op);

    const String bytes = encodeRefLogTxn(txn);
    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded, txn);
}

TEST(CASRefCodec, RoundTripSetPublishedAt)
{
    RefLogTxn txn;
    txn.ns = "srv1/db/table@cas@";
    txn.txn_id = RefTxnId{3, 5};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "all_1_1_0";
    op.expected_manifest_ref = manifestRef(3, 4, 1);
    op.published_at_ms = 1717000000000ULL;
    txn.ops.push_back(op);

    const String bytes = encodeRefLogTxn(txn);
    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded, txn);
}

/// No-tolerance decode pin: the `"pl"` (payload) field was removed from the
/// ref-op wire. Although the retired `set_payload` op WORD is already rejected by
/// `refOpKindFromWireWord`, the generic op-record reader reads all field keys before switching on kind, so a
/// `"pl"` field paired with a still-recognized op word would otherwise be `skipUnknown`'d. It is a
/// removed field, not a genuinely-unknown one: decoding an op record that still carries `"pl"` must FAIL
/// with `CORRUPTED_DATA` naming the removed field.
TEST(CASRefCodec, DecodeRejectsRemovedPayloadFieldInOpRecord)
{
    RefLogTxn txn;
    txn.ns = "srv1/db/table@cas@";
    txn.txn_id = RefTxnId{3, 5};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "all_1_1_0";
    op.expected_manifest_ref = manifestRef(3, 4, 1);
    op.published_at_ms = 1717000000000ULL;
    txn.ops.push_back(op);

    const String bytes = encodeRefLogTxn(txn);
    /// Splice the retired `"pl"` field back into the op record, just before its `"published_ms"` field.
    const String needle = ",\"published_ms\":";
    const auto pos = bytes.find(needle);
    ASSERT_NE(pos, String::npos);
    const String tampered = bytes.substr(0, pos) + R"(,"pl":"deadbeef")" + bytes.substr(pos);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefLogTxn(tampered, txn.ns, txn.txn_id); });
}

TEST(CASRefCodec, RoundTripSetPublishedAtZeroTimestamp)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "r";
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    op.published_at_ms = 0;
    txn.ops.push_back(op);

    const String bytes = encodeRefLogTxn(txn);
    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded, txn);
}

TEST(CASRefCodec, RoundTripOwnerTransitionAdd)
{
    /// new-only = add: no old_binding, a fresh new_binding.
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "all_1_1_0", manifestRef(1, 1, 1)};
    txn.ops.push_back(op);

    const String bytes = encodeRefLogTxn(txn);
    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded, txn);
    ASSERT_TRUE(decoded.ops[0].new_binding.has_value());
    EXPECT_FALSE(decoded.ops[0].old_binding.has_value());
}

TEST(CASRefCodec, RoundTripOwnerTransitionRemoval)
{
    /// old-only = removal: an old_binding, no new_binding.
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, "all_1_1_0", manifestRef(1, 1, 1)};
    txn.ops.push_back(op);

    const String bytes = encodeRefLogTxn(txn);
    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded, txn);
    EXPECT_FALSE(decoded.ops[0].new_binding.has_value());
    ASSERT_TRUE(decoded.ops[0].old_binding.has_value());
}

TEST(CASRefCodec, RoundTripOwnerTransitionReplace)
{
    /// both present = replace.
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, "all_1_1_0", manifestRef(1, 1, 1)};
    op.new_binding = RefOwnerBinding{RefOwnerKind::Committed, "all_1_1_0", manifestRef(1, 1, 1)};
    txn.ops.push_back(op);

    const String bytes = encodeRefLogTxn(txn);
    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded, txn);
    ASSERT_TRUE(decoded.ops[0].old_binding.has_value());
    ASSERT_TRUE(decoded.ops[0].new_binding.has_value());
}

TEST(CASRefCodec, OwnerTransitionBindingGroupsAreAbsentOrComplete)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, "old", manifestRef(1, 1, 1)};
    op.new_binding = RefOwnerBinding{RefOwnerKind::Committed, "new", manifestRef(1, 1, 1)};
    txn.ops.push_back(op);
    const String bytes = encodeRefLogTxn(txn);

    const String old_group = R"(,"old_kind":"precommit","old_ref":"old","old_epoch":"1","old_build":"1","old_ord":1)";
    const auto old_group_pos = bytes.find(old_group);
    ASSERT_NE(old_group_pos, String::npos);
    String old_absent = bytes;
    old_absent.erase(old_group_pos, old_group.size());
    const RefLogTxn without_old = decodeRefLogTxn(old_absent, txn.ns, txn.txn_id);
    ASSERT_EQ(without_old.ops.size(), 1u);
    EXPECT_FALSE(without_old.ops[0].old_binding.has_value());
    EXPECT_TRUE(without_old.ops[0].new_binding.has_value());

    const String old_ref = R"(,"old_ref":"old")";
    const auto old_ref_pos = bytes.find(old_ref);
    ASSERT_NE(old_ref_pos, String::npos);
    String incomplete_old = bytes;
    incomplete_old.erase(old_ref_pos, old_ref.size());
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefLogTxn(incomplete_old, txn.ns, txn.txn_id); });

    const String new_group = R"(,"new_kind":"committed","new_ref":"new","new_epoch":"1","new_build":"1","new_ord":1)";
    const auto new_group_pos = bytes.find(new_group);
    ASSERT_NE(new_group_pos, String::npos);
    String new_absent = bytes;
    new_absent.erase(new_group_pos, new_group.size());
    const RefLogTxn without_new = decodeRefLogTxn(new_absent, txn.ns, txn.txn_id);
    ASSERT_EQ(without_new.ops.size(), 1u);
    EXPECT_TRUE(without_new.ops[0].old_binding.has_value());
    EXPECT_FALSE(without_new.ops[0].new_binding.has_value());

    const String new_ref = R"(,"new_ref":"new")";
    const auto new_ref_pos = bytes.find(new_ref);
    ASSERT_NE(new_ref_pos, String::npos);
    String incomplete_new = bytes;
    incomplete_new.erase(new_ref_pos, new_ref.size());
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefLogTxn(incomplete_new, txn.ns, txn.txn_id); });
}

/// The anomaly diagnostic identifies an object found at a key it should not occupy by reading the
/// meta line's three identity fields. It reads them through the codec's own key constants, so this
/// test is what proves the reader did not quietly stop matching when those keys were renamed: with a
/// stale spelling the tolerant reader skips every real key and the peek answers nullopt on a
/// perfectly good ref-log.
TEST(CASRefCodec, PeekReadsTheMetaIdentityOfASealedRefLog)
{
    RefLogTxn txn;
    txn.ns = "srv1/db/table@cas@";
    txn.txn_id = RefTxnId{4, 9};
    RefOp birth;
    birth.kind = RefOpKind::NamespaceBirth;
    txn.ops.push_back(birth);

    const auto peek = peekRefLogMeta(sealObject(FormatId::RefLog, encodeRefLogTxn(txn)));
    ASSERT_TRUE(peek.has_value()) << "a well-formed ref-log must identify its own writer";
    EXPECT_EQ(peek->ns, "srv1/db/table@cas@");
    EXPECT_EQ(peek->writer_epoch, 4u);
    EXPECT_EQ(peek->ref_sequence, 9u);
}

/// The other half of its contract: it identifies a writer, it never certifies an object, so anything
/// it cannot read is `nullopt` rather than an exception escaping into the anomaly report.
TEST(CASRefCodec, PeekAnswersNulloptForBytesThatAreNotARefLog)
{
    EXPECT_FALSE(peekRefLogMeta("not a sealed cas object at all").has_value());
    EXPECT_FALSE(peekRefLogMeta(sealObject(FormatId::RefLog, "{\"type\":\"cas_ref_log\",\"v\":1}\n")).has_value());
}

TEST(CASRefCodec, RoundTripMultipleOpsInOneTransaction)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{9, 100};

    RefOp birth;
    birth.kind = RefOpKind::NamespaceBirth;
    txn.ops.push_back(birth);

    RefOp add;
    add.kind = RefOpKind::OwnerTransition;
    add.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "a/b/c", manifestRef(9, 1, 1)};
    txn.ops.push_back(add);

    RefOp set_published_at;
    set_published_at.kind = RefOpKind::SetPublishedAt;
    set_published_at.ref_name = "a/b/c";
    set_published_at.expected_manifest_ref = manifestRef(9, 1, 1);
    set_published_at.published_at_ms = 42;
    txn.ops.push_back(set_published_at);

    const String bytes = encodeRefLogTxn(txn);
    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded, txn);
    EXPECT_EQ(decoded.ops.size(), 3u);
}

/// A re-encode of a decoded transaction is byte-identical (the encoder is a pure function of the txn).
TEST(CASRefCodec, ByteIdenticalReencode)
{
    RefLogTxn txn;
    txn.ns = "srv1/db/table@cas@";
    txn.txn_id = RefTxnId{9, 100};

    RefOp birth;
    birth.kind = RefOpKind::NamespaceBirth;
    txn.ops.push_back(birth);

    RefOp add;
    add.kind = RefOpKind::OwnerTransition;
    add.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, "a/b/c", manifestRef(9, 1, 1)};
    add.new_binding = RefOwnerBinding{RefOwnerKind::Committed, "a/b/c", manifestRef(9, 1, 1)};
    txn.ops.push_back(add);

    RefOp set_published_at;
    set_published_at.kind = RefOpKind::SetPublishedAt;
    set_published_at.ref_name = "a/b/c";
    set_published_at.expected_manifest_ref = manifestRef(9, 1, 1);
    set_published_at.published_at_ms = 1717000000000ULL;
    txn.ops.push_back(set_published_at);

    const String bytes1 = encodeRefLogTxn(txn);
    const RefLogTxn decoded = decodeRefLogTxn(bytes1, txn.ns, txn.txn_id);
    const String bytes2 = encodeRefLogTxn(decoded);
    EXPECT_EQ(bytes1, bytes2);
}

/// ===================================================================================
/// RefLogTxn: validation rejections (encoder-side + key/body binding + truncation)
/// ===================================================================================

TEST(CASRefCodec, EncodeRejectsZeroTxnId)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{0, 1};
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, DecodeRejectsTruncatedBuffer)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "r";
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    txn.ops.push_back(op);
    const String bytes = encodeRefLogTxn(txn);

    /// Dropping the trailing bytes leaves the final line without its '\n' terminator -> fail closed.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeRefLogTxn(bytes.substr(0, bytes.size() - 3), txn.ns, txn.txn_id); });
}

TEST(CASRefCodec, DecodeRejectsBodyNamespaceMismatch)
{
    RefLogTxn txn;
    txn.ns = "ns-a";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::NamespaceBirth;
    txn.ops.push_back(op);
    const String bytes = encodeRefLogTxn(txn);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeRefLogTxn(bytes, "ns-b", txn.txn_id); });
}

TEST(CASRefCodec, DecodeRejectsBodyTxnIdMismatch)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::NamespaceBirth;
    txn.ops.push_back(op);
    const String bytes = encodeRefLogTxn(txn);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeRefLogTxn(bytes, txn.ns, RefTxnId{1, 2}); });
}

TEST(CASRefCodec, EncodeRejectsEmptyRefName)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "";
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    txn.ops.push_back(op);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, EncodeRejectsDotRefName)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = ".";
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    txn.ops.push_back(op);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, EncodeRejectsDotDotSegment)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "a/../b";
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    txn.ops.push_back(op);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, EncodeRejectsRepeatedSeparator)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "a//b";
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    txn.ops.push_back(op);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, EncodeRejectsLeadingSlash)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "/a";
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    txn.ops.push_back(op);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, EncodeRejectsTrailingSlash)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "a/";
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    txn.ops.push_back(op);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, EncodeRejectsBackslash)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "a\\b";
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    txn.ops.push_back(op);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, EncodeRejectsNonCanonicalOwnerBindingRefName)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "..", manifestRef(1, 1, 1)};
    txn.ops.push_back(op);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, EncodeRejectsEmbeddedNulRefName)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = String("a\0b", 3);   /// embedded NUL byte -- never legitimate in a ref name
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    txn.ops.push_back(op);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, EncodeRejectsTooManyOps)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    for (size_t i = 0; i < ref_txn_max_ops + 1; ++i)
    {
        RefOp op;
        op.kind = RefOpKind::NamespaceBirth;
        txn.ops.push_back(op);
    }
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, EncodeAllowsExactlyMaxOps)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    for (size_t i = 0; i < ref_txn_max_ops; ++i)
    {
        RefOp op;
        op.kind = RefOpKind::NamespaceBirth;
        txn.ops.push_back(op);
    }
    const String bytes = encodeRefLogTxn(txn);
    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded.ops.size(), ref_txn_max_ops);
}

TEST(CASRefCodec, EncodeRejectsOversizedNormalTransaction)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "r" + String(ref_txn_max_bytes + 1, 'x');
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    txn.ops.push_back(op);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, RemovalClassTransactionLiftsByteBudgetAboveNormalLimit)
{
    /// A RemoveNamespace transaction carrying a ref_name bigger than the NORMAL limit but within the
    /// REMOVAL limit must succeed -- proving the removal-class flag actually lifts the byte budget
    /// rather than merely being ignored. The single set_published_at op here is also vastly bigger
    /// than `ref_op_max_bytes`, so this doubles as proof that removal-class ops are exempt from the
    /// per-op cap too.
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};

    RefOp remove;
    remove.kind = RefOpKind::RemoveNamespace;
    txn.ops.push_back(remove);

    RefOp set_published_at;
    set_published_at.kind = RefOpKind::SetPublishedAt;
    set_published_at.ref_name = "r" + String(ref_txn_max_bytes + 1024, 'x');
    set_published_at.expected_manifest_ref = manifestRef(1, 1, 1);
    txn.ops.push_back(set_published_at);

    const String bytes = encodeRefLogTxn(txn);
    EXPECT_GT(bytes.size(), ref_txn_max_bytes);
    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded, txn);
}

TEST(CASRefCodec, RemovalClassTransactionStillRejectsBeyondRemovalLimit)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};

    RefOp remove;
    remove.kind = RefOpKind::RemoveNamespace;
    txn.ops.push_back(remove);

    RefOp set_published_at;
    set_published_at.kind = RefOpKind::SetPublishedAt;
    set_published_at.ref_name = "r" + String(ref_removal_max_bytes + 1, 'x');
    set_published_at.expected_manifest_ref = manifestRef(1, 1, 1);
    txn.ops.push_back(set_published_at);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, RemovalClassTransactionNotCappedOnOpCount)
{
    /// A removal-class transaction may exceed `ref_txn_max_ops` -- only the (much larger) byte budget
    /// bounds it, per spec ("its operation count is bounded by that byte limit").
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};

    RefOp remove;
    remove.kind = RefOpKind::RemoveNamespace;
    txn.ops.push_back(remove);
    for (size_t i = 0; i < ref_txn_max_ops + 10; ++i)
    {
        RefOp op;
        op.kind = RefOpKind::NamespaceBirth;
        txn.ops.push_back(op);
    }

    const String bytes = encodeRefLogTxn(txn);
    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded.ops.size(), txn.ops.size());
}

/// Stage-1 T8 (spec §3 "Budget: counts only, chunked flush") retires the scenario this test used to
/// pin: ONE op carrying almost the whole `ref_txn_max_bytes` budget in its payload. The new per-op
/// cap (`ref_op_max_bytes`, `EncodeAllowsExactlyMaxPerOpBytes` below) makes that construction illegal
/// for a normal-class transaction — no single op may exceed `ref_op_max_bytes` regardless of the
/// whole-transaction budget — so the exact-boundary pin moves to the per-op cap, the boundary a
/// legally-admitted normal-class transaction can actually reach (`ref_txn_max_ops * ref_op_max_bytes`
/// stays comfortably under `ref_txn_max_bytes`, pinned by `CanonicalMaxTransactionRoundTrips` in
/// `gtest_cas_ref_chunked_flush.cpp`).

TEST(CASRefCodec, EncodeAllowsExactlyMaxPerOpBytes)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "r";
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    txn.ops.push_back(op);

    const size_t base_size = encodedOpSize(op);
    ASSERT_LE(base_size, ref_op_max_bytes);
    /// Every added 'a' is one un-escaped byte inside the JSON ref-name string, so the encoded op size
    /// grows one-for-one to exactly the per-op cap.
    txn.ops[0].ref_name = "r" + String(ref_op_max_bytes - base_size, 'a');
    ASSERT_EQ(encodedOpSize(txn.ops[0]), ref_op_max_bytes);

    const String bytes = encodeRefLogTxn(txn);
    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded, txn);
}

TEST(CASRefCodec, EncodeRejectsOversizedOpOnNormalTransaction)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "r";
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    txn.ops.push_back(op);

    const size_t base_size = encodedOpSize(op);
    ASSERT_LE(base_size, ref_op_max_bytes);
    /// One byte past the per-op cap, well within the whole-transaction byte cap -- isolates the
    /// per-op check from the (much larger) whole-transaction one.
    txn.ops[0].ref_name = "r" + String(ref_op_max_bytes - base_size + 1, 'a');
    ASSERT_GT(encodedOpSize(txn.ops[0]), ref_op_max_bytes);
    ASSERT_LT(encodedOpSize(txn.ops[0]), ref_txn_max_bytes);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, EncodeAllowsExactlyMaxRemovalBytes)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp remove;
    remove.kind = RefOpKind::RemoveNamespace;
    txn.ops.push_back(remove);
    RefOp ts_op;
    ts_op.kind = RefOpKind::SetPublishedAt;
    ts_op.ref_name = "r";
    ts_op.expected_manifest_ref = manifestRef(1, 1, 1);
    txn.ops.push_back(ts_op);

    const size_t base_size = encodeRefLogTxn(txn).size();
    ASSERT_LE(base_size, ref_removal_max_bytes);
    /// Every added 'x' is one un-escaped byte inside the JSON ref-name string, so the encoded size
    /// grows one-for-one to exactly the cap; the base "r" contributes 1 byte already counted in
    /// base_size, so appending (rather than replacing) reaches the target exactly.
    txn.ops[1].ref_name = "r" + String(ref_removal_max_bytes - base_size, 'x');

    const String bytes = encodeRefLogTxn(txn);
    EXPECT_EQ(bytes.size(), ref_removal_max_bytes);
    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded, txn);
}

/// ManifestRef field validation, enforced by the log codec (spec's "invalid identifiers are rejected"
/// binds both codecs). Encoder-side only -- the decode path re-runs the identical checks and is
/// covered by the round-trips + the battery.

TEST(CASRefCodec, EncodeRejectsZeroManifestRefWriterEpochInOwnerBinding)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "r", manifestRef(0, 1, 1)};
    txn.ops.push_back(op);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, EncodeRejectsZeroManifestRefBuildSequenceInOwnerBinding)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "r", manifestRef(1, 0, 1)};
    txn.ops.push_back(op);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, EncodeRejectsOutOfRangeManifestOrdinalInOwnerBinding)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "r", manifestRef(1, 1, 0)};
    txn.ops.push_back(op);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefCodec, EncodeRejectsZeroManifestRefInSetPublishedAt)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "r";
    op.expected_manifest_ref = manifestRef(1, 1, 0);
    txn.ops.push_back(op);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

/// ===================================================================================
/// Shape-level failure-mode battery (truncation / v+1 gate / wrong type / leading garbage)
/// ===================================================================================

CAS_BATTERY_COVERS(RefLog);

TEST(CASFormatBattery, RefLog)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "all_1_1_0";
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    op.published_at_ms = 42;
    txn.ops.push_back(op);

    const String ns = txn.ns;
    const RefTxnId id = txn.txn_id;
    runFormatBattery({FormatId::RefLog,
        [txn] { return sealObject(FormatId::RefLog, encodeRefLogTxn(txn)); },
        [ns, id](std::string_view s) { decodeRefLogTxn(openObject(FormatId::RefLog, s), ns, id); },
        currentFormatHeader("cas_ref_log") +
        "{\"namespace\":\"ns\",\"txn_epoch\":\"1\",\"txn_seq\":\"1\"}\n"
        "{\"op\":\"set_published_at\",\"ref\":\"all_1_1_0\",\"epoch\":\"1\",\"build\":\"1\",\"ord\":1,\"published_ms\":42}\n"
        "{\"n\":1}\n"});
}
