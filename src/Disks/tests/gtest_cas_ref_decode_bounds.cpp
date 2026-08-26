#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <cstddef>
#include <string>

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
}

using namespace DB::Cas;
using DB::Cas::tests::expectThrowsCode;

/// Stage-1 T11 (spec §3 "Byte limits: encode-side estimation machinery is what dies; the decode-side
/// cap stays"). Two closures:
///
///  1. `openObject`'s raw (uncompressed) arm skipped `object_cap` entirely -- only the zstd arm checked
///     the declared decompressed content size against it. A tolerated-unknown-field-padded or raw-body
///     object up to `object_cap` would decode as though it were within budget just because it skipped
///     compression. Fixed by gating the raw arm on the SAME cap.
///  2. The writer's post-encode budget check (`checkBudget`, called from `encodeRefLogTxn`) must be a
///     real `if`+`throw` (CORRUPTED_DATA), never a debug-only `chassert` -- verified here, not
///     re-implemented (it was already a runtime throw as of stage-1 T8).

namespace
{

/// A single `SetPublishedAt` op whose `ref_name` is padded so its own encoded size (`encodedOpSize`)
/// is exactly `target_bytes` -- same construction as `gtest_cas_ref_chunked_flush.cpp`'s helper of
/// the same shape (not shared: each test file owns its small fixture helpers).
RefOp paddedSetPublishedAtOp(size_t target_bytes)
{
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "r";
    op.expected_manifest_ref = ManifestRef{1, 1, 1};
    op.published_at_ms = 0;
    const size_t base = encodedOpSize(op);
    op.ref_name = "r" + String(target_bytes - base, 'a');
    return op;
}

}

/// ---------------------------------------------------------------------------------------------
/// `openObject`: object_cap must gate a raw (uncompressed) body exactly as it gates a zstd frame's
/// declared content size -- skipping compression must never also skip the size cap.
/// ---------------------------------------------------------------------------------------------

TEST(CASRefDecodeBounds, RawOverCapObjectRejected)
{
    const FormatTraits & t = traitsFor(FormatId::RefLog);
    ASSERT_NE(t.object_cap, 0u);

    /// A raw body strictly larger than the format's object cap. It carries no valid header at all --
    /// the raw arm returns bytes verbatim (or, once fixed, rejects them by size) before any JSON
    /// parsing happens, so the content need not be well-formed.
    const String oversized(t.object_cap + 1, 'x');
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { openObject(FormatId::RefLog, oversized); });
}

TEST(CASRefDecodeBounds, RawAtCapObjectAccepted)
{
    /// The boundary itself must stay legal: exactly `object_cap` bytes, raw, still opens unchanged.
    const FormatTraits & t = traitsFor(FormatId::RefLog);
    const String at_cap(t.object_cap, 'x');
    EXPECT_EQ(openObject(FormatId::RefLog, at_cap), at_cap);
}

/// ---------------------------------------------------------------------------------------------
/// `checkBudget` (decode side): the whole-object byte cap is measured over the ACTUAL decoded bytes,
/// not accumulated per-op, so padding smuggled through a tolerant unknown field is caught exactly like
/// padding smuggled through an oversized raw body.
/// ---------------------------------------------------------------------------------------------

TEST(CASRefDecodeBounds, PaddedNormalTxnOver20MiBRejected)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};

    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "r";
    op.expected_manifest_ref = ManifestRef{1, 1, 1};
    op.published_at_ms = 1;
    txn.ops.push_back(op);

    const String text = encodeRefLogTxn(txn);
    ASSERT_GE(text.size(), 2u);
    ASSERT_EQ(text[text.size() - 1], '\n');
    ASSERT_EQ(text[text.size() - 2], '}');

    /// Pad the trailer line with an unknown tolerant field ("zz") -- legal per the wire's evolution
    /// policy (`skipUnknown`) -- inflating the decoded object well past `ref_txn_max_bytes` without
    /// touching a single op line or the op count. Padding an op line would only trip the per-op cap
    /// and prove nothing about this (much larger) whole-transaction bound.
    constexpr size_t pad_bytes = ref_txn_max_bytes + (1 << 20);
    String padded = text.substr(0, text.size() - 2);
    padded += ",\"zz\":\"" + String(pad_bytes, 'A') + "\"}\n"; // NOLINT(modernize-raw-string-literal): mixes '\"' quoting with '\n' line endings across this concatenated literal; a raw string can't hold the newline as-is.
    ASSERT_GT(padded.size(), ref_txn_max_bytes);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefLogTxn(padded, txn.ns, txn.txn_id); });
}

/// ---------------------------------------------------------------------------------------------
/// Writer side: the post-encode budget check is a real `if`+`throw`, never a debug-only `chassert` --
/// a release build must reject an over-cap encode, not silently persist it.
/// ---------------------------------------------------------------------------------------------

TEST(CASRefDecodeBounds, WriterPostEncodeThrowIsRuntime)
{
    /// Constructed directly at the codec level -- bypassing the ledger's op-count admission gate
    /// (`ref_txn_max_ops`) -- so the transaction's total encoded size alone drives the outcome: the
    /// canonical writer can never reach this state through admission (at most `ref_txn_max_ops` ops at
    /// `ref_op_max_bytes` each stays under `ref_txn_max_bytes`), but `encodeRefLogTxn`'s own post-encode
    /// `checkBudget` call must still catch a direct over-cap construction as a real exception, not an
    /// assert that a release build would silently skip.
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};

    constexpr size_t op_count = ref_txn_max_bytes / ref_op_max_bytes + 16;
    txn.ops.reserve(op_count);
    for (size_t i = 0; i < op_count; ++i)
        txn.ops.push_back(paddedSetPublishedAtOp(ref_op_max_bytes));

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}
