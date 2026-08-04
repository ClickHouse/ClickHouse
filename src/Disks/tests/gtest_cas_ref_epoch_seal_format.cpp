#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <vector>

/// v3 text codec tests for the `EpochSeal` record kind + strict seal grammar added to `cas_ref_log`
/// (stage A task 1, spec INV-2). Split into its own file per the plan's "prefer NEW test files"
/// constraint, rather than extending `gtest_cas_ref_log_format.cpp`. Covers: the new op kind's round
/// trip (including the meta-line `prev_epoch_seal` field), the context-free structural grammar
/// (`validateEpochSealGrammarStructural`, run by both `encodeRefLogTxn` and `decodeRefLogTxn`), and
/// the contextual required-iff rule (`validateEpochSealGrammarContextual`, exercised directly against
/// explicit `life_epoch` values -- its writer-runtime call sites land in later tasks).

using namespace DB::Cas;
using DB::Cas::tests::expectThrowsCode;

namespace
{

RefOp epochSealOp()
{
    RefOp op;
    op.kind = RefOpKind::EpochSeal;
    return op;
}

RefOp namespaceBirthOp()
{
    RefOp op;
    op.kind = RefOpKind::NamespaceBirth;
    return op;
}

}

/// ===================================================================================
/// refLogTxnIsEpochSeal / refLogTxnIsRemovalClass classification
/// ===================================================================================

TEST(CASRefEpochSealFormat, IsEpochSealTrueForSoleSealOp)
{
    RefLogTxn txn;
    txn.ops.push_back(epochSealOp());
    EXPECT_TRUE(refLogTxnIsEpochSeal(txn));
}

TEST(CASRefEpochSealFormat, IsEpochSealFalseForSealPlusOtherOp)
{
    RefLogTxn txn;
    txn.ops.push_back(epochSealOp());
    txn.ops.push_back(namespaceBirthOp());
    EXPECT_FALSE(refLogTxnIsEpochSeal(txn));
}

TEST(CASRefEpochSealFormat, IsEpochSealFalseForNonSealOp)
{
    RefLogTxn txn;
    txn.ops.push_back(namespaceBirthOp());
    EXPECT_FALSE(refLogTxnIsEpochSeal(txn));
}

TEST(CASRefEpochSealFormat, IsEpochSealFalseForEmptyOps)
{
    RefLogTxn txn;
    EXPECT_FALSE(refLogTxnIsEpochSeal(txn));
}

/// Step 3's explicit regression note: an `EpochSeal`-only op vector is not removal-class.
TEST(CASRefEpochSealFormat, RemovalClassIsFalseForEpochSeal)
{
    std::vector<RefOp> ops{epochSealOp()};
    EXPECT_FALSE(refLogTxnIsRemovalClass(ops));
}

/// ===================================================================================
/// Round trip
/// ===================================================================================

TEST(CASRefEpochSealFormat, RoundTripSealAtSequenceOneWithPrevEpochSeal)
{
    /// An empty dead epoch (3) closes with a sequence-1 seal, which is therefore itself required to
    /// carry `prev_epoch_seal` chaining to the seal that closed epoch 2 (spec INV-2's grammar: required
    /// on exactly sequence 1 of every epoch above genesis).
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{3, 1};
    txn.prev_epoch_seal = RefTxnId{2, 9};
    txn.ops.push_back(epochSealOp());

    const String bytes = encodeRefLogTxn(txn);
    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded, txn);
    ASSERT_TRUE(decoded.prev_epoch_seal.has_value());
    EXPECT_EQ(*decoded.prev_epoch_seal, (RefTxnId{2, 9}));
    ASSERT_EQ(decoded.ops.size(), 1u);
    EXPECT_EQ(decoded.ops[0].kind, RefOpKind::EpochSeal);
}

TEST(CASRefEpochSealFormat, RoundTripSealWithoutPrevEpochSeal)
{
    /// The common case: epoch 2 had real records (greatest applied sequence 5), so its closing seal
    /// lands at sequence 6 -- not sequence 1 -- and therefore must NOT carry `prev_epoch_seal`.
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{2, 6};
    txn.ops.push_back(epochSealOp());

    const String bytes = encodeRefLogTxn(txn);
    const RefLogTxn decoded = decodeRefLogTxn(bytes, txn.ns, txn.txn_id);
    EXPECT_EQ(decoded, txn);
    EXPECT_FALSE(decoded.prev_epoch_seal.has_value());
}

/// A re-encode of a decoded seal transaction is byte-identical (the encoder is a pure function of the
/// txn), matching the pin `gtest_cas_ref_log_format.cpp` keeps for the other op kinds.
TEST(CASRefEpochSealFormat, ByteIdenticalReencodeWithPrevEpochSeal)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{3, 1};
    txn.prev_epoch_seal = RefTxnId{2, 9};
    txn.ops.push_back(epochSealOp());

    const String bytes1 = encodeRefLogTxn(txn);
    const RefLogTxn decoded = decodeRefLogTxn(bytes1, txn.ns, txn.txn_id);
    const String bytes2 = encodeRefLogTxn(decoded);
    EXPECT_EQ(bytes1, bytes2);
}

/// ===================================================================================
/// Structural grammar (validateEpochSealGrammarStructural, via encode/decode -- context-free)
/// ===================================================================================

TEST(CASRefEpochSealFormat, EncodeRejectsSealTxnWithTwoSealOps)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    txn.ops.push_back(epochSealOp());
    txn.ops.push_back(epochSealOp());
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefEpochSealFormat, EncodeRejectsSealTxnWithSecondNonSealOp)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    txn.ops.push_back(epochSealOp());
    txn.ops.push_back(namespaceBirthOp());
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

/// Decode-side pin for the same op-count rule (review finding I1): `encodeRefLogTxn` can never
/// produce a 2-op seal body, so only a decode-only splice proves `decodeRefLogTxn` independently
/// re-derives the rule rather than trusting whatever the encoder produced -- deleting the structural
/// validator's call site inside `decodeRefLogTxn` would leave this the only failing test.
TEST(CASRefEpochSealFormat, DecodeRejectsSealTxnWithTwoOpsSpliced)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{2, 6};
    txn.ops.push_back(epochSealOp());
    const String bytes = encodeRefLogTxn(txn);

    const String op_line = "{\"op\":\"epoch_seal\"}\n";
    const auto op_pos = bytes.find(op_line);
    ASSERT_NE(op_pos, String::npos);
    String tampered = bytes;
    tampered.insert(op_pos, op_line);   /// two consecutive "epoch_seal" op lines now

    const String old_trailer = "{\"n\":1}\n";
    const auto trailer_pos = tampered.find(old_trailer);
    ASSERT_NE(trailer_pos, String::npos);
    tampered.replace(trailer_pos, old_trailer.size(), "{\"n\":2}\n");   /// keep the trailer honest

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefLogTxn(tampered, txn.ns, txn.txn_id); });
}

/// Decode-side pin for the same op-count rule, with a DIFFERENT second op kind -- proves the rule
/// rejects any companion op, not just a second `epoch_seal`.
TEST(CASRefEpochSealFormat, DecodeRejectsSealTxnWithSecondNonSealOpSpliced)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{2, 6};
    txn.ops.push_back(epochSealOp());
    const String bytes = encodeRefLogTxn(txn);

    const String op_line = "{\"op\":\"epoch_seal\"}\n";
    const auto op_pos = bytes.find(op_line);
    ASSERT_NE(op_pos, String::npos);
    String tampered = bytes;
    tampered.insert(op_pos + op_line.size(), "{\"op\":\"namespace_birth\"}\n");

    const String old_trailer = "{\"n\":1}\n";
    const auto trailer_pos = tampered.find(old_trailer);
    ASSERT_NE(trailer_pos, String::npos);
    tampered.replace(trailer_pos, old_trailer.size(), "{\"n\":2}\n");

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefLogTxn(tampered, txn.ns, txn.txn_id); });
}

TEST(CASRefEpochSealFormat, EncodeRejectsPrevEpochSealAtNonUnitSequence)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 2};
    txn.prev_epoch_seal = RefTxnId{1, 1};
    txn.ops.push_back(namespaceBirthOp());
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

/// Decode-side pin for the sequence-1-only rule (review finding I1). `prev_epoch_seal`'s
/// writer_epoch (1) is strictly below the transaction's own (5), satisfying the I3 chain-direction
/// rule, so this isolates the sequence-1 rule specifically rather than incidentally also tripping I3.
TEST(CASRefEpochSealFormat, DecodeRejectsPrevEpochSealAtNonUnitSequenceSpliced)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{5, 2};
    txn.ops.push_back(namespaceBirthOp());
    const String bytes = encodeRefLogTxn(txn);

    const String needle = R"("rs":"2")";
    const auto pos = bytes.find(needle);
    ASSERT_NE(pos, String::npos);
    String tampered = bytes;
    tampered.insert(pos + needle.size(), R"(,"!pse":"1","!pss":"1")");

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefLogTxn(tampered, txn.ns, txn.txn_id); });
}

/// Well-formedness (review finding M2): a zero component inside `prev_epoch_seal` is rejected the
/// same way a zero component in the primary `txn_id` is (`checkRefTxnIdNonzero`, shared code path).
TEST(CASRefEpochSealFormat, EncodeRejectsPrevEpochSealWithZeroWriterEpoch)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{3, 1};
    txn.prev_epoch_seal = RefTxnId{0, 9};
    txn.ops.push_back(epochSealOp());
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefEpochSealFormat, EncodeRejectsPrevEpochSealWithZeroRefSequence)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{3, 1};
    txn.prev_epoch_seal = RefTxnId{2, 0};
    txn.ops.push_back(epochSealOp());
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

/// Decode-side splice: `prev_epoch_seal` present as only one of its two wire fields ("!pse" without
/// "!pss") -- a shape only reachable via corrupted bytes, since the encoder always writes both
/// together. Boundary-plus-one for the additive-field decode contract (Constraint 7).
TEST(CASRefEpochSealFormat, DecodeRejectsPrevEpochSealMissingPssComponent)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{3, 1};
    txn.prev_epoch_seal = RefTxnId{2, 9};
    txn.ops.push_back(epochSealOp());
    const String bytes = encodeRefLogTxn(txn);

    const String needle = R"(,"!pss":"9")";
    const auto pos = bytes.find(needle);
    ASSERT_NE(pos, String::npos);
    String tampered = bytes;
    tampered.erase(pos, needle.size());

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefLogTxn(tampered, txn.ns, txn.txn_id); });
}

/// Chain direction (review finding I3): a seal closing epoch E always has id `{E, T+1}`, and the
/// sequence-1 transaction in the next numeric epoch must name it. This remains context-free (a
/// property of one transaction), so it belongs in the structural half; Tasks 2/6 walk this pointer
/// backwards over untrusted decoded bodies and must not have to re-derive the rule themselves.
TEST(CASRefEpochSealFormat, EncodeRejectsPrevEpochSealPointingAtSameEpoch)
{
    /// Self-pointer: prev_epoch_seal names the SAME epoch this transaction is in.
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{5, 1};
    txn.prev_epoch_seal = RefTxnId{5, 3};
    txn.ops.push_back(epochSealOp());
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

TEST(CASRefEpochSealFormat, EncodeRejectsPrevEpochSealPointingAtFutureEpoch)
{
    /// Forward-pointer: prev_epoch_seal names an epoch AFTER this transaction's own.
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{5, 1};
    txn.prev_epoch_seal = RefTxnId{9, 3};
    txn.ops.push_back(epochSealOp());
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

/// INV-2 materializes every global writer epoch for an existing life.  A sequence-1 transaction in
/// epoch E therefore chains to the seal of exactly E-1: accepting an older link would make an omitted
/// epoch look like a proved boundary and let a fold bypass its missing seal.
TEST(CASRefEpochSealFormat, EncodeRejectsPrevEpochSealSkippingImmediateEpoch)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{5, 1};
    txn.prev_epoch_seal = RefTxnId{3, 7};
    txn.ops.push_back(epochSealOp());

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefLogTxn(txn); });
}

/// A damaged object bypasses the encoder, so the decoder must independently reject the same skipped
/// link before any GC or recovery walker can treat it as boundary evidence.
TEST(CASRefEpochSealFormat, DecodeRejectsPrevEpochSealSkippingImmediateEpochSpliced)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{5, 1};
    txn.ops.push_back(namespaceBirthOp());
    const String bytes = encodeRefLogTxn(txn);

    const String needle = R"("rs":"1")";
    const auto pos = bytes.find(needle);
    ASSERT_NE(pos, String::npos);
    String tampered = bytes;
    tampered.insert(pos + needle.size(), R"(,"!pse":"3","!pss":"1")");

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefLogTxn(tampered, txn.ns, txn.txn_id); });
}

/// Decode-side pin for the chain-direction rule (review finding I3): the encoder's own check would
/// refuse to produce this shape (the two Encode* tests above pin that direction), so a splice into an
/// otherwise-valid sequence-1 body proves decode re-derives the rule independently.
TEST(CASRefEpochSealFormat, DecodeRejectsPrevEpochSealPointingAtSameOrFutureEpochSpliced)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{5, 1};
    txn.ops.push_back(namespaceBirthOp());
    const String bytes = encodeRefLogTxn(txn);   /// valid: sequence 1, no prev_epoch_seal

    const String needle = R"("rs":"1")";
    const auto pos = bytes.find(needle);
    ASSERT_NE(pos, String::npos);
    String tampered = bytes;
    tampered.insert(pos + needle.size(), R"(,"!pse":"5","!pss":"1")");   /// self-pointer

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefLogTxn(tampered, txn.ns, txn.txn_id); });
}

/// ===================================================================================
/// Contextual grammar (validateEpochSealGrammarContextual, called directly against explicit
/// life_epoch values -- the writer-runtime call sites are wired by later tasks)
/// ===================================================================================

TEST(CASRefEpochSealFormat, ContextualRejectsMissingPrevEpochSealWhenRequired)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{3, 1};
    txn.ops.push_back(namespaceBirthOp());
    /// life_epoch 1 < writer_epoch 3: a sequence-1 txn above genesis MUST carry prev_epoch_seal.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { validateEpochSealGrammarContextual(txn, /*life_epoch=*/1); });
}

TEST(CASRefEpochSealFormat, ContextualRejectsPrevEpochSealWhenForbidden)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{5, 1};
    txn.prev_epoch_seal = RefTxnId{4, 3};
    txn.ops.push_back(namespaceBirthOp());
    /// life_epoch == writer_epoch == 5: this IS the namespace's genesis sequence-1 txn, so
    /// prev_epoch_seal is forbidden -- there is no preceding epoch to chain to.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { validateEpochSealGrammarContextual(txn, /*life_epoch=*/5); });
}

/// codex r2 finding 2: "genesis" is per-namespace. A namespace first born at global epoch 5 (not
/// epoch 1) appends {5, 1} with NO prev_epoch_seal -- that IS its genesis, not a transition.
TEST(CASRefEpochSealFormat, ContextualAllowsGenesisBirthAboveEpochOneWithoutPrevEpochSeal)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{5, 1};
    txn.ops.push_back(namespaceBirthOp());
    EXPECT_NO_THROW(validateEpochSealGrammarContextual(txn, /*life_epoch=*/5));
}

/// Review finding I2: the `ref_sequence != 1` early return is load-bearing for Task 4's encode call
/// site, which calls this on every txn it mints, including ordinary sequence->=2 transactions in a
/// post-transition epoch that legitimately carry no `prev_epoch_seal`. Pinned on both sides of the
/// life_epoch relation to prove the early return fires regardless of it.
TEST(CASRefEpochSealFormat, ContextualPassesThroughNonSequenceOneAboveLifeEpoch)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{3, 5};
    txn.ops.push_back(namespaceBirthOp());
    /// writer_epoch(3) > life_epoch(1): would be REQUIRED if this were sequence 1.
    EXPECT_NO_THROW(validateEpochSealGrammarContextual(txn, /*life_epoch=*/1));
}

TEST(CASRefEpochSealFormat, ContextualPassesThroughNonSequenceOneAtOrBelowLifeEpoch)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 5};
    txn.ops.push_back(namespaceBirthOp());
    /// writer_epoch(1) == life_epoch(1): would be FORBIDDEN-if-present if this were sequence 1.
    EXPECT_NO_THROW(validateEpochSealGrammarContextual(txn, /*life_epoch=*/1));
}

/// ===================================================================================
/// Criticality of the prev_epoch_seal wire fields (review finding M4)
/// ===================================================================================

/// `!pse`/`!pss` are `!`-prefixed CRITICAL keys: `prev_epoch_seal` is INV-2 chain evidence, and a
/// build that silently dropped it would still pass the structural grammar (absent field => no check)
/// while losing the chain link. Proven here by splicing in a DIFFERENT, genuinely-unrecognized
/// `!`-key (simulating a future critical field this build predates) rather than `!pse`/`!pss`
/// themselves, which this build DOES recognize: `JsonObjectReader::skipUnknown` rejects any
/// unrecognized `!`-prefixed key with `UNKNOWN_FORMAT_VERSION` (never a silent skip), so this pins
/// the general mechanism the meta-line reader relies on to keep `!pse`/`!pss` safe against a decoder
/// that doesn't (yet, or anymore) understand them.
TEST(CASRefEpochSealFormat, DecodeRejectsUnknownCriticalKeyInMetaLine)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    txn.ops.push_back(namespaceBirthOp());
    const String bytes = encodeRefLogTxn(txn);

    const String needle = R"("rs":"1")";
    const auto pos = bytes.find(needle);
    ASSERT_NE(pos, String::npos);
    String tampered = bytes;
    tampered.insert(pos + needle.size(), R"(,"!future_critical_field":"1")");

    expectThrowsCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION, [&] { decodeRefLogTxn(tampered, txn.ns, txn.txn_id); });
}

/// ===================================================================================
/// Regression guard: existing unknown-op-word behavior stays intact after adding "epoch_seal"
/// ===================================================================================

TEST(CASRefEpochSealFormat, DecodeRejectsUnknownOpWordRegressionGuard)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{1, 1};
    txn.ops.push_back(epochSealOp());
    const String bytes = encodeRefLogTxn(txn);

    const String needle = "\"epoch_seal\"";
    const auto pos = bytes.find(needle);
    ASSERT_NE(pos, String::npos);
    String tampered = bytes;
    tampered.replace(pos, needle.size(), "\"totally_bogus_op\"");

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefLogTxn(tampered, txn.ns, txn.txn_id); });
}

/// ===================================================================================
/// Shape-level failure-mode battery (truncation / v+1 gate / wrong type / leading garbage)
/// ===================================================================================

TEST(CASRefEpochSealFormat, FormatBatteryEpochSeal)
{
    RefLogTxn txn;
    txn.ns = "ns";
    txn.txn_id = RefTxnId{3, 1};
    txn.prev_epoch_seal = RefTxnId{2, 9};
    txn.ops.push_back(epochSealOp());

    const String ns = txn.ns;
    const RefTxnId id = txn.txn_id;
    runFormatBattery({FormatId::RefLog,
        [txn] { return sealObject(FormatId::RefLog, encodeRefLogTxn(txn)); },
        [ns, id](std::string_view s) { decodeRefLogTxn(openObject(FormatId::RefLog, s), ns, id); },
        "{\"type\":\"cas_ref_log\",\"v\":9}\n"
        "{\"ns\":\"ns\",\"we\":\"3\",\"rs\":\"1\",\"!pse\":\"2\",\"!pss\":\"9\"}\n"
        "{\"op\":\"epoch_seal\"}\n"
        "{\"n\":1}\n"});
}
