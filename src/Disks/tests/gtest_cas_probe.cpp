#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInstrumentedBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasProbe.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
}

using namespace DB::Cas;

TEST(CASProbe, PassesOnEnforcingBackend)
{
    auto b = std::make_shared<InMemoryBackend>();
    EXPECT_NO_THROW(runCapabilityProbe(*b, "p/.cas_probe"));
    EXPECT_TRUE(b->list("p/.cas_probe", "", 10).keys.empty());   // probe cleans up after itself
}

/// AWS S3 answers 400 InvalidArgument to a conditional DELETE with an EMPTY If-Match, and the
/// probe's exit cleanup used to issue exactly that (deleteExact with the absent HeadResult's empty
/// token) after step 8 had already deleted the probe keys — two scary AWSClient <Error> log lines
/// on every real-S3 mount. The cleanup must HEAD-gate the delete instead of firing blindly.
class EmptyTokenDeleteRecorder : public InMemoryBackend
{
public:
    size_t empty_token_deletes = 0;

    DeleteOutcome deleteExact(const String & key, const Token & token) override
    {
        if (token.empty())
            ++empty_token_deletes;
        return InMemoryBackend::deleteExact(key, token);
    }
};

TEST(CASProbe, CleanupNeverDeletesWithEmptyToken)
{
    auto b = std::make_shared<EmptyTokenDeleteRecorder>();
    EXPECT_NO_THROW(runCapabilityProbe(*b, "p/.cas_probe"));
    EXPECT_EQ(b->empty_token_deletes, 0u);
}

TEST(CASProbe, FailsClosedOnNonEnforcingDelete)
{
    auto b = std::make_shared<InMemoryBackend>();
    b->setEnforceTokens(false);                                  // the MinIO-OSS failure mode
    EXPECT_THROW(runCapabilityProbe(*b, "p/.cas_probe"), DB::Exception);
}

TEST(CASProbe, FailsClosedOnDeleteMarkers)
{
    auto b = std::make_shared<InMemoryBackend>();
    b->setSimulateDeleteMarkers(true);                           // versioning enabled on the prefix
    EXPECT_THROW(runCapabilityProbe(*b, "p/.cas_probe"), DB::Exception);
}

TEST(CASProbe, PassesOnEmulatedLocal)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess);
    EXPECT_NO_THROW(runCapabilityProbe(*b, "p/.cas_probe"));
}

/// B135: two servers mounting the SAME shared CA pool concurrently must not race on the probe keys.
/// We simulate "a concurrent mounter's probe is in flight" by PRE-SEEDING the fixed-name probe key
/// `<pool>/_probe/token` over a shared backend, then opening the Pool. With the OLD fixed-key probe
/// the open's `putIfAbsent("<pool>/_probe/token", …)` returns PreconditionFailed and `Pool::open`
/// throws NOT_IMPLEMENTED ("putIfAbsent on a fresh key returned PreconditionFailed"). With the
/// per-mount unique probe prefix `<pool>/_probe/<rand>/token`, the seeded key does not collide and
/// the open succeeds — exactly the concurrent-shared-pool-mount behaviour we need.
TEST(CASProbe, ConcurrentMountsDoNotCollide)
{
    auto b = std::make_shared<InMemoryBackend>();

    /// Simulate a concurrent mounter whose probe object under the legacy fixed key is still present.
    ASSERT_EQ(b->putIfAbsent("p/_probe/token", "concurrent-mounter-in-flight").outcome, PutOutcome::Done);

    /// A real (second) mount over the same shared pool must still succeed — its probe runs under a
    /// fresh per-mount-unique prefix and never touches the seeded fixed key.
    EXPECT_NO_THROW(Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}));

    /// And two genuinely-concurrent mounts (distinct unique prefixes) both succeed over one backend.
    EXPECT_NO_THROW(Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}));

    /// The seeded fixed-key artifact is untouched (the probe never collided with it).
    EXPECT_TRUE(b->get("p/_probe/token").has_value());
}

/// The probe must consult the backend's store-preconditions hook BEFORE the op battery: a
/// generation-dialect store on a VERSIONED bucket passes every conditional-op check, but its
/// token-exact DELETEs archive noncurrent generations instead of reclaiming storage — only the
/// hook can see that, so a throwing hook must fail the probe closed.
class PreconditionRefusingBackend : public InMemoryBackend
{
public:
    void checkPoolPreconditions() override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
            "test: store precondition violated (e.g. bucket versioning enabled)");
    }
};

TEST(CASProbe, FailsClosedOnPoolPreconditions)
{
    auto b = std::make_shared<PreconditionRefusingBackend>();
    EXPECT_THROW(runCapabilityProbe(*b, "p/.cas_probe"), DB::Exception);
    /// The hook fires FIRST: no probe keys may have been written.
    EXPECT_TRUE(b->list("p/.cas_probe", "", 10).keys.empty());
}

/// `Pool::open` wraps the pool backend in `InstrumentedBackend` BEFORE calling `runCapabilityProbe`
/// (see CasPool.cpp), so the hook must actually fire THROUGH the wrapper on the real mount path —
/// not just on a raw backend, which `FailsClosedOnPoolPreconditions` above already covers.
TEST(CASProbe, PoolPreconditionsFireThroughInstrumentedWrapper)
{
    auto inner = std::make_shared<PreconditionRefusingBackend>();
    InstrumentedBackend wrapped(inner);
    EXPECT_THROW(runCapabilityProbe(wrapped, "p/.cas_probe"), DB::Exception);
    /// The hook fires FIRST: no probe keys may have been written to the inner backend.
    EXPECT_TRUE(inner->list("p/.cas_probe", "", 10).keys.empty());
}

/// RFC cas-s3-timeout-retry-control: a Native-mode mount over an object storage that does not support
/// the SingleAttempt retry profile must never silently proceed under the disk's default (~500-attempt)
/// transparent retry policy — see Backend::checkConditionalWriteSingleAttemptSupport.
/// LocalObjectStorage never supports the profile (IObjectStorage::supportsRetryProfile's default
/// implementation only answers true for Default), so Native mode over it is exactly the case this must
/// refuse. EmulatedSingleProcess is exempt: it never claims single-attempt S3 semantics in the first
/// place (PassesOnEmulatedLocal above).
TEST(CASProbe, FailsClosedOnUnsupportedSingleAttemptProfile)
{
    auto native = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native);
    EXPECT_THROW(native->checkConditionalWriteSingleAttemptSupport(), DB::Exception);

    auto emulated = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess);
    EXPECT_NO_THROW(emulated->checkConditionalWriteSingleAttemptSupport());
}

/// The same fail-closed refusal through the actual capability probe (Step 0b) — the real gate a
/// writable Pool::open goes through, not just the hook in isolation above.
TEST(CASProbe, MissingSingleAttemptClientFailsCapabilityProbe)
{
    auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();
    /// Native mode passes the key to the object storage verbatim, so the probe prefix must be anchored
    /// under this storage's own root: a bare prefix lands beside the test process, where an object left
    /// by another run answers the LIST below and an unrooted LIST answers "no keys" for free.
    const String probe_prefix = DB::Cas::tests::nativeKeyUnder(storage, "p/.cas_probe");

    auto b = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::Native);
    EXPECT_THROW(runCapabilityProbe(*b, probe_prefix), DB::Exception);
    /// The hook fires before the op battery: no probe keys may have been written.
    EXPECT_TRUE(b->list(probe_prefix, "", 10).keys.empty());

    /// The same LIST can see a key that IS under the prefix — otherwise the emptiness above would be
    /// indistinguishable from a prefix this backend can never enumerate.
    ASSERT_EQ(b->putIfAbsent(probe_prefix + "/token", "probe-v1").outcome, PutOutcome::Done);
    EXPECT_FALSE(b->list(probe_prefix, "", 10).keys.empty());
}

/// Mirrors PoolPreconditionsFireThroughInstrumentedWrapper: the real mount path wraps the backend in
/// InstrumentedBackend BEFORE calling runCapabilityProbe, so this check must fire through it too.
TEST(CASProbe, MissingSingleAttemptClientFiresThroughInstrumentedWrapper)
{
    auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();
    auto inner = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::Native);
    InstrumentedBackend wrapped(inner);
    EXPECT_THROW(runCapabilityProbe(wrapped, DB::Cas::tests::nativeKeyUnder(storage, "p/.cas_probe")), DB::Exception);
}

namespace
{

/// Models the exact shape of the trust-flip this suite must catch a regression of
/// (codex-review-triage §3.18, Critical): like the production `ObjectStorageBackend` in Native mode,
/// this backend mints and expects tokens under a dialect (`TokenType::ETag`) OTHER than
/// `TokenType::Emulated`, and rejects a foreign-dialect `expected`/`token` argument LOCALLY --
/// before the value it carries ever reaches the real conditional-compare beneath the gate (`inner`,
/// a genuinely enforcing `InMemoryBackend`, standing in for "the wire"). Every gated method counts
/// how many times it actually delegated to `inner`, so a test can tell "rejected by the dialect
/// gate" apart from "rejected by the real enforcement" -- the exact distinction `Cas::Probe` exists
/// to prove, and the one the №19 hardening risked collapsing (see CasProbe.cpp step 3/5c/6).
class DialectGatedCountingBackend final : public Backend
{
public:
    std::optional<GetResult> get(const String & key, Range range) override { return inner.get(key, range); }

    std::optional<GetStreamResult> getStream(const String & key, Range range) override { return inner.getStream(key, range); }

    HeadResult head(const String & key) override
    {
        HeadResult r = inner.head(key);
        if (r.exists)
            r.token.type = TokenType::ETag;
        return r;
    }

    bool supportsListTokens() const override { return inner.supportsListTokens(); }

    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        /// No `expected` token to gate -- matches production (ObjectStorageBackend::putIfAbsent has
        /// no dialect check either).
        PutResult r = inner.putIfAbsent(key, bytes, meta);
        if (r.outcome == PutOutcome::Done)
            r.token.type = TokenType::ETag;
        return r;
    }

    WriteSinkPtr putIfAbsentStream(const String & key, const ObjectMeta & meta) override { return inner.putIfAbsentStream(key, meta); }

    PutResult putOverwrite(const String & key, const String & bytes, const Token & expected, const ObjectMeta & meta) override
    {
        if (expected.type != TokenType::ETag)
            return {PutOutcome::PreconditionFailed, {}};   /// dialect-gated: never reaches `inner`
        ++overwrite_reached;
        PutResult r = inner.putOverwrite(key, bytes, Token{expected.value, TokenType::Emulated}, meta);
        if (r.outcome == PutOutcome::Done)
            r.token.type = TokenType::ETag;
        return r;
    }

    CasResult casPut(const String & key, const String & bytes, const std::optional<Token> & expected, const ObjectMeta & meta) override
    {
        if (expected.has_value() && expected->type != TokenType::ETag)
            return {CasOutcome::Conflict, {}};   /// dialect-gated: never reaches `inner`
        ++casput_reached;
        std::optional<Token> retyped;
        if (expected.has_value())
            retyped = Token{expected->value, TokenType::Emulated};
        CasResult r = inner.casPut(key, bytes, retyped, meta);
        if (r.outcome == CasOutcome::Committed)
            r.token.type = TokenType::ETag;
        return r;
    }

    DeleteOutcome deleteExact(const String & key, const Token & token) override
    {
        if (token.type != TokenType::ETag)
        {
            DeleteOutcome d;
            d.kind = DeleteOutcome::Kind::TokenMismatch;   /// dialect-gated: never reaches `inner`
            return d;
        }
        ++delete_reached;
        return inner.deleteExact(key, Token{token.value, TokenType::Emulated});
    }

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        ListPage p = inner.list(prefix, cursor, limit);
        for (auto & k : p.keys)
            if (k.token)
                k.token->type = TokenType::ETag;
        return p;
    }

    /// Number of times putOverwrite/casPut(with expected)/deleteExact actually delegated to `inner`
    /// (i.e. reached the real enforcement) rather than being short-circuited by the dialect gate.
    int overwrite_reached = 0;
    int casput_reached = 0;
    int delete_reached = 0;

private:
    InMemoryBackend inner;
};

}

/// codex-review-triage §3.18, Critical: `runCapabilityProbe`'s three wrong-token sites (step 3
/// putOverwrite, step 5c casPut, step 6 deleteExact) must send a token in the LIVE dialect this
/// backend mints (t1.type / ct1.type / t2.type), not a hardcoded `TokenType::Emulated`. A backend
/// whose native dialect differs from Emulated -- exactly what `ObjectStorageBackend` mints in Native
/// mode -- would otherwise reject the old hardcoded tokens LOCALLY via a dialect gate, never
/// exercising the real conditional enforcement those three steps exist to validate; the probe would
/// still report success (the outcome enums match either way), so a regression here is invisible
/// unless something counts whether the real enforcement was ever reached. `DialectGatedCountingBackend`
/// enforces real (correct) conditional semantics AND gates on dialect exactly like the production
/// risk, so `runCapabilityProbe` runs to completion (unlike a real Native-mode ObjectStorageBackend
/// over LocalObjectStorage, which cannot even reach this point -- see
/// MissingSingleAttemptClientFailsCapabilityProbe and the fact that LocalObjectStorage does not honor
/// WriteSettings conditions at all); the exact reached-counts below pin down that every wrong-token
/// site got past the gate: a probe that regressed to the hardcoded-Emulated construction would still
/// pass (no throw) but under-count here by exactly one at each of the three sites, since the dialect
/// gate would swallow that one call before `inner` ever saw it.
TEST(CASProbe, WrongTokenAttemptsReachTheBackendPastTheDialectGate)
{
    DialectGatedCountingBackend b;
    EXPECT_NO_THROW(runCapabilityProbe(b, "p/.cas_probe"));

    /// putOverwrite: step 3 (wrong token) + step 4 (correct token) -- both live-dialect, both gated
    /// through to `inner`.
    EXPECT_EQ(b.overwrite_reached, 2);
    /// casPut: 5a (create), 5b (conflict-on-exists, no expected token to gate), 5c (wrong token,
    /// live-dialect), 5d (correct token) -- all four reach `inner`.
    EXPECT_EQ(b.casput_reached, 4);
    /// deleteExact: step 6 (wrong token, live-dialect) + step 8 (correct token) + step 9 cleanup
    /// (correct token for cas_key) -- all three reach `inner`.
    EXPECT_EQ(b.delete_reached, 3);
}
