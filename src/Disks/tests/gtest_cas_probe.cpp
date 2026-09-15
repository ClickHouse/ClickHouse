#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasFence.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasProbe.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

using namespace DB::Cas;

namespace
{

/// Every test here constructs one backend and runs the battery against it once; a non-owning
/// `BackendPtr` over the test's stack- or shared_ptr-held backend keeps that construction pattern
/// rather than forcing a second allocation. The open fence never trips: `runCapabilityProbe` runs
/// during pool bootstrap, before any mount fence exists to enforce.
CasRequests makeRequests(Backend & backend)
{
    return CasRequests(BackendPtr(&backend, [](Backend *) {}), Fence::open());
}

}

TEST(CASProbe, PassesOnEnforcingBackend)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto requests = makeRequests(*b);
    auto op = requests.admit();
    EXPECT_NO_THROW(runCapabilityProbe(op, "p/.cas_probe"));
    EXPECT_TRUE(op.list("p/.cas_probe", "", 10, Retry::once()).keys.empty());   // probe cleans up after itself
}

TEST(CASProbe, FailsClosedOnNonEnforcingDelete)
{
    auto b = std::make_shared<InMemoryBackend>();
    b->setEnforceTokens(false);                                  // the MinIO-OSS failure mode
    auto requests = makeRequests(*b);
    auto op = requests.admit();
    EXPECT_THROW(runCapabilityProbe(op, "p/.cas_probe"), DB::Exception);
}

TEST(CASProbe, FailsClosedOnDeleteMarkers)
{
    auto b = std::make_shared<InMemoryBackend>();
    b->setSimulateDeleteMarkers(true);                           // versioning enabled on the prefix
    auto requests = makeRequests(*b);
    auto op = requests.admit();
    EXPECT_THROW(runCapabilityProbe(op, "p/.cas_probe"), DB::Exception);
}

TEST(CASProbe, PassesOnEmulatedLocal)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::EmulatedSingleProcess);
    auto requests = makeRequests(*b);
    auto op = requests.admit();
    EXPECT_NO_THROW(runCapabilityProbe(op, "p/.cas_probe"));
}

/// Two servers mounting the SAME shared CA pool concurrently must not race on the probe keys.
/// We simulate "a concurrent mounter's probe is in flight" by PRE-SEEDING the fixed-name probe key
/// `<pool>/_probe/token` over a shared backend, then opening the Pool. A fixed-key probe would meet
/// the seeded object as a refused precondition on its own `create` and fail the open; with the
/// per-mount unique probe prefix `<pool>/_probe/<rand>/token`, the seeded key does not collide and
/// the open succeeds — exactly the concurrent-shared-pool-mount behaviour we need.
///
/// Goes through `Pool::open` (owned elsewhere), so it exercises `runCapabilityProbe` only indirectly
/// and needs no signature change here.
TEST(CASProbe, ConcurrentMountsDoNotCollide)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto probe_requests = makeRequests(*b);
    auto probe_op = probe_requests.admit();

    /// Simulate a concurrent mounter whose probe object under the legacy fixed key is still present.
    ASSERT_TRUE(std::holds_alternative<Committed>(
        probe_op.create("p/_probe/token", "concurrent-mounter-in-flight", Retry::once())));

    /// A real (second) mount over the same shared pool must still succeed — its probe runs under a
    /// fresh per-mount-unique prefix and never touches the seeded fixed key.
    EXPECT_NO_THROW(Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}));

    /// And two genuinely-concurrent mounts (distinct unique prefixes) both succeed over one backend.
    EXPECT_NO_THROW(Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}));

    /// The seeded fixed-key artifact is untouched (the probe never collided with it).
    EXPECT_TRUE(probe_op.read("p/_probe/token", Retry::once()).has_value());
}

/// RFC cas-s3-timeout-retry-control: a Native-mode mount over an object storage that does not support
/// the SingleAttempt retry profile must never silently proceed under the disk's default (~500-attempt)
/// transparent retry policy — see Backend::checkConditionalWriteSingleAttemptSupport. This calls the
/// hook directly on the backend (not through `runCapabilityProbe`, which no longer runs it — see
/// CasProbe.h), so it is unaffected by the request-contract migration.
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

namespace
{

/// Honors every conditional WRITE but ignores the precondition on a conditional REMOVE. This is what a
/// GCS delete degenerates to when its numeric generation leaves as a raw `If-Match` — no
/// `x-goog-if-generation-match` — and the service ignores the header it does not recognise. Gated on
/// the PRIMITIVE (`Backend::remove`), which is what `CasOperation::remove` actually calls; a fault
/// injected on the legacy `deleteExact` forwarder would no longer intercept anything.
class IgnoresDeleteTokenBackend : public InMemoryBackend
{
public:
    RawRemoval remove(const String & key, const String & /*expected_value*/, TransportAccess & access) override
    {
        const auto meta = InMemoryBackend::head(key, access);
        if (!meta)
            return RawRemoval::Gone;
        return InMemoryBackend::remove(key, meta->value, access);
    }
};

/// The other half of that degeneracy: the service refuses the unrecognised header outright, so even
/// the correct incarnation never removes anything.
class RejectsDeleteTokenBackend : public InMemoryBackend
{
public:
    RawRemoval remove(const String &, const String &, TransportAccess &) override
    {
        return RawRemoval::Mismatch;
    }
};

}

/// A GCS mount whose exact deletes lost their generation semantics can fail in either direction, and
/// the probe's delete battery must reject the mount both times. Both backends enforce every
/// conditional write, so every step before the battery's delete checks passes and only the
/// stale-incarnation-preserved check or the correct-incarnation-removed check can be what fires —
/// `PassesOnEnforcingBackend` above is the control showing the same probe succeeds when only `remove`
/// is left alone.
///
/// This is about the battery, not about the marking: that the `NativeConditional` mode actually
/// reaches the production request object is proven where the request is built, not here.
TEST(CASProbe, ExactDeleteBatteryDetectsMissingGenerationMode)
{
    IgnoresDeleteTokenBackend ignores;
    auto ignores_requests = makeRequests(ignores);
    auto ignores_op = ignores_requests.admit();
    EXPECT_THROW(runCapabilityProbe(ignores_op, "p/.cas_probe"), DB::Exception);

    RejectsDeleteTokenBackend rejects;
    auto rejects_requests = makeRequests(rejects);
    auto rejects_op = rejects_requests.admit();
    EXPECT_THROW(runCapabilityProbe(rejects_op, "p/.cas_probe"), DB::Exception);
}

namespace
{

/// Rejects, LOCALLY and without touching the store, any conditional write/remove whose precondition
/// value is not grammar-valid under the claimed dialect — the production shape of the retired
/// `DialectGatedCountingBackend`, ported to the primitive interface (`write`/`remove` carry a raw
/// precondition VALUE now, not a typed token, so the gate is `isIncarnationValue` rather than a type-tag
/// compare). `write_reached`/`remove_reached` count only the calls that got PAST the gate, so a
/// regression that reintroduces a synthesized (grammar-invalid-somewhere) precondition drops one of
/// these counts instead of passing silently.
class DialectOverrideBackend : public InMemoryBackend
{
public:
    explicit DialectOverrideBackend(Dialect claimed_dialect_) : claimed_dialect(claimed_dialect_) {}
    Dialect dialect() const override { return claimed_dialect; }

    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value, TransportAccess & access) override
    {
        if (expected_value && !isIncarnationValue(claimed_dialect, *expected_value))
            return std::unexpected(RawConflict{});   /// dialect-gated: never reaches the real store
        ++write_reached;
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }

    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        if (!isIncarnationValue(claimed_dialect, expected_value))
            return RawRemoval::Mismatch;   /// dialect-gated: never reaches the real store
        ++remove_reached;
        return InMemoryBackend::remove(key, expected_value, access);
    }

    int write_reached = 0;
    int remove_reached = 0;

private:
    Dialect claimed_dialect;
};

}

/// The probe's reordered "wrong incarnation" steps always reuse a REAL, backend-minted `Etag`
/// from the same key rather than a synthesized value — see CasProbe.cpp's step comments — and every
/// such value is grammar-valid under every dialect by construction (`InMemoryBackend`'s minted values are
/// a monotonically increasing decimal starting at "1": non-empty and comma/`*`-free for ETag, a canonical
/// positive decimal for Generation, merely non-empty for Emulated). So under EVERY dialect the battery's
/// four conditional writes (steps 1-4) and two conditional removes (steps 5, 7) must all reach the real
/// store — asserting the exact counts is what makes this test able to fail: a regression that
/// reintroduces a synthesized, foreign-dialect precondition would get gated locally on at least one
/// dialect, dropping one of these counts below the total instead of merely changing an outcome enum.
TEST(CASProbe, ReorderedProbePassesOnAllThreeDialects)
{
    for (const Dialect dialect : {Dialect::ETag, Dialect::Generation, Dialect::Emulated})
    {
        DialectOverrideBackend b(dialect);
        auto requests = makeRequests(b);
        auto op = requests.admit();
        EXPECT_NO_THROW(runCapabilityProbe(op, "p/.cas_probe")) << "dialect " << static_cast<int>(dialect);
        EXPECT_TRUE(op.list("p/.cas_probe", "", 10, Retry::once()).keys.empty()) << "dialect " << static_cast<int>(dialect);
        EXPECT_EQ(b.write_reached, 4) << "dialect " << static_cast<int>(dialect);
        EXPECT_EQ(b.remove_reached, 2) << "dialect " << static_cast<int>(dialect);
    }
}
