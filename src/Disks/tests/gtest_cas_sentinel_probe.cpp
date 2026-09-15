#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasFence.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInstrumentedBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasSentinelProbe.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/WriteMode.h>
#include <Disks/tests/cas_test_helpers.h>
#include <IO/WriteHelpers.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <unistd.h>

#if USE_AWS_S3
#include <IO/S3Common.h>
#endif

using namespace DB::Cas;

/// Task 3 (spec §2): the typed sentinel probe below `Backend` must never conflate a transport error
/// with absence. These tests exercise the free-function entry point `probeSentinel` against the generic
/// `Backend::probeSentinelRaw` default (via `InMemoryBackend`, the "Emulated"-style in-memory backend used
/// by CAS tests) and against `ObjectStorageBackend`'s `EmulatedSingleProcess` override, which is the REAL
/// production mode for a content-addressed disk over `object_storage_type=local`.

namespace
{

using DB::Cas::tests::nativeKeyUnder;

/// Every test here constructs one backend and probes it once or a few times; a non-owning `BackendPtr`
/// over the test's stack-allocated backend keeps that construction pattern rather than forcing every
/// fixture in this file onto `std::make_shared`. The open fence never trips, matching every prior call
/// here having had no fence to enforce. `clock`, when given, drives `probeSentinel`'s reissue-on-
/// `Indeterminate` loop off an injected clock instead of a real sleep — needed by the one test whose
/// fault never resolves, so the loop runs its whole policy window without taking real wall-clock time.
CasRequests makeRequests(Backend & backend, DB::Cas::tests::FakeClock * clock = nullptr)
{
    BackendPtr ptr(&backend, [](Backend *) {});
    if (clock)
        return CasRequests(std::move(ptr), Fence::open(), clock->nowFn(), clock->sleepFn());
    return CasRequests(std::move(ptr), Fence::open());
}

/// A Backend decorator whose read/head/list all throw an untyped runtime error when armed — modelling
/// a backend with no sharper evidence than "something went wrong" (a network timeout, a 5xx, an
/// unclassifiable failure). The fault is injected on the PRIMITIVES, which is what
/// `Backend::probeSentinelRaw`'s default derives its answer from; every other operation delegates to
/// InMemoryBackend unchanged.
class TransportFaultBackend final : public InMemoryBackend
{
public:
    std::optional<RawMeta> head(const String & key, TransportAccess & access) override
    {
        if (fail.load())
            throw std::runtime_error("injected fault: transport error");
        return InMemoryBackend::head(key, access);
    }

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (fail.load())
            throw std::runtime_error("injected fault: transport error");
        return InMemoryBackend::read(key, access);
    }

    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
    {
        if (fail.load())
            throw std::runtime_error("injected fault: transport error");
        return InMemoryBackend::list(prefix, cursor, limit, access);
    }

    std::atomic<bool> fail{true};
};

}

/// The probe loop's own attempt counter reaches the transport too -- propagation only, the probe
/// keeps its ordinary backoff.
TEST(CASSentinelProbe, AttemptNumberPropagates)
{
    struct ProbeRecording : InMemoryBackend
    {
        std::vector<size_t> attempts;
        SentinelProbeResult probeSentinelRaw(const String & key, TransportAccess & access) override
        {
            attempts.push_back(access.attemptNo());
            if (attempts.size() == 1)
                return {ProbeOutcome::Indeterminate, std::nullopt};
            return InMemoryBackend::probeSentinelRaw(key, access);
        }
    };
    DB::Cas::tests::FakeClock clock;
    auto backend = std::make_shared<ProbeRecording>();
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn());
    auto op = requests.admit();
    (void)op.probeSentinel("probe", Retry::standard());
    EXPECT_EQ(backend->attempts, (std::vector<size_t>{1, 2}));
    EXPECT_EQ(clock.sleeps.size(), 1u);   /// propagation only: the probe keeps its ordinary backoff
}

/// (a) A present key probes Present and carries the materialized body.
TEST(CASSentinelProbe, PresentKeyReturnsPresentWithBody)
{
    InMemoryBackend backend;
    auto requests = makeRequests(backend);
    auto op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("k", "hello", Retry::once())));

    const auto result = probeSentinel(op, "k", Retry::standard());
    EXPECT_EQ(result.outcome, ProbeOutcome::Present);
    ASSERT_TRUE(result.body.has_value());
    EXPECT_EQ(*result.body, "hello");
}

/// (b) A deleted (never-written) key probes KeyAbsent while the container/backend is otherwise alive.
TEST(CASSentinelProbe, AbsentKeyWithContainerAliveReturnsKeyAbsent)
{
    InMemoryBackend backend;
    auto requests = makeRequests(backend);
    auto op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("other", "x", Retry::once())));   // proves the backend is alive

    const auto result = probeSentinel(op, "missing", Retry::standard());
    EXPECT_EQ(result.outcome, ProbeOutcome::KeyAbsent);
    EXPECT_FALSE(result.body.has_value());
}

/// (c) `ObjectStorageBackend::EmulatedSingleProcess` is the REAL production backend for a
/// content-addressed disk over `object_storage_type=local` (ContentAddressedMetadataStorage.cpp
/// selects it whenever the underlying storage is Local). Removing the WHOLE configured container
/// directory (the disk root) must probe `ContainerAbsent`, distinct from an ordinary absent key —
/// `LocalObjectStorage::listObjects` silently reports zero children for BOTH a missing directory and
/// an empty one, so the distinction only exists because `probeSentinelRaw` stats the container first.
TEST(CASSentinelProbe, ContainerDirectoryRemovedReturnsContainerAbsent)
{
    auto storage = tests::makeLocalObjectStorageForTest();
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::EmulatedSingleProcess);

    auto requests = makeRequests(backend);
    auto op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("k", "hello", Retry::once())));

    /// Sanity, container alive: Present vs. KeyAbsent are genuinely distinct before we remove anything.
    EXPECT_EQ(probeSentinel(op, "k", Retry::standard()).outcome, ProbeOutcome::Present);
    EXPECT_EQ(probeSentinel(op, "missing", Retry::standard()).outcome, ProbeOutcome::KeyAbsent);

    std::filesystem::remove_all(storage->getCommonKeyPrefix());

    const auto result = probeSentinel(op, "k", Retry::standard());
    EXPECT_EQ(result.outcome, ProbeOutcome::ContainerAbsent);
    EXPECT_FALSE(result.body.has_value());
}

/// `ObjectStorageBackend::Mode::Native` over a plain `LocalObjectStorage` (the same construction
/// `gtest_cas_backend.cpp`'s Native-mode tests use to exercise the Native code path without a live S3
/// endpoint): a present key must probe `Present` and carry the materialized body via the raw-HEAD ->
/// `get` path, not just the EmulatedSingleProcess path already covered above.
TEST(CASSentinelProbe, NativePresentKeyReturnsPresentWithBody)
{
    auto storage = tests::makeLocalObjectStorageForTest();
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);
    const String key = nativeKeyUnder(storage, "some/key");

    /// Placed through the object storage: a Native write over a local storage has no response
    /// incarnation to attribute itself to. Native passes the key verbatim, so this is the object the
    /// probe reads.
    {
        auto out = storage->writeObject(DB::StoredObject(key), DB::WriteMode::Rewrite);
        DB::writeString(String("native body"), *out);
        out->finalize();
    }

    auto requests = makeRequests(backend);
    auto op = requests.admit();
    const auto result = probeSentinel(op, key, Retry::standard());
    EXPECT_EQ(result.outcome, ProbeOutcome::Present);
    ASSERT_TRUE(result.body.has_value());
    EXPECT_EQ(*result.body, "native body");
}

/// (d) A backend forced to throw a transport error must probe Indeterminate — NEVER KeyAbsent, even
/// though the failure looks superficially like "nothing there" from the caller's point of view.
TEST(CASSentinelProbe, TransportErrorNeverClassifiesAsAbsent)
{
    TransportFaultBackend backend;
    /// The fault never resolves, so `probeSentinel`'s reissue-on-`Indeterminate` loop runs to its whole
    /// policy window before giving up; an injected clock keeps that instantaneous instead of real time.
    DB::Cas::tests::FakeClock clock;
    auto requests = makeRequests(backend, &clock);
    auto op = requests.admit();
    const auto result = probeSentinel(op, "k", Retry::standard());
    EXPECT_EQ(result.outcome, ProbeOutcome::Indeterminate);
    EXPECT_FALSE(result.body.has_value());
    EXPECT_GT(clock.sleeps.size(), 1u)
        << "a single attempt would not distinguish this reissue loop from a non-retrying policy that "
           "reaches the same Indeterminate give-up on its first try";
}

#if USE_AWS_S3

namespace
{

/// A `LocalObjectStorage` whose object ACCESS can be armed to throw a configurable synthetic
/// `S3Exception` — the same technique `gtest_cas_backend.cpp`'s `NativeReadThrowsNoSuchKeyObjectStorage`
/// uses to exercise S3 error codes without a live S3 endpoint. Constructing `ObjectStorageBackend` in
/// `Mode::Native` over this fake is the established pattern for testing the Native/S3 raw-error classifier
/// in isolation (see also `gtest_cas_backend.cpp`'s `NativeRejectsWrongDialectTokenBeforeTouchingTheWire`).
/// Both the read and the metadata surface throw: the Native sentinel probe issues one READ, and a
/// store that answers an error for a key answers it however the key is touched.
class ThrowingS3ObjectStorage final : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    void throwOnObjectAccess(Aws::S3::S3Errors code) { access_error = code; }

    std::unique_ptr<DB::ReadBufferFromFileBase> readObject(
        const DB::StoredObject & object,
        const DB::ReadSettings & read_settings,
        std::optional<size_t> read_hint,
        bool use_external_buffer,
        bool restrict_seek) const override
    {
        if (access_error)
            throw DB::S3Exception("injected fault: " + object.remote_path, *access_error);
        return DB::LocalObjectStorage::readObject(object, read_settings, read_hint, use_external_buffer, restrict_seek);
    }

    DB::ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override
    {
        if (access_error)
            throw DB::S3Exception("injected fault: " + path, *access_error);
        return DB::LocalObjectStorage::getObjectMetadata(path, with_tags);
    }

private:
    std::optional<Aws::S3::S3Errors> access_error;
};

DB::ObjectStoragePtr makeThrowingS3StorageForTest()
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_sentinel_probe_unit_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<ThrowingS3ObjectStorage>(std::move(settings));
}

}

/// The full S3 IAM permutation table (spec §2): a raw NO_SUCH_KEY/NO_SUCH_BUCKET/ACCESS_DENIED HEAD
/// error must classify EXACTLY, and anything unmodeled must fail closed to Indeterminate.
TEST(CASSentinelProbe, NativeClassifiesNoSuchKeyAsKeyAbsent)
{
    auto storage = std::static_pointer_cast<ThrowingS3ObjectStorage>(makeThrowingS3StorageForTest());
    storage->throwOnObjectAccess(Aws::S3::S3Errors::NO_SUCH_KEY);
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);

    auto requests = makeRequests(backend);
    auto op = requests.admit();
    EXPECT_EQ(probeSentinel(op, nativeKeyUnder(storage, "some/key"), Retry::standard()).outcome, ProbeOutcome::KeyAbsent);
}

/// A real S3 HEAD's 404 has no response body, so the SDK cannot parse a `NoSuchKey` `<Code>` and
/// instead derives `RESOURCE_NOT_FOUND` straight from the HTTP status (see `isNotFoundError`,
/// `src/IO/S3/getObjectInfo.cpp`) — THIS is the code a genuinely absent key throws on real S3, not
/// `NO_SUCH_KEY`. Without classifying it, every real-S3 absence would be `Indeterminate` forever.
TEST(CASSentinelProbe, NativeClassifiesResourceNotFoundAsKeyAbsent)
{
    auto storage = std::static_pointer_cast<ThrowingS3ObjectStorage>(makeThrowingS3StorageForTest());
    storage->throwOnObjectAccess(Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);

    auto requests = makeRequests(backend);
    auto op = requests.admit();
    EXPECT_EQ(probeSentinel(op, nativeKeyUnder(storage, "some/key"), Retry::standard()).outcome, ProbeOutcome::KeyAbsent);
}

TEST(CASSentinelProbe, NativeClassifiesNoSuchBucketAsContainerAbsent)
{
    auto storage = std::static_pointer_cast<ThrowingS3ObjectStorage>(makeThrowingS3StorageForTest());
    storage->throwOnObjectAccess(Aws::S3::S3Errors::NO_SUCH_BUCKET);
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);

    auto requests = makeRequests(backend);
    auto op = requests.admit();
    EXPECT_EQ(probeSentinel(op, nativeKeyUnder(storage, "some/key"), Retry::standard()).outcome, ProbeOutcome::ContainerAbsent);
}

TEST(CASSentinelProbe, NativeClassifiesAccessDeniedAsAccessDenied)
{
    auto storage = std::static_pointer_cast<ThrowingS3ObjectStorage>(makeThrowingS3StorageForTest());
    storage->throwOnObjectAccess(Aws::S3::S3Errors::ACCESS_DENIED);
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);

    auto requests = makeRequests(backend);
    auto op = requests.admit();
    EXPECT_EQ(probeSentinel(op, nativeKeyUnder(storage, "some/key"), Retry::standard()).outcome, ProbeOutcome::AccessDenied);
}

TEST(CASSentinelProbe, NativeClassifiesUnmodeledErrorAsIndeterminate)
{
    auto storage = std::static_pointer_cast<ThrowingS3ObjectStorage>(makeThrowingS3StorageForTest());
    storage->throwOnObjectAccess(Aws::S3::S3Errors::SERVICE_UNAVAILABLE);
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);

    /// Every attempt classifies Indeterminate here too, so the reissue loop runs its whole policy
    /// window; an injected clock keeps that instantaneous instead of real time.
    DB::Cas::tests::FakeClock clock;
    auto requests = makeRequests(backend, &clock);
    auto op = requests.admit();
    EXPECT_EQ(probeSentinel(op, nativeKeyUnder(storage, "some/key"), Retry::standard()).outcome, ProbeOutcome::Indeterminate);
    EXPECT_GT(clock.sleeps.size(), 1u)
        << "a single attempt would not distinguish this reissue loop from a non-retrying policy that "
           "reaches the same Indeterminate give-up on its first try";
}

/// Production wiring (`Pool::open`) ALWAYS wraps the real backend in `InstrumentedBackend` before
/// anything calls it. `InstrumentedBackend` must forward `probeSentinelRaw` to `inner`, not fall
/// through to `Backend::probeSentinelRaw`'s generic head/get-based default — the default would derive
/// its answer from THIS object's own (correctly delegating, but non-typed) `head`/`get` overrides,
/// silently discarding `ObjectStorageBackend`'s real S3-error classification. NO_SUCH_BUCKET is chosen
/// deliberately: the generic default cannot produce `ContainerAbsent` at all (it only ever returns
/// Present/KeyAbsent/Indeterminate), so this test can ONLY pass if the typed override is actually
/// reached through the wrapper.
TEST(CASSentinelProbe, InstrumentedBackendForwardsToInnerClassification)
{
    auto storage = std::static_pointer_cast<ThrowingS3ObjectStorage>(makeThrowingS3StorageForTest());
    storage->throwOnObjectAccess(Aws::S3::S3Errors::NO_SUCH_BUCKET);
    auto inner = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::Native);
    InstrumentedBackend instrumented(inner);

    auto requests = makeRequests(instrumented);
    auto op = requests.admit();
    EXPECT_EQ(probeSentinel(op, nativeKeyUnder(storage, "some/key"), Retry::standard()).outcome, ProbeOutcome::ContainerAbsent);
}

#endif
