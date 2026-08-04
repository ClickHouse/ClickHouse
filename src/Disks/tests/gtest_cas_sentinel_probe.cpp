#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInstrumentedBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasSentinelProbe.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/tests/cas_test_helpers.h>

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

/// A Backend decorator whose head/get/list all throw an untyped runtime error when armed — modelling
/// a backend with no sharper evidence than "something went wrong" (a network timeout, a 5xx, an
/// unclassifiable failure). Mirrors the existing MetaWriteFaultBackend fault-injection pattern
/// (cas_test_helpers.h): every other operation delegates to InMemoryBackend unchanged.
class TransportFaultBackend final : public InMemoryBackend
{
public:
    /// Unhide the base convenience overloads, matching every other Backend subclass in this suite.
    using Backend::get;
    using Backend::getStream;
    using Backend::putIfAbsent;
    using Backend::putIfAbsentStream;
    using Backend::putOverwrite;
    using Backend::casPut;

    HeadResult head(const String & key) override
    {
        if (fail.load())
            throw std::runtime_error("injected fault: transport error");
        return InMemoryBackend::head(key);
    }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        if (fail.load())
            throw std::runtime_error("injected fault: transport error");
        return InMemoryBackend::get(key, range);
    }

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        if (fail.load())
            throw std::runtime_error("injected fault: transport error");
        return InMemoryBackend::list(prefix, cursor, limit);
    }

    std::atomic<bool> fail{true};
};

}

/// (a) A present key probes Present and carries the materialized body.
TEST(CASSentinelProbe, PresentKeyReturnsPresentWithBody)
{
    InMemoryBackend backend;
    ASSERT_EQ(backend.putIfAbsent("k", "hello").outcome, PutOutcome::Done);

    const auto result = probeSentinel(backend, "k");
    EXPECT_EQ(result.outcome, ProbeOutcome::Present);
    ASSERT_TRUE(result.body.has_value());
    EXPECT_EQ(*result.body, "hello");
}

/// (b) A deleted (never-written) key probes KeyAbsent while the container/backend is otherwise alive.
TEST(CASSentinelProbe, AbsentKeyWithContainerAliveReturnsKeyAbsent)
{
    InMemoryBackend backend;
    ASSERT_EQ(backend.putIfAbsent("other", "x").outcome, PutOutcome::Done);   // proves the backend is alive

    const auto result = probeSentinel(backend, "missing");
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

    ASSERT_EQ(backend.putIfAbsent("k", "hello").outcome, PutOutcome::Done);

    /// Sanity, container alive: Present vs. KeyAbsent are genuinely distinct before we remove anything.
    EXPECT_EQ(probeSentinel(backend, "k").outcome, ProbeOutcome::Present);
    EXPECT_EQ(probeSentinel(backend, "missing").outcome, ProbeOutcome::KeyAbsent);

    std::filesystem::remove_all(storage->getCommonKeyPrefix());

    const auto result = probeSentinel(backend, "k");
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

    ASSERT_EQ(backend.putIfAbsent(key, "native body").outcome, PutOutcome::Done);

    const auto result = probeSentinel(backend, key);
    EXPECT_EQ(result.outcome, ProbeOutcome::Present);
    ASSERT_TRUE(result.body.has_value());
    EXPECT_EQ(*result.body, "native body");
}

/// (d) A backend forced to throw a transport error must probe Indeterminate — NEVER KeyAbsent, even
/// though the failure looks superficially like "nothing there" from the caller's point of view.
TEST(CASSentinelProbe, TransportErrorNeverClassifiesAsAbsent)
{
    TransportFaultBackend backend;
    const auto result = probeSentinel(backend, "k");
    EXPECT_EQ(result.outcome, ProbeOutcome::Indeterminate);
    EXPECT_FALSE(result.body.has_value());
}

#if USE_AWS_S3

namespace
{

/// A `LocalObjectStorage` whose `getObjectMetadata` can be armed to throw a configurable synthetic
/// `S3Exception` — the same technique `gtest_cas_backend.cpp`'s `NativeReadThrowsNoSuchKeyObjectStorage`
/// uses to exercise S3 error codes without a live S3 endpoint. Constructing `ObjectStorageBackend` in
/// `Mode::Native` over this fake is the established pattern for testing the Native/S3 raw-error classifier
/// in isolation (see also `gtest_cas_backend.cpp`'s `NativeRejectsWrongDialectTokenBeforeTouchingTheWire`).
class ThrowingS3MetadataObjectStorage final : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    void throwOnGetObjectMetadata(Aws::S3::S3Errors code) { metadata_error = code; }

    DB::ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override
    {
        if (metadata_error)
            throw DB::S3Exception("injected fault: " + path, *metadata_error);
        return DB::LocalObjectStorage::getObjectMetadata(path, with_tags);
    }

private:
    std::optional<Aws::S3::S3Errors> metadata_error;
};

DB::ObjectStoragePtr makeThrowingS3MetadataStorageForTest()
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_sentinel_probe_unit_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<ThrowingS3MetadataObjectStorage>(std::move(settings));
}

}

/// The full S3 IAM permutation table (spec §2): a raw NO_SUCH_KEY/NO_SUCH_BUCKET/ACCESS_DENIED HEAD
/// error must classify EXACTLY, and anything unmodeled must fail closed to Indeterminate.
TEST(CASSentinelProbe, NativeClassifiesNoSuchKeyAsKeyAbsent)
{
    auto storage = std::static_pointer_cast<ThrowingS3MetadataObjectStorage>(makeThrowingS3MetadataStorageForTest());
    storage->throwOnGetObjectMetadata(Aws::S3::S3Errors::NO_SUCH_KEY);
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);

    EXPECT_EQ(probeSentinel(backend, nativeKeyUnder(storage, "some/key")).outcome, ProbeOutcome::KeyAbsent);
}

/// A real S3 HEAD's 404 has no response body, so the SDK cannot parse a `NoSuchKey` `<Code>` and
/// instead derives `RESOURCE_NOT_FOUND` straight from the HTTP status (see `isNotFoundError`,
/// `src/IO/S3/getObjectInfo.cpp`) — THIS is the code a genuinely absent key throws on real S3, not
/// `NO_SUCH_KEY`. Without classifying it, every real-S3 absence would be `Indeterminate` forever.
TEST(CASSentinelProbe, NativeClassifiesResourceNotFoundAsKeyAbsent)
{
    auto storage = std::static_pointer_cast<ThrowingS3MetadataObjectStorage>(makeThrowingS3MetadataStorageForTest());
    storage->throwOnGetObjectMetadata(Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);

    EXPECT_EQ(probeSentinel(backend, nativeKeyUnder(storage, "some/key")).outcome, ProbeOutcome::KeyAbsent);
}

TEST(CASSentinelProbe, NativeClassifiesNoSuchBucketAsContainerAbsent)
{
    auto storage = std::static_pointer_cast<ThrowingS3MetadataObjectStorage>(makeThrowingS3MetadataStorageForTest());
    storage->throwOnGetObjectMetadata(Aws::S3::S3Errors::NO_SUCH_BUCKET);
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);

    EXPECT_EQ(probeSentinel(backend, nativeKeyUnder(storage, "some/key")).outcome, ProbeOutcome::ContainerAbsent);
}

TEST(CASSentinelProbe, NativeClassifiesAccessDeniedAsAccessDenied)
{
    auto storage = std::static_pointer_cast<ThrowingS3MetadataObjectStorage>(makeThrowingS3MetadataStorageForTest());
    storage->throwOnGetObjectMetadata(Aws::S3::S3Errors::ACCESS_DENIED);
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);

    EXPECT_EQ(probeSentinel(backend, nativeKeyUnder(storage, "some/key")).outcome, ProbeOutcome::AccessDenied);
}

TEST(CASSentinelProbe, NativeClassifiesUnmodeledErrorAsIndeterminate)
{
    auto storage = std::static_pointer_cast<ThrowingS3MetadataObjectStorage>(makeThrowingS3MetadataStorageForTest());
    storage->throwOnGetObjectMetadata(Aws::S3::S3Errors::SERVICE_UNAVAILABLE);
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);

    EXPECT_EQ(probeSentinel(backend, nativeKeyUnder(storage, "some/key")).outcome, ProbeOutcome::Indeterminate);
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
    auto storage = std::static_pointer_cast<ThrowingS3MetadataObjectStorage>(makeThrowingS3MetadataStorageForTest());
    storage->throwOnGetObjectMetadata(Aws::S3::S3Errors::NO_SUCH_BUCKET);
    auto inner = std::make_shared<ObjectStorageBackend>(storage, ObjectStorageBackend::Mode::Native);
    InstrumentedBackend instrumented(inner);

    EXPECT_EQ(probeSentinel(instrumented, nativeKeyUnder(storage, "some/key")).outcome, ProbeOutcome::ContainerAbsent);
}

#endif
