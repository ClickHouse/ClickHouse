#include <gtest/gtest.h>
#include "cas_test_helpers.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountRuntime.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>

#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int NETWORK_ERROR;
}

/// Task 4 (spec §1 "Gate lifetime [C2]"): every durable-effect path on the plain-object surface
/// (`CasPlainObjects::casPutObject`/`casRemoveObject`), the S3-native staging-buffer finalize
/// (`Cas::CaContentWriteBuffer`), and the part-write condemned-displacement raw writes capture the mount
/// runtime's fence generation at admission and re-check it -- and `mayMutate()` -- immediately before their
/// durable backend call, throwing the typed transient error (`NETWORK_ERROR` -- the upstream-retryable
/// class every CA write-plane transient uses) on a mismatch instead of letting a stale-incarnation write
/// land.
///
/// These tests drive a real `Cas::Pool` over `InMemoryBackend` (the "Emulated"-style in-memory
/// backend) via `Pool::open`, exactly like `gtest_cas_mount.cpp`/`gtest_cas_s3_staging.cpp` -- the
/// fence is tripped/observed through `Pool`'s public forwarders (`tripMountLost`, `mayMutate`,
/// `fenceGeneration`, `checkFenceOrThrow`).

using namespace DB::Cas;

namespace
{

/// A backend whose `head()` call can trigger an injected side-effect exactly once -- deterministically
/// simulates a fence trip landing BETWEEN a durable-effect operation's admission and its durable
/// backend call, with no real concurrency at all (mirrors the injected-fault shape of
/// `TransportFaultBackend` in gtest_cas_sentinel_probe.cpp, but fires a callback instead of throwing).
class TripOnHeadBackend final : public InMemoryBackend
{
public:
    HeadResult head(const String & key) override
    {
        if (trigger)
            std::exchange(trigger, {})();
        return InMemoryBackend::head(key);
    }

    std::function<void()> trigger;
};

/// Same idea as `TripOnHeadBackend`, but fires on the SECOND `head()` call and forces a first-attempt
/// `PreconditionFailed` so the retry loop actually reaches a second iteration -- proves the fence
/// re-check runs on EVERY conditional-retry iteration, not just the admission-time first attempt.
class TripOnSecondHeadBackend final : public InMemoryBackend
{
public:
    using Backend::putIfAbsent;

    HeadResult head(const String & key) override
    {
        ++head_calls;
        if (head_calls == 2 && trigger)
            std::exchange(trigger, {})();
        return InMemoryBackend::head(key);
    }

    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        if (fail_first_put)
        {
            fail_first_put = false;
            return PutResult{.outcome = PutOutcome::PreconditionFailed, .token = {}};
        }
        return InMemoryBackend::putIfAbsent(key, bytes, meta);
    }

    int head_calls = 0;
    /// Default false: `Pool::open`'s own capability probe issues `putIfAbsent` calls before the test
    /// gets to arm this, and those must succeed normally. The test flips this to `true` only right
    /// before driving the write it actually targets.
    bool fail_first_put = false;
    std::function<void()> trigger;
};

/// A minimal in-memory `WriteBufferFromFileBase` standing in for an object-store sink, trimmed to just
/// what these tests observe (whether `finalizeImpl` ran) -- mirrors `FakeStagingSink` in
/// gtest_cas_s3_staging.cpp (not reusable from here: that one lives in that file's own anonymous
/// namespace).
class RecordingSink final : public DB::WriteBufferFromFileBase
{
public:
    explicit RecordingSink(std::string key_)
        : DB::WriteBufferFromFileBase(/*buf_size=*/8192, nullptr, 0), key(std::move(key_))
    {
    }

    void sync() override {}
    std::string getFileName() const override { return key; }
    bool wasFinalizedForTest() const { return did_finalize; }

protected:
    void nextImpl() override
    {
        if (offset())
            written.append(working_buffer.begin(), offset());
    }

    void finalizeImpl() override
    {
        next();
        did_finalize = true;
    }

    void cancelImpl() noexcept override { cancelled = true; }

private:
    std::string key;
    std::string written;
    bool did_finalize = false;
    bool cancelled = false;
};

PoolPtr openTestPool(BackendPtr backend)
{
    return Pool::open(std::move(backend), PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

/// I2 (backlog {#c2-resurrect-putoverwrite-fence-check}): a backend that (1) trips an injected side-effect
/// on the FIRST head() -- deterministically landing a fence trip BETWEEN `streamIfAbsent`'s conditional
/// create (which returns Occupied over a present condemned blob WITHOUT a head -- see
/// `conditionalCreateControlled`) and `uploadFromSource`'s condemned-displacement decision (whose first
/// head is the `head(key)` right before the raw `resurrect`/`putOverwrite` write) -- and (2) records
/// those two raw displacement calls, so a test proves the [C2] fence check aborted BEFORE any durable
/// displacement landed. Mirrors `TripOnHeadBackend` above and `RecordingStagingBackend` in
/// gtest_cas_s3_staging.cpp (neither reusable across their anonymous namespaces).
class TripOnHeadDisplacementBackend final : public InMemoryBackend
{
public:
    using Backend::putOverwrite;

    HeadResult head(const String & key) override
    {
        if (trigger)
            std::exchange(trigger, {})();
        return InMemoryBackend::head(key);
    }

    PutResult putOverwrite(const String & key, const String & bytes, const Token & expected, const ObjectMeta & meta) override
    {
        ++put_overwrite_calls;
        return InMemoryBackend::putOverwrite(key, bytes, expected, meta);
    }

    Token resurrect(DB::ReadBuffer & payload, uint64_t payload_size, const String & blob_key, const String & fresh_header) override
    {
        ++resurrect_staged_calls;
        return InMemoryBackend::resurrect(payload, payload_size, blob_key, fresh_header);
    }

    std::function<void()> trigger;
    int put_overwrite_calls = 0;
    int resurrect_staged_calls = 0;
};

/// A `BlobSource` that promotes via the S3 server-side-copy path (no local `open`) -- mirrors
/// `serverSideCopySource` in gtest_cas_s3_staging.cpp.
BlobSource serverSideCopySource(const std::string & staging_key, uint64_t size)
{
    BlobSource source;
    source.size = size;
    source.server_side_copy_from = staging_key;
    return source;
}

TEST(CASFenceGeneration, RearmPublishesTheNewGenerationBeforeOpeningTheFence)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openTestPool(backend);
    const RootNamespace ns{"srv1/rearm-publication-order"};
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns, store->liveWriterEpoch());
    ASSERT_EQ(store->refTableRuntimeIdentityForTest(ns), 0u);

    store->tripMountLost();
    const uint64_t dead_generation = store->fenceGeneration();
    bool admitted_in_interposition = false;
    store->setArmMountFenceInterpositionHookForTest([&]
    {
        EXPECT_EQ(store->fenceGeneration(), dead_generation + 1)
            << "the fresh generation must be visible before the fence can become live";
        try
        {
            (void)store->namespaceLife(ns);
            admitted_in_interposition = true;
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::NETWORK_ERROR);
        }
        EXPECT_EQ(store->refTableRuntimeIdentityForTest(ns), 0u)
            << "no runtime may be published in the re-arm interposition";
    });

    store->armMountFence(DB::UInt128{0, 1}, store->writerEpoch(), store->bootMsNow() + 600000);
    store->setArmMountFenceInterpositionHookForTest(nullptr);

    EXPECT_FALSE(admitted_in_interposition);
    EXPECT_EQ(store->refTableRuntimeIdentityForTest(ns), 0u);
    EXPECT_NO_THROW((void)store->namespaceLife(ns));
    EXPECT_EQ(store->refTableRuntimeAdmittedFenceGenerationForTest(ns), store->fenceGeneration());
}

}

/// (a) `casPutObject` (reached via `Pool::putNamespaceFile`) with the fence tripped BETWEEN admission
/// and the durable PUT: the typed transient refusal, and the object is never actually written.
TEST(CASFenceGeneration, PlainObjectPutAbortsWhenFenceTripsBetweenAdmissionAndDurableCall)
{
    auto backend = std::make_shared<TripOnHeadBackend>();
    auto store = openTestPool(backend);
    ASSERT_TRUE(store->mayMutate());

    const RootNamespace ns{"test/ns"};
    backend->trigger = [&] { store->tripMountLost(); };

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        store->putNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "somefile", "hello");
    });

    /// No durable write ever landed -- assert via the Emulated backend listing.
    EXPECT_TRUE(store->listNamespaceFiles(DB::Cas::tests::fixture::fixtureLife(ns)).empty());
}

/// `casRemoveObject`'s delete sibling, same shape: the fence trips between admission and the durable
/// delete, so the victim object survives untouched.
TEST(CASFenceGeneration, PlainObjectRemoveAbortsWhenFenceTripsBetweenAdmissionAndDurableCall)
{
    auto backend = std::make_shared<TripOnHeadBackend>();
    auto store = openTestPool(backend);
    const RootNamespace ns{"test/ns"};

    /// Seed the victim BEFORE arming the trigger -- the seeding write itself must not trip the fence.
    store->putNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "victim", "still here");
    ASSERT_TRUE(store->mayMutate());

    backend->trigger = [&] { store->tripMountLost(); };

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        store->removeNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "victim");
    });

    /// The durable delete never ran -- the object survives (reads are not fence-gated by this task).
    const auto still_there = store->getNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "victim");
    ASSERT_TRUE(still_there.has_value());
    EXPECT_EQ(*still_there, "still here");
}

/// The fence re-check must run before EVERY conditional-retry iteration, not just the first attempt
/// (spec wording, verbatim): a synthetic `PreconditionFailed` forces a second loop iteration, and the
/// fence trips on the SECOND `head()` call. If the check ran only once, at admission, this write would
/// incorrectly succeed on the retry.
TEST(CASFenceGeneration, PlainObjectPutRechecksFenceOnEveryRetryIterationNotJustFirst)
{
    auto backend = std::make_shared<TripOnSecondHeadBackend>();
    auto store = openTestPool(backend);
    ASSERT_TRUE(store->mayMutate());

    const RootNamespace ns{"test/ns"};
    backend->head_calls = 0;   /// reset past whatever `Pool::open`'s own probe/mount claim already did
    backend->fail_first_put = true;
    backend->trigger = [&] { store->tripMountLost(); };

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        store->putNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "somefile", "hello");
    });

    EXPECT_EQ(backend->head_calls, 2);
    EXPECT_TRUE(store->listNamespaceFiles(DB::Cas::tests::fixture::fixtureLife(ns)).empty());
}

/// (b) The S3-native staging-buffer finalize: the fence trips AFTER the buffer is constructed
/// (admission) but BEFORE `finalize()` reaches the durable `sink->finalize()` call -- same typed abort,
/// and the sink is never actually finalized (`on_finalized` never fires either, so the transaction
/// never learns of a promote-worthy hash/size for bytes that were never durable).
TEST(CASFenceGeneration, S3StagingFinalizeAbortsWhenFenceTripsBeforeDurableCall)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openTestPool(backend);
    ASSERT_TRUE(store->mayMutate());

    const std::string staging_key = "staging/mount1/racer.tmp";
    auto * sink_ptr = new RecordingSink(staging_key);
    std::unique_ptr<DB::WriteBufferFromFileBase> sink(sink_ptr);

    bool on_finalized_called = false;
    const uint64_t admitted_generation = store->fenceGeneration();

    auto buf = std::make_unique<CaContentWriteBuffer>(
        std::move(sink),
        staging_key,
        /*envelope_header=*/std::string(),
        BlobHashAlgo::CityHash128,
        /*buf_size=*/8192,
        /*use_adaptive_buffer_size=*/false,
        /*adaptive_buffer_initial_size=*/0,
        [&](const std::string &, size_t, const std::string &) { on_finalized_called = true; },
        [store, admitted_generation] { store->checkFenceOrThrow(admitted_generation); });

    const std::string payload = "some bytes that must never become durable";
    buf->write(payload.data(), payload.size());

    /// The race this test targets: admission already captured `admitted_generation` above, and now the
    /// fence trips before `finalize()` runs.
    store->tripMountLost();

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { buf->finalize(); });

    EXPECT_FALSE(on_finalized_called);
    EXPECT_FALSE(sink_ptr->wasFinalizedForTest());
}

/// (d) Happy path unchanged: an ordinary plain-object write/read/remove, and an ordinary S3-staging
/// finalize, both succeed exactly as before when the fence stays live throughout.
TEST(CASFenceGeneration, HappyPathPlainObjectWriteReadRemoveUnaffected)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openTestPool(backend);
    const RootNamespace ns{"test/ns"};

    store->putNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "a", "hello");
    const auto got = store->getNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "a");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, "hello");

    store->removeNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "a");
    EXPECT_FALSE(store->getNamespaceFile(DB::Cas::tests::fixture::fixtureLife(ns), "a").has_value());
}

TEST(CASFenceGeneration, HappyPathS3StagingFinalizeUnaffected)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openTestPool(backend);

    const std::string staging_key = "staging/mount1/happy.tmp";
    auto * sink_ptr = new RecordingSink(staging_key);
    std::unique_ptr<DB::WriteBufferFromFileBase> sink(sink_ptr);

    bool on_finalized_called = false;
    const uint64_t admitted_generation = store->fenceGeneration();

    auto buf = std::make_unique<CaContentWriteBuffer>(
        std::move(sink),
        staging_key,
        /*envelope_header=*/std::string(),
        BlobHashAlgo::CityHash128,
        /*buf_size=*/8192,
        /*use_adaptive_buffer_size=*/false,
        /*adaptive_buffer_initial_size=*/0,
        [&](const std::string &, size_t, const std::string &) { on_finalized_called = true; },
        [store, admitted_generation] { store->checkFenceOrThrow(admitted_generation); });

    const std::string payload = "unaffected happy path bytes";
    buf->write(payload.data(), payload.size());
    buf->finalize();

    EXPECT_TRUE(on_finalized_called);
    EXPECT_TRUE(sink_ptr->wasFinalizedForTest());
}

/// (I2, backlog {#c2-resurrect-putoverwrite-fence-check}) The condemned-displacement `putOverwrite` branch
/// of `PartWriteTxn::uploadFromSource` -- a RAW backend write outside the request controller's fence gate,
/// the last durable write left uncovered by Task 4. Seed a PRESENT, CONDEMNED blob, then trip the fence on
/// the head() that precedes the displacement decision: the [C2] `checkFenceOrThrow` must abort with the
/// typed transient refusal BEFORE `putOverwrite`, leaving the condemned incarnation untouched (no
/// stale-incarnation displacement).
TEST(CASFenceGeneration, CondemnedPutOverwriteAbortsWhenFenceTripsBeforeDurableCall)
{
    auto backend = std::make_shared<TripOnHeadDisplacementBackend>();
    auto store = openTestPool(backend);
    ASSERT_TRUE(store->mayMutate());

    const std::string payload = "condemned-overwrite-victim";
    const DB::UInt128 hash = DB::Cas::tests::u128Of(payload);
    const BlobRef id = DB::Cas::tests::idOf(payload);
    const std::string blob_key = store->layout().blobKey(id);

    /// A PRESENT, CONDEMNED incarnation (raw body + Clean meta flipped to Condemned) -- the only state that
    /// routes `uploadFromSource` into the streaming `putOverwrite` displacement branch.
    const uint64_t header_len = store->poolMeta().blob_header_len;
    std::string raw_body(header_len, '\0');
    raw_body += payload;
    DB::Cas::tests::writeRawBlobBody(*backend, store->layout(), hash, raw_body);
    DB::Cas::tests::writeMetaClean(*backend, store->layout(), hash, payload.size());
    DB::Cas::tests::condemnMeta(*backend, store->layout(), hash, /*condemn_round=*/1);
    const DB::Cas::Token condemned_token = backend->head(blob_key).token;
    ASSERT_FALSE(condemned_token.empty());

    auto build = store->beginPartWrite({});
    /// Reset counters past the setup writes (`condemnMeta` itself putOverwrites the meta key) and arm AFTER
    /// beginPartWrite (whose W-HEARTBEAT would otherwise consume the one-shot trigger). The first head()
    /// after this is the `head(key)` at uploadFromSource's condemned-displacement decision.
    backend->put_overwrite_calls = 0;
    backend->resurrect_staged_calls = 0;
    backend->trigger = [&] { store->tripMountLost(); };

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        build->putBlob(id, BlobSource::fromString(payload));
    });

    /// No displacement write landed: no putOverwrite at all, and the condemned incarnation survives with its
    /// original token.
    EXPECT_EQ(backend->put_overwrite_calls, 0) << "the fence check must abort before the raw putOverwrite";
    const DB::Cas::HeadResult after = backend->head(blob_key);
    ASSERT_TRUE(after.exists);
    EXPECT_EQ(after.token, condemned_token) << "the condemned blob must be untouched (INV: never a stale displacement)";
}

/// (I2, backlog {#c2-resurrect-putoverwrite-fence-check}) The S3-native-staging sibling: the condemned
/// displacement `resurrect` branch (`BlobSource::server_side_copy_from` set) is the same RAW,
/// controller-uncoupled backend write. Same setup + fence trip -> the [C2] `checkFenceOrThrow` aborts with
/// the typed transient refusal BEFORE `resurrect`, so no unconditional re-upload displaces
/// the condemned incarnation.
TEST(CASFenceGeneration, CondemnedResurrectStagedAbortsWhenFenceTripsBeforeDurableCall)
{
    auto backend = std::make_shared<TripOnHeadDisplacementBackend>();
    auto store = openTestPool(backend);
    ASSERT_TRUE(store->mayMutate());

    const std::string payload(300, 'c');
    const DB::UInt128 hash = DB::Cas::tests::u128Of(payload);
    const BlobRef id = DB::Cas::tests::idOf(payload);
    const std::string blob_key = store->layout().blobKey(id);
    const std::string staging_key = "p/staging/mount1/ccc.tmp";

    /// The staging object holds `[header][payload]`; the condemned blob is a plausible prior incarnation.
    DB::Cas::EnvelopeHeader staging_h;
    staging_h.kind = DB::Cas::ObjectKind::Blob;
    staging_h.incarnation_tag = DB::UInt128(0xC0FFEE);
    const std::string staging_header = DB::Cas::encodeEnvelopeHeader(
        staging_h, static_cast<uint32_t>(store->poolMeta().blob_header_len));
    const std::string staging_bytes = staging_header + payload;
    backend->putIfAbsent(staging_key, staging_bytes);
    backend->putIfAbsent(blob_key, staging_bytes);
    DB::Cas::tests::writeMetaClean(*backend, store->layout(), hash, payload.size());
    DB::Cas::tests::condemnMeta(*backend, store->layout(), hash, /*condemn_round=*/5);
    const DB::Cas::Token condemned_token = backend->head(blob_key).token;
    ASSERT_FALSE(condemned_token.empty());

    auto build = store->beginPartWrite({});
    backend->put_overwrite_calls = 0;
    backend->resurrect_staged_calls = 0;
    backend->trigger = [&] { store->tripMountLost(); };

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        build->putBlob(id, serverSideCopySource(staging_key, payload.size()));
    });

    /// No unconditional server-side copy displaced the condemned incarnation.
    EXPECT_EQ(backend->resurrect_staged_calls, 0) << "the fence check must abort before the raw resurrect";
    const DB::Cas::HeadResult after = backend->head(blob_key);
    ASSERT_TRUE(after.exists);
    EXPECT_EQ(after.token, condemned_token) << "the condemned blob must be untouched (INV: never a stale displacement)";
}
