#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobMeta.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <new>
#include <vector>

using namespace DB::Cas;
using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;
using DB::Cas::tests::loadMetaForTest;
using DB::Cas::tests::writeMetaClean;
using DB::Cas::tests::condemnMeta;
using DB::Cas::tests::blobEntryFor;
using DB::Cas::tests::expectThrowsCode;  // NOLINT(misc-unused-using-decls): only used inside `#ifndef DEBUG_OR_SANITIZER_BUILD` -- unused in a sanitizer build's TU, used in a release build's

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

/// Open a Pool over `b`. `head_first_min_bytes` steers the HEAD-before-PUT size trigger: the default
/// (1 MiB) keeps the trigger off for the small test payloads (so a branch is reached only via the dedup
/// cache), while a value of 1 forces the HEAD-first path for any non-empty blob (the `HeadFirstHit` leg).
PoolPtr openUploadPool(const std::shared_ptr<InMemoryBackend> & b, uint64_t head_first_min_bytes = (1ULL << 20))
{
    return Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                                    .deduplication_head_first_min_bytes = head_first_min_bytes});
}

/// Stage a one-blob manifest for `payload` and precommit it, so `observeAndAdmit`'s EDGE-BEFORE-OBSERVE
/// fail-closed check holds on the adopt branches. Returns the build ready for an upload of `idOf(payload)`.
PartWriteTxnPtr precommitBuildFor(
    const PoolPtr & s, const RootNamespace & ns, const String & ref, const String & payload)
{
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    PartWriteTxnPtr build = s->beginPartWrite(std::move(info));
    const ManifestId id = build->stageManifest({blobEntryFor("col.bin", u128Of(payload), payload.size())});
    build->precommitAdd(ns, ref, id);
    return build;
}

/// Seed a present, well-formed blob body whose LOGICAL bytes are exactly `payload` (a fixed envelope
/// header followed by the payload), so a later HEAD returns a token and a logical size of `payload.size()`.
void seedPresentBody(
    InMemoryBackend & b, const Layout & layout, const PoolMeta & pm, const BlobRef & ref, const String & payload)
{
    EnvelopeHeader h;
    h.kind = ObjectKind::Blob;
    h.incarnation_tag = DB::UInt128(0xABCD);
    h.build_id = DB::UInt128(0x1111);
    const String head = encodeEnvelopeHeader(h, static_cast<uint32_t>(pm.blob_header_len));
    b.putIfAbsent(layout.blobKey(ref), head + payload);
}

/// The logical payload stored at `key` (object body minus the fixed blob header), or empty when absent.
String logicalPayloadAt(InMemoryBackend & b, const String & key, uint64_t header_len)
{
    const auto got = b.get(key);
    if (!got || got->bytes.size() < header_len)
        return {};
    return got->bytes.substr(header_len);
}

/// The blob's meta state, or nullopt when the meta object is absent.
std::optional<MetaState> metaStateAt(InMemoryBackend & b, const Layout & layout, const String & payload)
{
    const auto lm = loadMetaForTest(b, layout, u128Of(payload));
    return lm ? std::optional<MetaState>(lm->meta.state) : std::nullopt;
}

}

/// dedup-cache hit: the ref is known-present in the dedup cache ⇒ HEAD-first ⇒ present ⇒ adopt.
/// The returned result is a complete tokened adopt dep with the `DeduplicationCacheHit` outcome; the build's
/// dep set stays untouched; the backend body/meta match the serial `putBlob` for the same input.
TEST(CASUploadDetached, DeduplicationCacheHitAdoptsBuildUntouched)
{
    const RootNamespace ns{"srv1/nsDedup"};
    const String ref_name = "part";
    const String payload = "dedup-cache-hit-payload";
    const BlobRef blob = idOf(payload);

    auto arrange = [&](std::shared_ptr<InMemoryBackend> & b, PoolPtr & s, PartWriteTxnPtr & build)
    {
        b = std::make_shared<InMemoryBackend>();
        s = openUploadPool(b);
        seedPresentBody(*b, s->layout(), s->poolMeta(), blob, payload);
        writeMetaClean(*b, s->layout(), u128Of(payload), payload.size());
        s->dedupCacheAdd(blob);
        build = precommitBuildFor(s, ns, ref_name, payload);
    };

    std::shared_ptr<InMemoryBackend> b1;
    PoolPtr s1;
    PartWriteTxnPtr build1;
    arrange(b1, s1, build1);
    const String key = s1->layout().blobKey(blob);

    EXPECT_FALSE(build1->depIsTokened(blob));

    const BlobUploadResult r = build1->uploadBlobDetached(
        BlobUploadRequest{blob, BlobSource::fromString(payload), payload.size()});

    EXPECT_EQ(r.outcome, BlobUploadOutcome::DeduplicationCacheHit);
    EXPECT_EQ(r.ref, blob);
    EXPECT_EQ(r.dep.kind, ObjectKind::Blob);
    ASSERT_TRUE(r.dep.token.has_value());
    EXPECT_FALSE(r.dep.token->value.empty());
    EXPECT_FALSE(r.dep.adopted);
    EXPECT_EQ(r.dep.size, payload.size());

    /// Build UNTOUCHED: the detached primitive folded no dep.
    EXPECT_FALSE(build1->depIsTokened(blob));

    /// Serial reference on an identically-arranged world: putBlob folds the dep; end-state matches.
    std::shared_ptr<InMemoryBackend> b2;
    PoolPtr s2;
    PartWriteTxnPtr build2;
    arrange(b2, s2, build2);
    const PutBlobResult pr = build2->putBlob(blob, BlobSource::fromString(payload));
    EXPECT_TRUE(build2->depIsTokened(blob));
    EXPECT_EQ(pr.size, r.dep.size);

    EXPECT_EQ(logicalPayloadAt(*b1, key, s1->poolMeta().blob_header_len),
              logicalPayloadAt(*b2, key, s2->poolMeta().blob_header_len));
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), metaStateAt(*b2, s2->layout(), payload));
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), std::optional<MetaState>(MetaState::Clean));
}

/// HEAD-first hit (size-triggered, not cached): a present body under the size trigger is adopted with
/// the `HeadHit` outcome. Distinct from the dedup-cache leg by NOT being in the dedup cache.
TEST(CASUploadDetached, HeadFirstHitAdopts)
{
    const RootNamespace ns{"srv1/nsHead"};
    const String ref_name = "part";
    const String payload = "head-first-hit-payload";
    const BlobRef blob = idOf(payload);

    auto arrange = [&](std::shared_ptr<InMemoryBackend> & b, PoolPtr & s, PartWriteTxnPtr & build)
    {
        b = std::make_shared<InMemoryBackend>();
        s = openUploadPool(b, /*head_first_min_bytes=*/1);   /// force HEAD-first without the dedup cache
        seedPresentBody(*b, s->layout(), s->poolMeta(), blob, payload);
        writeMetaClean(*b, s->layout(), u128Of(payload), payload.size());
        build = precommitBuildFor(s, ns, ref_name, payload);
    };

    std::shared_ptr<InMemoryBackend> b1;
    PoolPtr s1;
    PartWriteTxnPtr build1;
    arrange(b1, s1, build1);
    const String key = s1->layout().blobKey(blob);

    EXPECT_FALSE(build1->depIsTokened(blob));

    const BlobUploadResult r = build1->uploadBlobDetached(
        BlobUploadRequest{blob, BlobSource::fromString(payload), payload.size()});

    EXPECT_EQ(r.outcome, BlobUploadOutcome::HeadHit);
    ASSERT_TRUE(r.dep.token.has_value());
    EXPECT_FALSE(r.dep.adopted);
    EXPECT_EQ(r.dep.size, payload.size());

    EXPECT_FALSE(build1->depIsTokened(blob));

    std::shared_ptr<InMemoryBackend> b2;
    PoolPtr s2;
    PartWriteTxnPtr build2;
    arrange(b2, s2, build2);
    build2->putBlob(blob, BlobSource::fromString(payload));
    EXPECT_TRUE(build2->depIsTokened(blob));

    EXPECT_EQ(logicalPayloadAt(*b1, key, s1->poolMeta().blob_header_len),
              logicalPayloadAt(*b2, key, s2->poolMeta().blob_header_len));
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), std::optional<MetaState>(MetaState::Clean));
}

/// HEAD-first miss, then a live adopt via the conditional-create 412 path, with the meta point-read
/// finding no meta and backfilling `Clean`. Outcome `HeadMissAdopted`.
TEST(CASUploadDetached, HeadMissLiveAdoptBackfills)
{
    const RootNamespace ns{"srv1/nsAdopt"};
    const String ref_name = "part";
    const String payload = "head-miss-adopt-payload";
    const BlobRef blob = idOf(payload);

    auto arrange = [&](std::shared_ptr<InMemoryBackend> & b, PoolPtr & s, PartWriteTxnPtr & build)
    {
        b = std::make_shared<InMemoryBackend>();
        s = openUploadPool(b);                        /// default trigger off ⇒ no HEAD-first
        seedPresentBody(*b, s->layout(), s->poolMeta(), blob, payload);   /// body present, NO meta ⇒ backfill
        build = precommitBuildFor(s, ns, ref_name, payload);
    };

    std::shared_ptr<InMemoryBackend> b1;
    PoolPtr s1;
    PartWriteTxnPtr build1;
    arrange(b1, s1, build1);
    const String key = s1->layout().blobKey(blob);

    ASSERT_FALSE(metaStateAt(*b1, s1->layout(), payload).has_value());   /// precondition: meta absent
    EXPECT_FALSE(build1->depIsTokened(blob));

    const BlobUploadResult r = build1->uploadBlobDetached(
        BlobUploadRequest{blob, BlobSource::fromString(payload), payload.size()});

    EXPECT_EQ(r.outcome, BlobUploadOutcome::HeadMissAdopted);
    ASSERT_TRUE(r.dep.token.has_value());
    EXPECT_FALSE(r.dep.adopted);
    EXPECT_EQ(r.dep.size, payload.size());

    EXPECT_FALSE(build1->depIsTokened(blob));
    /// The point-read backfilled a Clean meta.
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), std::optional<MetaState>(MetaState::Clean));

    std::shared_ptr<InMemoryBackend> b2;
    PoolPtr s2;
    PartWriteTxnPtr build2;
    arrange(b2, s2, build2);
    build2->putBlob(blob, BlobSource::fromString(payload));
    EXPECT_TRUE(build2->depIsTokened(blob));

    EXPECT_EQ(logicalPayloadAt(*b1, key, s1->poolMeta().blob_header_len),
              logicalPayloadAt(*b2, key, s2->poolMeta().blob_header_len));
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), metaStateAt(*b2, s2->layout(), payload));
}

/// Fresh local streaming: nothing present ⇒ the write-once conditional create streams the body and
/// creates the Clean meta. Outcome `FreshUpload`, a tokened dep sized to the source.
TEST(CASUploadDetached, FreshLocalStreaming)
{
    const RootNamespace ns{"srv1/nsFresh"};
    const String ref_name = "part";
    const String payload = "fresh-local-streaming-payload";
    const BlobRef blob = idOf(payload);

    auto arrange = [&](std::shared_ptr<InMemoryBackend> & b, PoolPtr & s, PartWriteTxnPtr & build)
    {
        b = std::make_shared<InMemoryBackend>();
        s = openUploadPool(b);
        build = precommitBuildFor(s, ns, ref_name, payload);
    };

    std::shared_ptr<InMemoryBackend> b1;
    PoolPtr s1;
    PartWriteTxnPtr build1;
    arrange(b1, s1, build1);
    const String key = s1->layout().blobKey(blob);

    ASSERT_FALSE(b1->head(key).exists);   /// precondition: absent
    EXPECT_FALSE(build1->depIsTokened(blob));

    const BlobUploadResult r = build1->uploadBlobDetached(
        BlobUploadRequest{blob, BlobSource::fromString(payload), payload.size()});

    EXPECT_EQ(r.outcome, BlobUploadOutcome::FreshUpload);
    EXPECT_EQ(r.ref, blob);
    ASSERT_TRUE(r.dep.token.has_value());
    EXPECT_FALSE(r.dep.token->value.empty());
    EXPECT_FALSE(r.dep.adopted);
    EXPECT_EQ(r.dep.size, payload.size());

    EXPECT_FALSE(build1->depIsTokened(blob));
    EXPECT_TRUE(b1->head(key).exists);
    EXPECT_EQ(logicalPayloadAt(*b1, key, s1->poolMeta().blob_header_len), payload);
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), std::optional<MetaState>(MetaState::Clean));

    std::shared_ptr<InMemoryBackend> b2;
    PoolPtr s2;
    PartWriteTxnPtr build2;
    arrange(b2, s2, build2);
    const PutBlobResult pr = build2->putBlob(blob, BlobSource::fromString(payload));
    EXPECT_TRUE(build2->depIsTokened(blob));
    EXPECT_EQ(pr.size, r.dep.size);

    /// The envelope's fresh incarnation tag differs per upload, but the LOGICAL payload and meta match.
    EXPECT_EQ(logicalPayloadAt(*b1, key, s1->poolMeta().blob_header_len),
              logicalPayloadAt(*b2, key, s2->poolMeta().blob_header_len));
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), metaStateAt(*b2, s2->layout(), payload));
}

/// S3-native staging promotion: the source carries a server-side-copy descriptor and the blob key is
/// absent ⇒ a write-once conditional server-side copy creates the blob. Outcome `StagingPromoted`.
TEST(CASUploadDetached, S3StagingPromotion)
{
    const RootNamespace ns{"srv1/nsStaging"};
    const String ref_name = "part";
    const String payload = "s3-staging-promotion-payload";
    const BlobRef blob = idOf(payload);
    const String staging_key = "p/staging/mount1/promote.tmp";

    auto arrange = [&](std::shared_ptr<InMemoryBackend> & b, PoolPtr & s, PartWriteTxnPtr & build, String & staging_bytes)
    {
        b = std::make_shared<InMemoryBackend>();
        s = openUploadPool(b);
        /// The staging object holds [header][payload], exactly as the S3-staging writer emits it.
        EnvelopeHeader h;
        h.kind = ObjectKind::Blob;
        h.incarnation_tag = DB::UInt128(0xC0FFEE);
        staging_bytes = encodeEnvelopeHeader(h, static_cast<uint32_t>(s->poolMeta().blob_header_len)) + payload;
        b->putIfAbsent(staging_key, staging_bytes);
        build = precommitBuildFor(s, ns, ref_name, payload);
    };

    auto stagingSource = [&]() -> BlobSource
    {
        BlobSource src;
        src.size = payload.size();
        src.server_side_copy_from = staging_key;
        return src;
    };

    std::shared_ptr<InMemoryBackend> b1;
    PoolPtr s1;
    PartWriteTxnPtr build1;
    String staging_bytes1;
    arrange(b1, s1, build1, staging_bytes1);
    const String key = s1->layout().blobKey(blob);

    ASSERT_FALSE(b1->head(key).exists);
    EXPECT_FALSE(build1->depIsTokened(blob));

    const BlobUploadResult r = build1->uploadBlobDetached(
        BlobUploadRequest{blob, stagingSource(), payload.size()});

    EXPECT_EQ(r.outcome, BlobUploadOutcome::StagingPromoted);
    ASSERT_TRUE(r.dep.token.has_value());
    EXPECT_FALSE(r.dep.token->value.empty());
    EXPECT_FALSE(r.dep.adopted);
    EXPECT_EQ(r.dep.size, payload.size());

    EXPECT_FALSE(build1->depIsTokened(blob));
    ASSERT_TRUE(b1->head(key).exists);
    /// The server-side copy moved the staging bytes verbatim to the blob key.
    const auto got = b1->get(key);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, staging_bytes1);

    std::shared_ptr<InMemoryBackend> b2;
    PoolPtr s2;
    PartWriteTxnPtr build2;
    String staging_bytes2;
    arrange(b2, s2, build2, staging_bytes2);
    build2->putBlob(blob, stagingSource());
    EXPECT_TRUE(build2->depIsTokened(blob));

    const auto got2 = b2->get(key);
    ASSERT_TRUE(got2.has_value());
    EXPECT_EQ(got->bytes, got2->bytes);
}

/// Condemned-local resurrection: a present body observed condemned via the meta point-read is displaced
/// by a fresh incarnation streamed from the writer's OWN source (`putOverwrite`), never a read of the
/// dying object. Outcome `ResurrectedLocal`; the token is refreshed and the meta returns to Clean.
TEST(CASUploadDetached, CondemnedLocalResurrection)
{
    const RootNamespace ns{"srv1/nsResLocal"};
    const String ref_name = "part";
    const String payload = "condemned-local-resurrect-payload";
    const BlobRef blob = idOf(payload);

    auto arrange = [&](std::shared_ptr<InMemoryBackend> & b, PoolPtr & s, PartWriteTxnPtr & build)
    {
        b = std::make_shared<InMemoryBackend>();
        s = openUploadPool(b);
        seedPresentBody(*b, s->layout(), s->poolMeta(), blob, payload);
        writeMetaClean(*b, s->layout(), u128Of(payload), payload.size());
        condemnMeta(*b, s->layout(), u128Of(payload), /*condemn_round=*/7);
        build = precommitBuildFor(s, ns, ref_name, payload);
    };

    std::shared_ptr<InMemoryBackend> b1;
    PoolPtr s1;
    PartWriteTxnPtr build1;
    arrange(b1, s1, build1);
    const String key = s1->layout().blobKey(blob);
    const Token condemned_token = b1->head(key).token;

    ASSERT_EQ(metaStateAt(*b1, s1->layout(), payload), std::optional<MetaState>(MetaState::Condemned));
    EXPECT_FALSE(build1->depIsTokened(blob));

    const BlobUploadResult r = build1->uploadBlobDetached(
        BlobUploadRequest{blob, BlobSource::fromString(payload), payload.size()});

    EXPECT_EQ(r.outcome, BlobUploadOutcome::ResurrectedLocal);
    ASSERT_TRUE(r.dep.token.has_value());
    EXPECT_FALSE(r.dep.adopted);
    EXPECT_EQ(r.dep.size, payload.size());

    EXPECT_FALSE(build1->depIsTokened(blob));
    /// The condemned incarnation was displaced by a fresh one (token changed) and the meta is Clean again.
    const Token after_token = b1->head(key).token;
    EXPECT_NE(after_token.value, condemned_token.value);
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), std::optional<MetaState>(MetaState::Clean));
    EXPECT_EQ(logicalPayloadAt(*b1, key, s1->poolMeta().blob_header_len), payload);

    std::shared_ptr<InMemoryBackend> b2;
    PoolPtr s2;
    PartWriteTxnPtr build2;
    arrange(b2, s2, build2);
    build2->putBlob(blob, BlobSource::fromString(payload));
    EXPECT_TRUE(build2->depIsTokened(blob));

    EXPECT_EQ(logicalPayloadAt(*b1, key, s1->poolMeta().blob_header_len),
              logicalPayloadAt(*b2, key, s2->poolMeta().blob_header_len));
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), metaStateAt(*b2, s2->layout(), payload));
}

/// Condemned-S3 resurrection: a present body observed condemned with an S3 staging source is displaced
/// by an unconditional server-side copy from the SAME staging object under a fresh-tagged header, never
/// a read/copy of the condemned blob key. Outcome `ResurrectedS3`.
TEST(CASUploadDetached, CondemnedS3Resurrection)
{
    const RootNamespace ns{"srv1/nsResS3"};
    const String ref_name = "part";
    const String payload = "condemned-s3-resurrect-payload";
    const BlobRef blob = idOf(payload);
    const String staging_key = "p/staging/mount1/resurrect.tmp";

    auto arrange = [&](std::shared_ptr<InMemoryBackend> & b, PoolPtr & s, PartWriteTxnPtr & build)
    {
        b = std::make_shared<InMemoryBackend>();
        s = openUploadPool(b);
        EnvelopeHeader h;
        h.kind = ObjectKind::Blob;
        h.incarnation_tag = DB::UInt128(0xC0FFEE);
        const String staging_bytes = encodeEnvelopeHeader(h, static_cast<uint32_t>(s->poolMeta().blob_header_len)) + payload;
        b->putIfAbsent(staging_key, staging_bytes);
        /// Seed the condemned blob body = exactly a verbatim promote of the staging object would produce.
        b->putIfAbsent(s->layout().blobKey(blob), staging_bytes);
        writeMetaClean(*b, s->layout(), u128Of(payload), payload.size());
        condemnMeta(*b, s->layout(), u128Of(payload), /*condemn_round=*/9);
        build = precommitBuildFor(s, ns, ref_name, payload);
    };

    auto stagingSource = [&]() -> BlobSource
    {
        BlobSource src;
        src.size = payload.size();
        src.server_side_copy_from = staging_key;
        return src;
    };

    std::shared_ptr<InMemoryBackend> b1;
    PoolPtr s1;
    PartWriteTxnPtr build1;
    arrange(b1, s1, build1);
    const String key = s1->layout().blobKey(blob);
    const Token condemned_token = b1->head(key).token;

    ASSERT_EQ(metaStateAt(*b1, s1->layout(), payload), std::optional<MetaState>(MetaState::Condemned));
    EXPECT_FALSE(build1->depIsTokened(blob));

    const BlobUploadResult r = build1->uploadBlobDetached(
        BlobUploadRequest{blob, stagingSource(), payload.size()});

    EXPECT_EQ(r.outcome, BlobUploadOutcome::ResurrectedS3);
    ASSERT_TRUE(r.dep.token.has_value());
    EXPECT_FALSE(r.dep.adopted);
    EXPECT_EQ(r.dep.size, payload.size());

    EXPECT_FALSE(build1->depIsTokened(blob));
    /// A fresh incarnation displaced the condemned one (INV-NO-RETURN: fresh tag ⇒ different token).
    const Token after_token = b1->head(key).token;
    EXPECT_NE(after_token.value, condemned_token.value);
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), std::optional<MetaState>(MetaState::Clean));

    std::shared_ptr<InMemoryBackend> b2;
    PoolPtr s2;
    PartWriteTxnPtr build2;
    arrange(b2, s2, build2);
    build2->putBlob(blob, stagingSource());
    EXPECT_TRUE(build2->depIsTokened(blob));

    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), metaStateAt(*b2, s2->layout(), payload));
}

/// `mergeBlobUploadResults` folds N detached results in ONE call to EXACTLY the same deps a serial
/// putBlob fold would produce. Both worlds run the identical sequence of backend calls (same
/// precommit, same blobs in the same order), so their independently-minted tokens line up too -- the
/// merge path adds no backend calls of its own, only in-memory bookkeeping, so a DEEP dep-map
/// comparison (tokens included) is exact, not just per-ref.
TEST(CASUploadDetached, MergeAppliesAllDeps)
{
    const RootNamespace ns{"srv1/nsMergeAll"};
    const String ref_name = "part";
    const std::vector<String> payloads = {"merge-fresh-a", "merge-fresh-b", "merge-fresh-c"};

    auto arrange = [&](std::shared_ptr<InMemoryBackend> & b, PoolPtr & s, PartWriteTxnPtr & build)
    {
        b = std::make_shared<InMemoryBackend>();
        s = openUploadPool(b);
        build = precommitBuildFor(s, ns, ref_name, "manifest-seed");
    };

    std::shared_ptr<InMemoryBackend> b1;
    PoolPtr s1;
    PartWriteTxnPtr build1;
    arrange(b1, s1, build1);

    std::vector<BlobUploadResult> results;
    for (const auto & payload : payloads)
    {
        const BlobRef blob = idOf(payload);
        EXPECT_FALSE(build1->depIsTokened(blob));
        results.push_back(build1->uploadBlobDetached(
            BlobUploadRequest{blob, BlobSource::fromString(payload), payload.size()}));
    }
    /// Still untouched before the merge -- uploadBlobDetached folds nothing.
    for (const auto & payload : payloads)
        EXPECT_FALSE(build1->depIsTokened(idOf(payload)));

    build1->mergeBlobUploadResults(results);

    for (const auto & payload : payloads)
        EXPECT_TRUE(build1->depIsTokened(idOf(payload)));

    std::shared_ptr<InMemoryBackend> b2;
    PoolPtr s2;
    PartWriteTxnPtr build2;
    arrange(b2, s2, build2);
    for (const auto & payload : payloads)
        build2->putBlob(idOf(payload), BlobSource::fromString(payload));

    EXPECT_EQ(build1->depsSnapshotForTest(), build2->depsSnapshotForTest());
}

/// Merge exception safety (spec Test 16): a hook injected between per-result applications throws
/// after the FIRST result would have applied; the SECOND result must never reach `deps`, and neither
/// may a PRE-EXISTING unrelated dep be disturbed -- a DEEP snapshot (the whole map, not one ref probed
/// at a time) proves the build is byte-for-byte at its pre-merge state, all-or-nothing observed.
TEST(CASUploadDetached, MergeFailureLeavesBuildUntouched)
{
    const RootNamespace ns{"srv1/nsMergeFail"};
    const String ref_name = "part";
    const String payload_existing = "merge-fail-existing";
    const String payload_a = "merge-fail-a";
    const String payload_b = "merge-fail-b";

    auto b = std::make_shared<InMemoryBackend>();
    auto s = openUploadPool(b);
    auto build = precommitBuildFor(s, ns, ref_name, "manifest-seed");

    /// A pre-existing folded dep the merge must leave completely alone.
    build->putBlob(idOf(payload_existing), BlobSource::fromString(payload_existing));
    ASSERT_TRUE(build->depIsTokened(idOf(payload_existing)));

    std::vector<BlobUploadResult> results;
    results.push_back(build->uploadBlobDetached(
        BlobUploadRequest{idOf(payload_a), BlobSource::fromString(payload_a), payload_a.size()}));
    results.push_back(build->uploadBlobDetached(
        BlobUploadRequest{idOf(payload_b), BlobSource::fromString(payload_b), payload_b.size()}));

    const auto pre_merge_snapshot = build->depsSnapshotForTest();
    ASSERT_EQ(pre_merge_snapshot.size(), 1u);   /// only the pre-existing dep; the detached uploads folded nothing

    build->setMergeHookForTest([](size_t applied_so_far)
    {
        if (applied_so_far == 1)
            throw std::bad_alloc();
    });

    EXPECT_THROW(build->mergeBlobUploadResults(results), std::bad_alloc);

    EXPECT_EQ(build->depsSnapshotForTest(), pre_merge_snapshot);
    EXPECT_FALSE(build->depIsTokened(idOf(payload_a)));
    EXPECT_FALSE(build->depIsTokened(idOf(payload_b)));
}

/// Duplicate-grouping consistency: two results for the SAME ref with conflicting sizes are rejected
/// as a staging bug (LOGICAL_ERROR) BEFORE any result applies -- the fan-out's one-task-per-unique-ref
/// invariant means this should never happen upstream, so merge itself is the backstop. LOGICAL_ERROR
/// aborts the whole process in debug/sanitizer builds instead of behaving like a catchable exception
/// (`Common/Exception.cpp`'s `handle_error_code`) -- `CASUploadDetachedDeathTest` below proves the
/// abort positively in those builds instead (it cannot also verify the build-untouched postcondition,
/// since there is no continuation after a real abort).
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASUploadDetached, MergeValidatesSizes)
{
    const RootNamespace ns{"srv1/nsMergeSizes"};
    const String ref_name = "part";
    const String payload = "merge-size-conflict";
    const BlobRef blob = idOf(payload);

    auto b = std::make_shared<InMemoryBackend>();
    auto s = openUploadPool(b);
    auto build = precommitBuildFor(s, ns, ref_name, "manifest-seed");

    const BlobUploadResult r = build->uploadBlobDetached(
        BlobUploadRequest{blob, BlobSource::fromString(payload), payload.size()});
    ASSERT_FALSE(build->depIsTokened(blob));

    BlobUploadResult conflicting = r;
    conflicting.dep.size = r.dep.size + 1;   /// same ref, conflicting declared size

    const auto pre_merge_snapshot = build->depsSnapshotForTest();

    expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&]
    {
        build->mergeBlobUploadResults(std::vector<BlobUploadResult>{r, conflicting});
    });

    EXPECT_EQ(build->depsSnapshotForTest(), pre_merge_snapshot);
    EXPECT_FALSE(build->depIsTokened(blob));
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASUploadDetachedDeathTest, MergeValidatesSizesAborts)
{
    const RootNamespace ns{"srv1/nsMergeSizes"};
    const String ref_name = "part";
    const String payload = "merge-size-conflict";
    const BlobRef blob = idOf(payload);

    auto b = std::make_shared<InMemoryBackend>();
    auto s = openUploadPool(b);
    auto build = precommitBuildFor(s, ns, ref_name, "manifest-seed");

    const BlobUploadResult r = build->uploadBlobDetached(
        BlobUploadRequest{blob, BlobSource::fromString(payload), payload.size()});

    BlobUploadResult conflicting = r;
    conflicting.dep.size = r.dep.size + 1;   /// same ref, conflicting declared size

    EXPECT_DEATH(
        { build->mergeBlobUploadResults(std::vector<BlobUploadResult>{r, conflicting}); }, "");
}
#endif
