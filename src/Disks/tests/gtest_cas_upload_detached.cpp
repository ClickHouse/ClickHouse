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
#include <Common/ProfileEvents.h>
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

namespace ProfileEvents
{
extern const Event CASBlobBodyPutAvoided;
}

namespace DB::ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int LOGICAL_ERROR;
}

namespace
{

/// Open a Pool over `b`.
PoolPtr openUploadPool(const std::shared_ptr<InMemoryBackend> & b)
{
    return Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

BlobSource reReadableStagedSource(
    const BackendPtr & backend, const String & staging_key, uint64_t payload_size, uint64_t header_len)
{
    BlobSource source;
    source.size = payload_size;
    source.server_side_copy_from = staging_key;
    source.open = [backend, staging_key, header_len, payload_size]() -> std::unique_ptr<DB::ReadBuffer>
    {
        DB::Cas::tests::OperationForTest op(backend);
        std::unique_ptr<DB::ReadBuffer> staged = (*op).stream(staging_key, Retry::once());
        if (!staged)
            throw DB::Exception(DB::ErrorCodes::FILE_DOESNT_EXIST, "staging object {} is absent", staging_key);
        String encoded_header(header_len, '\0');
        staged->readStrict(encoded_header.data(), encoded_header.size());
        (void)decodeEnvelopeHeader(encoded_header, header_len + payload_size, ObjectKind::Blob);
        return staged;
    };
    return source;
}

/// Stage a one-blob manifest for `payload` and durably precommit it before materialization.
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

/// A one-shot `create`, asserting it committed (mirrors the retired `backend.putIfAbsent(key, bytes)`).
void createObj(Backend & backend, const String & key, const String & bytes)
{
    DB::Cas::tests::OperationForTest op(backend);
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).create(key, bytes, Retry::once())));
}

/// An exact read (mirrors the retired `backend.get(key)`).
std::optional<Object> readObj(Backend & backend, const String & key)
{
    DB::Cas::tests::OperationForTest op(backend);
    return (*op).read(key, Retry::standard());
}

/// Whether `key` has a value, through a HEAD (mirrors the retired `backend.head(key).exists`).
bool headPresent(Backend & backend, const String & key)
{
    DB::Cas::tests::OperationForTest op(backend);
    return (*op).head(key, Retry::standard()).has_value();
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
    createObj(b, layout.blobKey(ref), head + payload);
}

/// The logical payload stored at `key` (object body minus the fixed blob header), or empty when absent.
String logicalPayloadAt(InMemoryBackend & b, const String & key, uint64_t header_len)
{
    const auto got = readObj(b, key);
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

/// Records only the watched blob lane, so pool-open and precommit traffic cannot obscure the
/// transaction-level ordering asserted below.
class ProtocolRecordingBackend final : public InMemoryBackend
{
public:
    /// Unhide the legacy overload that the primitive override below would otherwise hide.
    using InMemoryBackend::head;
    void watch(String blob_key_, String meta_key_)
    {
        blob_key = std::move(blob_key_);
        meta_key = std::move(meta_key_);
        operations.clear();
        blob_heads = 0;
        meta_gets = 0;
        publish_calls = 0;
        meta_gets_before_first_publish.reset();
    }

    std::optional<RawMeta> head(const String & key, TransportAccess & access) override
    {
        if (key == blob_key)
        {
            ++blob_heads;
            operations.emplace_back("head");
        }
        return InMemoryBackend::head(key, access);
    }

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (key == meta_key)
        {
            ++meta_gets;
            operations.emplace_back("meta-get");
        }
        return InMemoryBackend::read(key, access);
    }

    void publish(const BlobPublishRequest & request, TransportAccess & access) override
    {
        if (request.destination_key == blob_key)
        {
            ++publish_calls;
            operations.emplace_back("publish");
            if (!meta_gets_before_first_publish)
                meta_gets_before_first_publish = meta_gets;
        }
        InMemoryBackend::publish(request, access);
    }

    String blob_key;
    String meta_key;
    std::vector<String> operations;
    size_t blob_heads = 0;
    size_t meta_gets = 0;
    size_t publish_calls = 0;
    std::optional<size_t> meta_gets_before_first_publish;
};

}

TEST(CASUploadDetached, FreshMissHeadsThenPublishesWithoutPrepublicationMetaGet)
{
    const String payload = "mandatory-head-fresh-miss";
    const BlobRef ref = idOf(payload);
    auto backend = std::make_shared<ProtocolRecordingBackend>();
    auto store = openUploadPool(backend);
    auto build = precommitBuildFor(store, RootNamespace{"srv1/protocol-fresh"}, "part", payload);
    const String blob_key = store->layout().blobKey(ref);
    backend->watch(blob_key, store->layout().blobMetaKey(ref));

    const BlobUploadResult result = build->uploadBlobDetached(
        BlobUploadRequest{ref, BlobSource::fromString(payload), payload.size()});

    EXPECT_EQ(result.dep.proof, BlobDependencyProof::Materialized);
    ASSERT_FALSE(backend->operations.empty());
    EXPECT_EQ(backend->operations.front(), "head");
    EXPECT_EQ(backend->blob_heads, 1u);
    EXPECT_EQ(backend->publish_calls, 1u);
    ASSERT_TRUE(backend->meta_gets_before_first_publish.has_value());
    EXPECT_EQ(*backend->meta_gets_before_first_publish, 0u);
}

TEST(CASUploadDetached, ExistingCleanHeadsAndObservesWithoutPublication)
{
    const String payload = "mandatory-head-existing-clean";
    const BlobRef ref = idOf(payload);
    auto backend = std::make_shared<ProtocolRecordingBackend>();
    auto store = openUploadPool(backend);
    seedPresentBody(*backend, store->layout(), store->poolMeta(), ref, payload);
    writeMetaClean(*backend, store->layout(), u128Of(payload), payload.size());
    auto build = precommitBuildFor(store, RootNamespace{"srv1/protocol-clean"}, "part", payload);
    backend->watch(store->layout().blobKey(ref), store->layout().blobMetaKey(ref));
    const uint64_t avoided_before = ProfileEvents::global_counters[ProfileEvents::CASBlobBodyPutAvoided].load();

    const BlobUploadResult result = build->uploadBlobDetached(
        BlobUploadRequest{ref, BlobSource::fromString(payload), payload.size()});

    EXPECT_EQ(result.dep.proof, BlobDependencyProof::Materialized);
    ASSERT_FALSE(backend->operations.empty());
    EXPECT_EQ(backend->operations.front(), "head");
    EXPECT_EQ(backend->blob_heads, 1u);
    EXPECT_EQ(backend->meta_gets, 1u);
    EXPECT_EQ(backend->publish_calls, 0u);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASBlobBodyPutAvoided].load(), avoided_before + 1);
}

TEST(CASUploadDetached, ExistingBodyWithoutMetadataBackfillsWithoutPublication)
{
    const String payload = "mandatory-head-metadata-backfill";
    const BlobRef ref = idOf(payload);
    auto backend = std::make_shared<ProtocolRecordingBackend>();
    auto store = openUploadPool(backend);
    seedPresentBody(*backend, store->layout(), store->poolMeta(), ref, payload);
    auto build = precommitBuildFor(store, RootNamespace{"srv1/protocol-backfill"}, "part", payload);
    backend->watch(store->layout().blobKey(ref), store->layout().blobMetaKey(ref));

    const BlobUploadResult result = build->uploadBlobDetached(
        BlobUploadRequest{ref, BlobSource::fromString(payload), payload.size()});

    EXPECT_EQ(result.dep.proof, BlobDependencyProof::Materialized);
    ASSERT_FALSE(backend->operations.empty());
    EXPECT_EQ(backend->operations.front(), "head");
    EXPECT_EQ(backend->blob_heads, 1u);
    EXPECT_EQ(backend->meta_gets, 1u);
    EXPECT_EQ(backend->publish_calls, 0u);
    const auto meta = loadMetaForTest(*backend, store->layout(), u128Of(payload));
    ASSERT_TRUE(meta.has_value());
    EXPECT_EQ(meta->meta.state, MetaState::Clean);
    EXPECT_EQ(meta->meta.size, payload.size());
}

TEST(CASUploadDetached, AbsentBodyWithStaleCondemnedPublishesBeforeMetadataRead)
{
    const String payload = "mandatory-head-absent-stale-condemned";
    const BlobRef ref = idOf(payload);
    auto backend = std::make_shared<ProtocolRecordingBackend>();
    auto store = openUploadPool(backend);
    writeMetaClean(*backend, store->layout(), u128Of(payload), payload.size());
    condemnMeta(*backend, store->layout(), u128Of(payload), 17);
    auto build = precommitBuildFor(store, RootNamespace{"srv1/protocol-stale"}, "part", payload);
    backend->watch(store->layout().blobKey(ref), store->layout().blobMetaKey(ref));

    const BlobUploadResult result = build->uploadBlobDetached(
        BlobUploadRequest{ref, BlobSource::fromString(payload), payload.size()});

    EXPECT_EQ(result.dep.proof, BlobDependencyProof::Materialized);
    ASSERT_FALSE(backend->operations.empty());
    EXPECT_EQ(backend->operations.front(), "head");
    EXPECT_EQ(backend->blob_heads, 1u);
    EXPECT_EQ(backend->publish_calls, 1u);
    ASSERT_TRUE(backend->meta_gets_before_first_publish.has_value());
    EXPECT_EQ(*backend->meta_gets_before_first_publish, 0u);
    EXPECT_GT(backend->meta_gets, 0u) << "the stale marker is read only while reconciling after publication";
    EXPECT_EQ(metaStateAt(*backend, store->layout(), payload), std::optional<MetaState>(MetaState::Clean));
}

TEST(CASUploadDetached, PresentCondemnedPublishesFreshAndQueuedOldDeleteMisses)
{
    const String payload = "mandatory-head-present-condemned";
    const BlobRef ref = idOf(payload);
    auto backend = std::make_shared<ProtocolRecordingBackend>();
    auto store = openUploadPool(backend);
    seedPresentBody(*backend, store->layout(), store->poolMeta(), ref, payload);
    writeMetaClean(*backend, store->layout(), u128Of(payload), payload.size());
    condemnMeta(*backend, store->layout(), u128Of(payload), 19);
    auto build = precommitBuildFor(store, RootNamespace{"srv1/protocol-condemned"}, "part", payload);
    const String blob_key = store->layout().blobKey(ref);
    DB::Cas::tests::OperationForTest condemned_probe(*backend);
    const Etag condemned_token = (*condemned_probe).head(blob_key, Retry::standard())->etag;
    backend->watch(blob_key, store->layout().blobMetaKey(ref));
    const uint64_t avoided_before = ProfileEvents::global_counters[ProfileEvents::CASBlobBodyPutAvoided].load();

    const BlobUploadResult result = build->uploadBlobDetached(
        BlobUploadRequest{ref, BlobSource::fromString(payload), payload.size()});

    EXPECT_EQ(result.dep.proof, BlobDependencyProof::Materialized);
    ASSERT_FALSE(backend->operations.empty());
    EXPECT_EQ(backend->operations.front(), "head");
    EXPECT_EQ(backend->blob_heads, 1u);
    EXPECT_EQ(backend->publish_calls, 1u);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASBlobBodyPutAvoided].load(), avoided_before);
    {
        DB::Cas::tests::OperationForTest op(*backend);
        EXPECT_EQ((*op).remove(blob_key, condemned_token, Retry::once()), Removal::Mismatch);
        EXPECT_TRUE((*op).head(blob_key, Retry::standard()).has_value());
    }
}

/// A present body with absent metadata is observed and backfilled `Clean` without publication.
TEST(CASUploadDetached, PresentBodyWithoutMetadataBackfills)
{
    const RootNamespace ns{"srv1/nsAdopt"};
    const String ref_name = "part";
    const String payload = "head-miss-adopt-payload";
    const BlobRef blob = idOf(payload);

    auto arrange = [&](std::shared_ptr<InMemoryBackend> & b, PoolPtr & s, PartWriteTxnPtr & build)
    {
        b = std::make_shared<InMemoryBackend>();
        s = openUploadPool(b);
        seedPresentBody(*b, s->layout(), s->poolMeta(), blob, payload);   /// body present, no meta: backfill
        build = precommitBuildFor(s, ns, ref_name, payload);
    };

    std::shared_ptr<InMemoryBackend> b1;
    PoolPtr s1;
    PartWriteTxnPtr build1;
    arrange(b1, s1, build1);
    const String key = s1->layout().blobKey(blob);

    ASSERT_FALSE(metaStateAt(*b1, s1->layout(), payload).has_value());   /// precondition: meta absent
    EXPECT_EQ(build1->dependencyProof(blob), std::nullopt);

    const BlobUploadResult r = build1->uploadBlobDetached(
        BlobUploadRequest{blob, BlobSource::fromString(payload), payload.size()});

    EXPECT_EQ(r.diagnostics.action, BlobMaterializationAction::Observed);
    EXPECT_EQ(r.diagnostics.reason, std::nullopt);
    EXPECT_EQ(r.diagnostics.transport, std::nullopt);
    EXPECT_EQ(r.dep.proof, BlobDependencyProof::Materialized);
    EXPECT_EQ(r.dep.size, payload.size());

    EXPECT_EQ(build1->dependencyProof(blob), std::nullopt);
    /// The point-read backfilled a Clean meta.
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), std::optional<MetaState>(MetaState::Clean));

    std::shared_ptr<InMemoryBackend> b2;
    PoolPtr s2;
    PartWriteTxnPtr build2;
    arrange(b2, s2, build2);
    build2->putBlob(blob, BlobSource::fromString(payload));
    EXPECT_EQ(build2->dependencyProof(blob), BlobDependencyProof::Materialized);

    EXPECT_EQ(logicalPayloadAt(*b1, key, s1->poolMeta().blob_header_len),
              logicalPayloadAt(*b2, key, s2->poolMeta().blob_header_len));
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), metaStateAt(*b2, s2->layout(), payload));
}

/// Fresh local streaming: mandatory `HEAD` observes absence, then unconditional publication creates
/// the body and reconciles `Clean` metadata.
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

    ASSERT_FALSE(headPresent(*b1, key));   /// precondition: absent
    EXPECT_EQ(build1->dependencyProof(blob), std::nullopt);

    const BlobUploadResult r = build1->uploadBlobDetached(
        BlobUploadRequest{blob, BlobSource::fromString(payload), payload.size()});

    EXPECT_EQ(r.diagnostics.action, BlobMaterializationAction::Published);
    EXPECT_EQ(r.diagnostics.reason, BlobPublicationReason::Absent);
    EXPECT_EQ(r.diagnostics.transport, BlobPublicationTransport::Streaming);
    EXPECT_EQ(r.ref, blob);
    EXPECT_EQ(r.dep.proof, BlobDependencyProof::Materialized);
    EXPECT_EQ(r.dep.size, payload.size());

    EXPECT_EQ(build1->dependencyProof(blob), std::nullopt);
    EXPECT_TRUE(headPresent(*b1, key));
    EXPECT_EQ(logicalPayloadAt(*b1, key, s1->poolMeta().blob_header_len), payload);
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), std::optional<MetaState>(MetaState::Clean));

    std::shared_ptr<InMemoryBackend> b2;
    PoolPtr s2;
    PartWriteTxnPtr build2;
    arrange(b2, s2, build2);
    const PutBlobResult pr = build2->putBlob(blob, BlobSource::fromString(payload));
    EXPECT_EQ(build2->dependencyProof(blob), BlobDependencyProof::Materialized);
    EXPECT_EQ(pr.size, r.dep.size);

    /// The envelope's fresh incarnation tag differs per upload, but the LOGICAL payload and meta match.
    EXPECT_EQ(logicalPayloadAt(*b1, key, s1->poolMeta().blob_header_len),
              logicalPayloadAt(*b2, key, s2->poolMeta().blob_header_len));
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), metaStateAt(*b2, s2->layout(), payload));
}

/// A first absent observation with an S3-staged source selects verbatim native copy.
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
        createObj(*b, staging_key, staging_bytes);
        build = precommitBuildFor(s, ns, ref_name, payload);
    };

    std::shared_ptr<InMemoryBackend> b1;
    PoolPtr s1;
    PartWriteTxnPtr build1;
    String staging_bytes1;
    arrange(b1, s1, build1, staging_bytes1);
    const String key = s1->layout().blobKey(blob);

    ASSERT_FALSE(headPresent(*b1, key));
    EXPECT_EQ(build1->dependencyProof(blob), std::nullopt);

    const BlobUploadResult r = build1->uploadBlobDetached(
        BlobUploadRequest{
            blob,
            reReadableStagedSource(b1, staging_key, payload.size(), s1->poolMeta().blob_header_len),
            payload.size()});

    EXPECT_EQ(r.diagnostics.action, BlobMaterializationAction::Published);
    EXPECT_EQ(r.diagnostics.reason, BlobPublicationReason::Absent);
    EXPECT_EQ(r.diagnostics.transport, BlobPublicationTransport::ServerSideCopy);
    EXPECT_EQ(r.dep.proof, BlobDependencyProof::Materialized);
    EXPECT_EQ(r.dep.size, payload.size());

    EXPECT_EQ(build1->dependencyProof(blob), std::nullopt);
    ASSERT_TRUE(headPresent(*b1, key));
    /// The server-side copy moved the staging bytes verbatim to the blob key.
    const auto got = readObj(*b1, key);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, staging_bytes1);

    std::shared_ptr<InMemoryBackend> b2;
    PoolPtr s2;
    PartWriteTxnPtr build2;
    String staging_bytes2;
    arrange(b2, s2, build2, staging_bytes2);
    build2->putBlob(
        blob,
        reReadableStagedSource(b2, staging_key, payload.size(), s2->poolMeta().blob_header_len));
    EXPECT_EQ(build2->dependencyProof(blob), BlobDependencyProof::Materialized);

    const auto got2 = readObj(*b2, key);
    ASSERT_TRUE(got2.has_value());
    EXPECT_EQ(got->bytes, got2->bytes);
}

/// Condemned-local replacement: a present body observed condemned via the metadata point-read is displaced
/// by a fresh incarnation streamed from the writer's OWN source, never a read of the dying object.
/// Diagnostics are `Published` + `Condemned` + `Streaming`; the token changes and metadata returns to `Clean`.
TEST(CASUploadDetached, CondemnedLocalResurrection)
{
    const RootNamespace ns{"srv1/nsResLocal"};
    const String ref_name = "part";
    const String payload = "condemned-local-republish-payload";
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
    DB::Cas::tests::OperationForTest token_probe(*b1);
    const Etag condemned_token = (*token_probe).head(key, Retry::standard())->etag;

    ASSERT_EQ(metaStateAt(*b1, s1->layout(), payload), std::optional<MetaState>(MetaState::Condemned));
    EXPECT_EQ(build1->dependencyProof(blob), std::nullopt);

    const BlobUploadResult r = build1->uploadBlobDetached(
        BlobUploadRequest{blob, BlobSource::fromString(payload), payload.size()});

    EXPECT_EQ(r.diagnostics.action, BlobMaterializationAction::Published);
    EXPECT_EQ(r.diagnostics.reason, BlobPublicationReason::Condemned);
    EXPECT_EQ(r.diagnostics.transport, BlobPublicationTransport::Streaming);
    EXPECT_EQ(r.dep.proof, BlobDependencyProof::Materialized);
    EXPECT_EQ(r.dep.size, payload.size());

    EXPECT_EQ(build1->dependencyProof(blob), std::nullopt);
    /// The condemned incarnation was displaced by a fresh one (token changed) and the meta is Clean again.
    const Etag after_token = (*token_probe).head(key, Retry::standard())->etag;
    EXPECT_NE(after_token, condemned_token);
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), std::optional<MetaState>(MetaState::Clean));
    EXPECT_EQ(logicalPayloadAt(*b1, key, s1->poolMeta().blob_header_len), payload);

    std::shared_ptr<InMemoryBackend> b2;
    PoolPtr s2;
    PartWriteTxnPtr build2;
    arrange(b2, s2, build2);
    build2->putBlob(blob, BlobSource::fromString(payload));
    EXPECT_EQ(build2->dependencyProof(blob), BlobDependencyProof::Materialized);

    EXPECT_EQ(logicalPayloadAt(*b1, key, s1->poolMeta().blob_header_len),
              logicalPayloadAt(*b2, key, s2->poolMeta().blob_header_len));
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), metaStateAt(*b2, s2->layout(), payload));
}

/// Condemned-S3 replacement: a present body observed condemned with an S3 staging source is displaced
/// by an unconditional retagged stream from that writer-owned staging payload, never a read/copy of
/// the condemned blob key. Diagnostics are `Published` + `Condemned` + `Streaming`.
TEST(CASUploadDetached, CondemnedS3Resurrection)
{
    const RootNamespace ns{"srv1/nsResS3"};
    const String ref_name = "part";
    const String payload = "condemned-s3-republish-payload";
    const BlobRef blob = idOf(payload);
    const String staging_key = "p/staging/mount1/republish.tmp";

    auto arrange = [&](std::shared_ptr<InMemoryBackend> & b, PoolPtr & s, PartWriteTxnPtr & build)
    {
        b = std::make_shared<InMemoryBackend>();
        s = openUploadPool(b);
        EnvelopeHeader h;
        h.kind = ObjectKind::Blob;
        h.incarnation_tag = DB::UInt128(0xC0FFEE);
        const String staging_bytes = encodeEnvelopeHeader(h, static_cast<uint32_t>(s->poolMeta().blob_header_len)) + payload;
        createObj(*b, staging_key, staging_bytes);
        /// Seed the condemned blob body = exactly a verbatim promote of the staging object would produce.
        createObj(*b, s->layout().blobKey(blob), staging_bytes);
        writeMetaClean(*b, s->layout(), u128Of(payload), payload.size());
        condemnMeta(*b, s->layout(), u128Of(payload), /*condemn_round=*/9);
        build = precommitBuildFor(s, ns, ref_name, payload);
    };

    std::shared_ptr<InMemoryBackend> b1;
    PoolPtr s1;
    PartWriteTxnPtr build1;
    arrange(b1, s1, build1);
    const String key = s1->layout().blobKey(blob);
    DB::Cas::tests::OperationForTest token_probe(*b1);
    const Etag condemned_token = (*token_probe).head(key, Retry::standard())->etag;

    ASSERT_EQ(metaStateAt(*b1, s1->layout(), payload), std::optional<MetaState>(MetaState::Condemned));
    EXPECT_EQ(build1->dependencyProof(blob), std::nullopt);

    const BlobUploadResult r = build1->uploadBlobDetached(
        BlobUploadRequest{
            blob,
            reReadableStagedSource(b1, staging_key, payload.size(), s1->poolMeta().blob_header_len),
            payload.size()});

    EXPECT_EQ(r.diagnostics.action, BlobMaterializationAction::Published);
    EXPECT_EQ(r.diagnostics.reason, BlobPublicationReason::Condemned);
    EXPECT_EQ(r.diagnostics.transport, BlobPublicationTransport::Streaming);
    EXPECT_EQ(r.dep.proof, BlobDependencyProof::Materialized);
    EXPECT_EQ(r.dep.size, payload.size());

    EXPECT_EQ(build1->dependencyProof(blob), std::nullopt);
    /// A fresh incarnation displaced the condemned one (INV-NO-RETURN: fresh tag ⇒ different token).
    const Etag after_token = (*token_probe).head(key, Retry::standard())->etag;
    EXPECT_NE(after_token, condemned_token);
    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), std::optional<MetaState>(MetaState::Clean));

    std::shared_ptr<InMemoryBackend> b2;
    PoolPtr s2;
    PartWriteTxnPtr build2;
    arrange(b2, s2, build2);
    build2->putBlob(
        blob,
        reReadableStagedSource(b2, staging_key, payload.size(), s2->poolMeta().blob_header_len));
    EXPECT_EQ(build2->dependencyProof(blob), BlobDependencyProof::Materialized);

    EXPECT_EQ(metaStateAt(*b1, s1->layout(), payload), metaStateAt(*b2, s2->layout(), payload));
}

/// `mergeBlobUploadResults` folds N detached results in ONE call to EXACTLY the same deps a serial
/// putBlob fold would produce. Both worlds run the identical sequence of backend calls (same
/// precommit and same blobs in the same order). The merge path adds no backend calls of its own, only
/// in-memory bookkeeping, so a deep dependency-map comparison is exact.
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
        EXPECT_EQ(build1->dependencyProof(blob), std::nullopt);
        results.push_back(build1->uploadBlobDetached(
            BlobUploadRequest{blob, BlobSource::fromString(payload), payload.size()}));
    }
    /// Still untouched before the merge -- uploadBlobDetached folds nothing.
    for (const auto & payload : payloads)
        EXPECT_EQ(build1->dependencyProof(idOf(payload)), std::nullopt);

    build1->mergeBlobUploadResults(results);

    for (const auto & payload : payloads)
        EXPECT_EQ(build1->dependencyProof(idOf(payload)), BlobDependencyProof::Materialized);

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
    ASSERT_EQ(build->dependencyProof(idOf(payload_existing)), BlobDependencyProof::Materialized);

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
    EXPECT_EQ(build->dependencyProof(idOf(payload_a)), std::nullopt);
    EXPECT_EQ(build->dependencyProof(idOf(payload_b)), std::nullopt);
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
    ASSERT_EQ(build->dependencyProof(blob), std::nullopt);

    BlobUploadResult conflicting = r;
    conflicting.dep.size = r.dep.size + 1;   /// same ref, conflicting declared size

    const auto pre_merge_snapshot = build->depsSnapshotForTest();

    expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&]
    {
        build->mergeBlobUploadResults(std::vector<BlobUploadResult>{r, conflicting});
    });

    EXPECT_EQ(build->depsSnapshotForTest(), pre_merge_snapshot);
    EXPECT_EQ(build->dependencyProof(blob), std::nullopt);
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
