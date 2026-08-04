#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartPathParser.h>
#include "cas_test_helpers.h"

using namespace DB::Cas;

namespace
{
/// A `BlobRef` at `algo` whose first bytes are `0x00, 0xaa, 0xbb` (the rest zero) -- for key-shape
/// tests that need a stable, recognizable hex prefix. `Layout` no longer captures an algo (Phase 3
/// T2/T3): every blob key is built from a `BlobRef` alone, so key-shape tests construct one directly.
BlobRef prefixedRef(BlobHashAlgo algo)
{
    BlobDigest d{};
    d.bytes[0] = 0x00; d.bytes[1] = 0xaa; d.bytes[2] = 0xbb;
    return BlobRef{algo, d};
}
}

TEST(CASLayout, KeyShapes)
{
    /// Per design §10 EVERY algo carries an explicit path segment: `blobs/ch128/...`, not the legacy
    /// `blobs/...`.
    Layout l{"p"};
    const BlobRef ref = prefixedRef(BlobHashAlgo::CityHash128);
    const String hex = codecFor(BlobHashAlgo::CityHash128).toHex(ref.digest);
    EXPECT_EQ(l.blobKey(ref), "p/blobs/ch128/" + hex.substr(0, 2) + "/" + hex);
    EXPECT_EQ(l.gcStateKey(), "p/gc/state");
    EXPECT_EQ(l.outcomesKey(4, 42, 7, 1), "p/gc/gen/4/attempt/42/outcomes/7/1.zst");
    EXPECT_EQ(l.poolMetaKey(), "p/_pool_meta");
}

TEST(CASLayout, BlobKeyCarriesAlgoSegment)
{
    /// Every algo gets its own segment (design §3/§10), so two algos can never collide in the key
    /// space even after a config change on a fresh pool. `Layout` itself carries no algo anymore --
    /// the segment comes from the `BlobRef` passed to `blobKey`/`blobMetaKey`.
    const Layout l("p");

    const BlobRef ch128_ref = prefixedRef(BlobHashAlgo::CityHash128);
    const String ch128_hex = codecFor(BlobHashAlgo::CityHash128).toHex(ch128_ref.digest);
    EXPECT_EQ(l.blobKey(ch128_ref), "p/blobs/ch128/" + ch128_hex.substr(0, 2) + "/" + ch128_hex);
    EXPECT_EQ(l.blobMetaKey(ch128_ref), l.blobKey(ch128_ref) + ".meta");

    const BlobRef xxh3_ref = prefixedRef(BlobHashAlgo::XXH3_128);
    const String xxh3_hex = codecFor(BlobHashAlgo::XXH3_128).toHex(xxh3_ref.digest);
    EXPECT_EQ(l.blobKey(xxh3_ref), "p/blobs/xxh3/" + xxh3_hex.substr(0, 2) + "/" + xxh3_hex);
    EXPECT_EQ(l.blobMetaKey(xxh3_ref), l.blobKey(xxh3_ref) + ".meta");

    const BlobRef sha256_ref = prefixedRef(BlobHashAlgo::Sha256);
    const String sha256_hex = codecFor(BlobHashAlgo::Sha256).toHex(sha256_ref.digest);
    EXPECT_EQ(l.blobKey(sha256_ref), "p/blobs/sha256/" + sha256_hex.substr(0, 2) + "/" + sha256_hex);

    /// Trees/manifests/refs are UNCHANGED -- only blob-body keys gain the algo segment.
    EXPECT_EQ(l.blobsPrefix(), "p/blobs/");
}

TEST(CASLayout, RootNamespaceKeys)
{
    Layout l("p");
    RootNamespace ns{"srv1/3f2e-uuid"};
    const NamespaceLifeId ns_id = DB::Cas::tests::fixture::fixtureLife(ns);
    EXPECT_EQ(l.namespaceStreamPrefix(ns_id),
        "p/cas/ns/stream/" + renderIncarnation(ns_id.incarnation) + "/");
    EXPECT_EQ(l.namespaceFileKey(ns_id, "format_version.txt"),
        "p/cas/ns/state/" + renderIncarnation(ns_id.incarnation) + "/_files/format_version.txt");
    EXPECT_EQ(l.namespaceFilesPrefix(ns_id),
        "p/cas/ns/state/" + renderIncarnation(ns_id.incarnation) + "/_files/");
}

TEST(CASLayout, OpaqueLifeIdSeparatesStreamFromState)
{
    /// This catches a builder that accidentally puts the logical namespace back into a life-owned
    /// key. The two different names deliberately share one physical id: object identity is the id,
    /// while the name remains catalog-only.
    Layout l("p");
    const UInt128 life_id = UInt128(0x1234);
    const NamespaceLifeId first = NamespaceLifeId::fromCatalogEntry(RootNamespace{"root/first"}, life_id);
    const NamespaceLifeId second = NamespaceLifeId::fromCatalogEntry(RootNamespace{"root/second"}, life_id);
    const RefTxnId txn{7, 9};

    EXPECT_EQ(l.namespaceStreamPrefix(first), "p/cas/ns/stream/00000000000000000000000000001234/");
    EXPECT_EQ(l.namespaceStatePrefix(first), "p/cas/ns/state/00000000000000000000000000001234/");
    EXPECT_EQ(l.refLogKey(first, txn), "p/cas/ns/stream/00000000000000000000000000001234/_log/0000000000000007-0000000000000009.zst");
    EXPECT_EQ(l.refSnapshotKey(first, txn), "p/cas/ns/stream/00000000000000000000000000001234/_snap/0000000000000007-0000000000000009.zst");
    EXPECT_EQ(l.refCkptKey(first), "p/cas/ns/state/00000000000000000000000000001234/_ckpt");
    EXPECT_EQ(l.namespaceFileKey(first, "nested/file"), "p/cas/ns/state/00000000000000000000000000001234/_files/nested/file");

    EXPECT_EQ(l.refLogKey(second, txn), l.refLogKey(first, txn));
    EXPECT_EQ(l.namespaceFileKey(second, "nested/file"), l.namespaceFileKey(first, "nested/file"));
}

TEST(CASLayout, RelocatedRefAndManifestKeys)
{
    Layout l("p");
    const RootNamespace ns{"srid/store/ab/uuid@cas@"};
    const NamespaceLifeId ns_id = DB::Cas::tests::fixture::fixtureLife(ns);
    EXPECT_EQ(l.namespaceStreamPrefix(ns_id),
        "p/cas/ns/stream/" + renderIncarnation(ns_id.incarnation) + "/");
    EXPECT_EQ(l.casRefsPrefix(), "p/cas/ns/stream/");
    /// All manifests of a namespace: cas/manifests/<ns>/ (replaces roots/<ns>/_manifests/).
    EXPECT_EQ(l.manifestNamespacePrefix(ns), "p/cas/manifests/srid/store/ab/uuid@cas@/");

    /// manifestKey: canonical hex build directory, under cas/manifests/<ns>/ (no /_manifests/ infix).
    ManifestId id;
    id.root_namespace = ns;
    id.ref.writer_epoch = 1;
    id.ref.build_sequence = 1042;
    id.ref.manifest_ordinal = 1;
    const String key = l.manifestKey(id);
    EXPECT_EQ(key, "p/cas/manifests/srid/store/ab/uuid@cas@/"
        "0000000000000001-0000000000000412/000001.zst");
    EXPECT_EQ(key.find("/_manifests/"), String::npos) << key;
}

TEST(CASLayout, RootNamespaceValidation)
{
    Layout l("p");
    /// Opaque physical life keys deliberately do not inspect the logical namespace. Namespace-bearing
    /// families such as manifests remain responsible for validating it.
    EXPECT_THROW(l.manifestNamespacePrefix(RootNamespace{""}), DB::Exception);
    EXPECT_THROW(l.manifestNamespacePrefix(RootNamespace{"/lead"}), DB::Exception);
    EXPECT_THROW(l.manifestNamespacePrefix(RootNamespace{"trail/"}), DB::Exception);
    /// File names may be NESTED relative paths (M-W T2: deduplication_logs/...); only unclean
    /// shapes are rejected (empty, leading/trailing '/', empty segments, '..' escapes).
    const NamespaceLifeId ok_id = DB::Cas::tests::fixture::fixtureLife(RootNamespace{"ok"});
    EXPECT_NO_THROW(l.namespaceFileKey(ok_id, "a/b"));
    EXPECT_THROW(l.namespaceFileKey(ok_id, ""), DB::Exception);
    EXPECT_THROW(l.namespaceFileKey(ok_id, "/lead"), DB::Exception);
    EXPECT_THROW(l.namespaceFileKey(ok_id, "trail/"), DB::Exception);
    EXPECT_THROW(l.namespaceFileKey(ok_id, "a//b"), DB::Exception);
    EXPECT_THROW(l.namespaceFileKey(ok_id, "../up"), DB::Exception);
    EXPECT_THROW(l.namespaceFileKey(ok_id, "a/../b"), DB::Exception);

    EXPECT_THROW(l.manifestNamespacePrefix(RootNamespace{"a//b"}), DB::Exception);
    EXPECT_THROW(l.manifestNamespacePrefix(RootNamespace{"srv1/_files/x"}), DB::Exception);
    EXPECT_NO_THROW(l.manifestNamespacePrefix(RootNamespace{"my_files/tbl"}));
}

TEST(CASLayout, GenerationAndRootsKeys)
{
    Layout l("p");
    /// rev. 15: gc/snap is gone; generations carry write-once seals + blob-target / cleanup runs.
    /// rev. 16: every per-round artifact is attempt-scoped under gc/gen/<gen>/attempt/<attempt>/.
    EXPECT_EQ(l.foldSealKey(12, 0), "p/gc/gen/12/attempt/0/fold_seal");
    EXPECT_EQ(l.blobTargetRunKey(12, 0, 0, 0), "p/gc/gen/12/attempt/0/blob_target/0/0");
    EXPECT_EQ(l.namespaceRootPrefix(), "p/cas/ns/");
    EXPECT_EQ(l.rootsPrefix(), "p/roots/");
}

TEST(CASLayout, AttemptScopedGenKeys)
{
    DB::Cas::Layout layout("p");
    EXPECT_EQ(layout.foldSealKey(4, 42), "p/gc/gen/4/attempt/42/fold_seal");
    EXPECT_EQ(layout.blobTargetRunKey(4, 42, 3, 0), "p/gc/gen/4/attempt/42/blob_target/3/0");
    EXPECT_EQ(layout.outcomesKey(5, 42, 7, 3), "p/gc/gen/5/attempt/42/outcomes/7/3.zst");
    EXPECT_EQ(layout.gcGenPrefix(4), "p/gc/gen/4/");
    EXPECT_EQ(layout.gcGenAttemptPrefix(4, 42), "p/gc/gen/4/attempt/42/");
}

TEST(CASLayout, RegistryDeletedGcDiscoveryViaList)
{
    /// Task 4: the namespace registry (`gc/registry`) is deleted; discovery authority moved to LIST.
    /// The `_registry` namespace segment is not reserved (it was only reserved while the registry lived
    /// under `roots/_registry`, which was already relocated to `gc/registry` before being deleted).
    Layout l("p");
    EXPECT_NO_THROW(l.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(RootNamespace{"a/_registry@cas@"})));
    /// Opaque stream keys are independent of namespace-segment reservations.
    EXPECT_NO_THROW(l.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(RootNamespace{"a/_files"})));
}

TEST(CASLayout, CasArchiveSuffixConstant)
{
    EXPECT_EQ(DB::Cas::kCasArchiveSuffix, "@cas@");
}

TEST(CASVfsPaths, MirroredArchiveNamespace)
{
    using DB::Cas::mirroredArchiveNamespace;
    /// Atomic: bare uuid -> store/<u3>/<uuid>@cas@
    EXPECT_EQ(mirroredArchiveNamespace("3f2a0000-0000-0000-0000-000000000001"),
              "store/3f2/3f2a0000-0000-0000-0000-000000000001@cas@");
    /// Non-Atomic: a full data/db/tbl path is used verbatim, @cas@ appended to the last segment.
    EXPECT_EQ(mirroredArchiveNamespace("data/mydb/events"),
              "data/mydb/events@cas@");
}

TEST(CASLayout, ManifestKeyShape)
{
    Layout l("p");
    ManifestId id;
    id.root_namespace = RootNamespace("srv-a/3f2e-uuid@cas@");
    id.ref.writer_epoch = 7;
    id.ref.build_sequence = 1042;
    id.ref.manifest_ordinal = 1;
    const String key = l.manifestKey(id);
    EXPECT_EQ(key,
        "p/cas/manifests/srv-a/3f2e-uuid@cas@/"
        "0000000000000007-0000000000000412/000001.zst");
}

TEST(CASLayout, ManifestsSegmentReserved)
{
    Layout l("p");
    ManifestId bad;
    bad.root_namespace = RootNamespace("srv-a/_manifests/x");
    EXPECT_THROW(l.manifestKey(bad), DB::Exception);
    /// Opaque life prefixes ignore the logical spelling; manifests still enforce the reservation.
    EXPECT_NO_THROW(l.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(RootNamespace{"srv-a/_manifests/tbl"})));
    EXPECT_NO_THROW(l.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(RootNamespace{"my_manifests/tbl"})));
}

TEST(CASLayout, ManifestKeyHexRoundTrip)
{
    Layout l("p");
    ManifestId id;
    id.root_namespace = RootNamespace("srv-a/3f2e-uuid@cas@");
    id.ref.writer_epoch = 7;
    id.ref.build_sequence = 0x8e;
    id.ref.manifest_ordinal = 42;
    const String key = l.manifestKey(id);
    EXPECT_EQ(key,
        "p/cas/manifests/srv-a/3f2e-uuid@cas@/"
        "0000000000000007-000000000000008e/000042.zst");

    const auto parsed = l.parseManifestKey(key);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->root_namespace, id.root_namespace);
    EXPECT_EQ(parsed->ref, id.ref);

    /// The old two-directory decimal shape (`<writer_epoch>/<build_sequence>/<ordinal>.zst`) is no
    /// longer canonical: the segment right before the file is a plain decimal number, not two
    /// fixed-width hex fields joined by '-', so `parseRefTxnId` rejects it.
    EXPECT_FALSE(l.parseManifestKey("p/cas/manifests/srv-a/3f2e-uuid@cas@/7/142/000042.zst").has_value());
    /// Foreign prefix, missing build segment, non-registered-suffix file, and out-of-range ordinal
    /// are all rejected.
    EXPECT_FALSE(l.parseManifestKey("p/cas/refs/srv-a/3f2e-uuid@cas@/"
        "0000000000000007-000000000000008e/000042.zst").has_value());
    EXPECT_FALSE(l.parseManifestKey("p/cas/manifests/0000000000000007-000000000000008e/000042.zst").has_value());
    EXPECT_FALSE(l.parseManifestKey("p/cas/manifests/srv-a/3f2e-uuid@cas@/"
        "0000000000000007-000000000000008e/000042.bin").has_value());
    EXPECT_FALSE(l.parseManifestKey("p/cas/manifests/srv-a/3f2e-uuid@cas@/"
        "0000000000000007-000000000000008e/000000.zst").has_value());
    EXPECT_FALSE(l.parseManifestKey("p/cas/manifests/srv-a/3f2e-uuid@cas@/"
        "0000000000000007-000000000000008E/000042.zst").has_value());   /// uppercase hex
}

TEST(CASLayout, RefObjectKeyRoundTrips)
{
    Layout l("p");
    const RootNamespace ns{"srv1/tbl@cas@"};
    const NamespaceLifeId ns_id = DB::Cas::tests::fixture::fixtureLife(ns);
    const RefTxnId id{7, 0x8e};
    const String life = "p/cas/ns/stream/" + renderIncarnation(ns_id.incarnation) + "/";

    const String log_key = l.refLogKey(ns_id, id);
    EXPECT_EQ(log_key, life + "_log/0000000000000007-000000000000008e.zst");
    const auto parsed_log = l.parseRefObjectKey(log_key);
    ASSERT_TRUE(parsed_log.has_value());
    EXPECT_EQ(parsed_log->life_id, ns_id.incarnation);
    EXPECT_EQ(parsed_log->kind, RefObjectKind::Log);
    EXPECT_EQ(parsed_log->txn_id, id);

    const String snap_key = l.refSnapshotKey(ns_id, id);
    EXPECT_EQ(snap_key, life + "_snap/0000000000000007-000000000000008e.zst");
    const auto parsed_snap = l.parseRefObjectKey(snap_key);
    ASSERT_TRUE(parsed_snap.has_value());
    EXPECT_EQ(parsed_snap->life_id, ns_id.incarnation);
    EXPECT_EQ(parsed_snap->kind, RefObjectKind::Snap);
    EXPECT_EQ(parsed_snap->txn_id, id);

}

TEST(CASLayout, RefObjectKeyLexicalOrder)
{
    Layout l("p");
    const NamespaceLifeId ns_id = DB::Cas::tests::fixture::fixtureLife(RootNamespace{"srv1/tbl@cas@"});
    const RefTxnId id{7, 0x8e};
    EXPECT_LT(l.refLogKey(ns_id, id), l.refSnapshotKey(ns_id, id));
}

TEST(CASLayout, ParseRefObjectKeyRejections)
{
    Layout l("p");
    const RootNamespace ns{"srv1/tbl@cas@"};
    const NamespaceLifeId ns_id = DB::Cas::tests::fixture::fixtureLife(ns);
    const RefTxnId id{7, 0x8e};
    const String log_key = l.refLogKey(ns_id, id);
    const String snap_key = l.refSnapshotKey(ns_id, id);

    /// Foreign top-level prefix.
    EXPECT_FALSE(l.parseRefObjectKey("p/cas/manifests/srv1/tbl@cas@/_log/" + renderRefTxnId(id)).has_value());
    /// Unknown kind directory (also covers the removed numeric-shard ref-key shape, which has no kind dir).
    EXPECT_FALSE(l.parseRefObjectKey("p/cas/ns/stream/00000000000000000000000000000001/_bogus/" + renderRefTxnId(id)).has_value());
    EXPECT_FALSE(l.parseRefObjectKey(l.namespaceStreamPrefix(ns_id) + "3").has_value());
    /// Uppercase hex and a short id are non-canonical RefTxnId renders. The id is judged BEFORE the
    /// life segment, so these stay "not ours" rather than becoming an incarnation refusal.
    EXPECT_FALSE(l.parseRefObjectKey(l.namespaceStreamPrefix(ns_id) + "_log/"
        "0000000000000007-000000000000008E").has_value());
    EXPECT_FALSE(l.parseRefObjectKey(l.namespaceStreamPrefix(ns_id) + "_log/7-8e").has_value());
    /// `_snap` without its stored suffix, and WITH a stray one, are both rejected. The suffix is taken
    /// from the registry rather than spelled out: it was `.proto` when this test was written and is
    /// `.zst` today, and stripping the wrong number of characters would have tested nothing.
    const String snap_suffix{storedSuffix(FormatId::RefSnapshot)};
    EXPECT_FALSE(l.parseRefObjectKey(snap_key.substr(0, snap_key.size() - snap_suffix.size())).has_value());
    EXPECT_FALSE(l.parseRefObjectKey(log_key + ".proto").has_value());
    /// Trailing garbage after the id.
    EXPECT_FALSE(l.parseRefObjectKey(log_key + "/extra").has_value());
    EXPECT_FALSE(l.parseRefObjectKey(snap_key + "/extra").has_value());
    /// Missing namespace segment entirely.
    EXPECT_FALSE(l.parseRefObjectKey("p/cas/ns/stream/_log/" + renderRefTxnId(id)).has_value());
    /// The `_ckpt` (spec INV-4) has no kind directory and no transaction id, so the id-bearing parser
    /// must not claim it. Every sweep over the ref prefix has to consult `parseRefCkptKey` as well --
    /// `groupRefKeys` treats a key neither parser recognizes as corruption that aborts ref folding.
    EXPECT_FALSE(l.parseRefObjectKey(l.refCkptKey(ns_id)).has_value());
}

/// Stage A task 5 (spec INV-4): `refCkptKey` and `parseRefCkptKey` are inverses, and the `_ckpt`
/// parser is exactly as strict as its id-bearing sibling -- it claims OUR checkpoint keys and nothing
/// else. A key that is not one of ours at all still yields `std::nullopt` rather than an exception,
/// for the same reason `parseRefObjectKey` does: classifying an untrusted listed key is an ordinary
/// "is this ours" question. Refusal is reserved for a key that IS ours but names no life --
/// `gtest_cas_ref_namespace_id.cpp` owns that half.
TEST(CASLayout, RefCkptKeyRoundTripsAndRejectsEverythingElse)
{
    Layout l("p");
    const RootNamespace ns{"srv1/tbl@cas@"};
    const NamespaceLifeId ns_id = DB::Cas::tests::fixture::fixtureLife(ns);
    const RefTxnId id{7, 0x8e};

    /// The state prefix plus the bare leaf, with no compression suffix (the format is raw), so the key
    /// is exactly `cas/ns/state/<life_id>/_ckpt`.
    EXPECT_EQ(l.refCkptKey(ns_id), l.namespaceStatePrefix(ns_id) + "_ckpt");
    EXPECT_EQ(l.parseRefCkptKey(l.refCkptKey(ns_id)), ns_id.incarnation);
    const NamespaceLifeId deep = DB::Cas::tests::fixture::fixtureLife(RootNamespace{"a/b/c"});
    EXPECT_EQ(l.parseRefCkptKey(l.refCkptKey(deep)), deep.incarnation);

    /// Foreign pool prefix.
    EXPECT_FALSE(l.parseRefCkptKey("q/cas/ns/state/00000000000000000000000000000001/_ckpt").has_value());
    /// The two id-bearing kinds are not checkpoints.
    EXPECT_FALSE(l.parseRefCkptKey(l.refLogKey(ns_id, id)).has_value());
    EXPECT_FALSE(l.parseRefCkptKey(l.refSnapshotKey(ns_id, id)).has_value());
    /// A suffix the registry does not put there, and trailing garbage.
    EXPECT_FALSE(l.parseRefCkptKey(l.refCkptKey(ns_id) + ".zst").has_value());
    EXPECT_FALSE(l.parseRefCkptKey(l.refCkptKey(ns_id) + "/extra").has_value());
    /// A near-miss leaf name.
    EXPECT_FALSE(l.parseRefCkptKey(l.namespaceStatePrefix(ns_id) + "_ckp").has_value());
    EXPECT_FALSE(l.parseRefCkptKey(l.namespaceStatePrefix(ns_id) + "_ckpt2").has_value());
    /// Missing namespace segment entirely.
    EXPECT_FALSE(l.parseRefCkptKey("p/cas/ns/state/_ckpt").has_value());
    /// The mirror of the rejection above: `_ckpt` is not a canonical `RefTxnId` render, so a key that
    /// puts it inside a kind directory is claimed by NEITHER parser.
    EXPECT_FALSE(l.parseRefObjectKey(l.namespaceStreamPrefix(ns_id) + "_log/_ckpt").has_value());
    /// The same key used to be READ by this parser as the checkpoint of a phantom namespace named
    /// `srv1/tbl@cas@/<inc>/_log`, because a namespace is an OPAQUE multi-segment string and nothing
    /// distinguished a deeper real namespace from a shallower one with a stray segment. The life
    /// segment closes that: `_log` is not a canonical incarnation, so the key is now REFUSED instead
    /// of quietly naming a table that cannot exist.
    EXPECT_FALSE(l.parseRefCkptKey(l.namespaceStatePrefix(ns_id) + "_log/_ckpt").has_value());
}

/// C3: blobKey/parseBlobKey are inverses; pins the grammar before relocating the definitions
/// from CasPartWriteTxn.cpp to CasLayout.cpp (relocation must not change a single byte of output).
TEST(CASLayout, BlobKeyRoundTripsThroughParse)
{
    DB::Cas::Layout layout("pool0");
    const DB::Cas::BlobRef ref{DB::Cas::BlobHashAlgo::XXH3_128,
                               DB::Cas::codecFor(DB::Cas::BlobHashAlgo::XXH3_128).fromHex(std::string(32, 'a'))};
    const String body = layout.blobKey(ref);
    const String meta = layout.blobMetaKey(ref);
    EXPECT_EQ(meta, body + ".meta");

    auto parsed_body = layout.parseBlobKey(body);
    auto parsed_meta = layout.parseBlobKey(meta);   /// body and .meta parse to the SAME BlobRef
    ASSERT_TRUE(parsed_body.has_value());
    ASSERT_TRUE(parsed_meta.has_value());
    EXPECT_EQ(*parsed_body, ref);
    EXPECT_EQ(*parsed_meta, ref);
    EXPECT_FALSE(layout.parseBlobKey("pool0/blobs/unknown-algo/aa/aa00").has_value());  /// foreign => nullopt
}
