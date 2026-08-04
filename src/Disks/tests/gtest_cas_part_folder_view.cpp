#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartFolderAccess.h>
#include <gtest/gtest.h>

using namespace DB;

TEST(CASPartRefKey, CacheKeyIsUnambiguous)
{
    /// Refs may contain '/' (the `detached/<part>` fold, B181); the '\0' join keeps
    /// (ns="a", ref="b/c") distinct from (ns="a/b", ref="c").
    const Cas::PartRefKey k1{Cas::RootNamespace{"a"}, "b/c"};
    const Cas::PartRefKey k2{Cas::RootNamespace{"a/b"}, "c"};
    EXPECT_NE(k1.cacheKey(), k2.cacheKey());
    EXPECT_FALSE(k1 == k2);
    EXPECT_TRUE((k1 == Cas::PartRefKey{Cas::RootNamespace{"a"}, "b/c"}));
}

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <algorithm>

namespace
{

using namespace DB;

std::shared_ptr<const Cas::PartFolderView> makeView()
{
    auto manifest = std::make_shared<Cas::PartManifest>();
    auto add = [&](const char * path, Cas::EntryPlacement placement, const char * bytes, uint64_t blob_size)
    {
        Cas::ManifestEntry e;
        e.path = path;
        e.placement = placement;
        e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(UInt128(manifest->entries.size() + 1))};

        e.blob_size = blob_size;
        e.inline_bytes = bytes;
        manifest->entries.push_back(e);
    };
    /// Canonical (sorted) order — the ctor chasserts it. All-tree-part-files Task 9: `txn_version.txt`
    /// is an ordinary Inline entry now, not a separate mutable payload.
    add("checksums.txt", Cas::EntryPlacement::Inline, "cs", 2);
    add("data.bin", Cas::EntryPlacement::Blob, "", 100);
    add("p.proj/checksums.txt", Cas::EntryPlacement::Inline, "pc", 2);
    add("p.proj/data.bin", Cas::EntryPlacement::Blob, "", 50);
    add("txn_version.txt", Cas::EntryPlacement::Inline, "ver", 3);

    return std::make_shared<const Cas::PartFolderView>(
        Cas::PartRefKey{Cas::RootNamespace{"srv/t"}, "part_1"},
        Cas::ManifestId{Cas::RootNamespace{"srv/t"}, Cas::ManifestRef{1, 2, 3}},
        /*manifest_size=*/1000, manifest,
        /*validated_at_ms=*/42);
}

std::vector<String> sorted(std::vector<String> v) { std::sort(v.begin(), v.end()); return v; }

}

TEST(CASPartFolderView, FindFileAndHasFile)
{
    auto v = makeView();
    ASSERT_NE(v->findFile("data.bin"), nullptr);
    EXPECT_EQ(v->findFile("data.bin")->blob_size, 100u);
    EXPECT_EQ(v->findFile("absent.bin"), nullptr);
    EXPECT_TRUE(v->hasFile("p.proj/data.bin"));
    EXPECT_TRUE(v->hasFile("txn_version.txt"));      /// an ordinary Inline entry
    EXPECT_FALSE(v->hasFile("p.proj"));              /// a directory, not a file
}

TEST(CASPartFolderView, ListChildrenCollapsesFirstComponent)
{
    auto v = makeView();
    EXPECT_EQ(sorted(v->listChildren("")),
              sorted({"checksums.txt", "data.bin", "p.proj", "txn_version.txt"}));
    EXPECT_EQ(sorted(v->listChildren("p.proj/")), sorted({"checksums.txt", "data.bin"}));
    EXPECT_TRUE(v->listChildren("q.proj/").empty());
}

TEST(CASPartFolderView, HasDirectory)
{
    auto v = makeView();
    EXPECT_TRUE(v->hasDirectory("p.proj/"));
    EXPECT_FALSE(v->hasDirectory("q.proj/"));
}

TEST(CASPartFolderView, SizesAndBytes)
{
    auto v = makeView();
    EXPECT_EQ(v->fileSize("checksums.txt"), std::optional<uint64_t>(2));   /// inline: bytes size
    EXPECT_EQ(v->fileSize("data.bin"), std::optional<uint64_t>(100));      /// blob: blob_size
    EXPECT_EQ(v->fileSize("txn_version.txt"), std::optional<uint64_t>(3)); /// inline: bytes size
    EXPECT_EQ(v->fileSize("absent"), std::nullopt);
    EXPECT_EQ(v->inlineBytes("checksums.txt"), std::optional<String>("cs"));
    EXPECT_EQ(v->inlineBytes("data.bin"), std::nullopt);                   /// blob has no inline bytes
    EXPECT_EQ(v->inlineBytes("txn_version.txt"), std::optional<String>("ver"));
    EXPECT_GE(v->estimatedBytes(), 1000u);                                 /// >= manifest_size
}

TEST(CASPartFolderView, ProjectionDirPrefixRecognizer)
{
    using V = Cas::PartFolderView;
    EXPECT_EQ(V::projectionDirPrefix("p.proj"), std::optional<std::string>("p.proj/"));
    EXPECT_EQ(V::projectionDirPrefix("a/b.tmp_proj"), std::optional<std::string>("a/b.tmp_proj/"));
    EXPECT_EQ(V::projectionDirPrefix("data.bin"), std::nullopt);
    EXPECT_EQ(V::projectionDirPrefix(""), std::nullopt);
}
