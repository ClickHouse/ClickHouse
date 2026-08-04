#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>

using namespace DB::Cas;

namespace
{

ManifestRef ref(uint64_t w, uint64_t seq, uint64_t m)
{
    return ManifestRef{w, seq, static_cast<uint32_t>(m)};
}

ManifestId id(const char * ns, uint64_t w, uint64_t seq, uint64_t m)
{
    return ManifestId{RootNamespace(ns), ref(w, seq, m)};
}

}

TEST(CASManifestId, RefEqualityAndOrdering)
{
    EXPECT_EQ(ref(1, 2, 3), ref(1, 2, 3));
    EXPECT_NE(ref(1, 2, 3), ref(1, 2, 4));
    /// Strict total order: distinct by manifest_ordinal, then build_sequence, then writer_epoch.
    EXPECT_LT(ref(1, 2, 3), ref(1, 2, 4));
    EXPECT_LT(ref(1, 2, 9), ref(1, 3, 0));
    EXPECT_LT(ref(1, 9, 9), ref(2, 0, 0));
    EXPECT_FALSE(ref(1, 2, 3) < ref(1, 2, 3));
}

TEST(CASManifestId, IdIsNamespaceQualified)
{
    /// Same ref tuple, different namespace => DIFFERENT ids (the SabotageKeyByRefNotId guard).
    EXPECT_NE(id("nsA", 1, 1, 1), id("nsB", 1, 1, 1));
    EXPECT_EQ(id("nsA", 1, 1, 1), id("nsA", 1, 1, 1));
    /// Ordering separates by namespace first.
    EXPECT_LT(id("nsA", 9, 9, 9), id("nsB", 0, 0, 0));
}

TEST(CASManifestId, UsableAsMapAndSetKey)
{
    std::set<ManifestId> s;
    s.insert(id("nsA", 1, 1, 1));
    s.insert(id("nsB", 1, 1, 1));   /// distinct namespace -> distinct key
    s.insert(id("nsA", 1, 1, 1));   /// duplicate -> no growth
    EXPECT_EQ(s.size(), 2u);

    std::map<ManifestRef, int> m;
    m[ref(1, 1, 1)] = 10;
    m[ref(1, 1, 2)] = 20;
    EXPECT_EQ(m.size(), 2u);
    EXPECT_EQ(m[ref(1, 1, 1)], 10);
}

TEST(CASManifestId, UsableInUnorderedContainers)
{
    /// std::hash<ManifestRef> / std::hash<ManifestId> let the read-path cache (Phase 1c) and GC use
    /// unordered_map/set. Equal values => equal hash; distinct values => (overwhelmingly) distinct.
    std::unordered_set<ManifestId> s;
    s.insert(id("nsA", 1, 1, 1));
    s.insert(id("nsB", 1, 1, 1));   /// distinct namespace -> distinct key
    s.insert(id("nsA", 1, 1, 1));   /// duplicate -> no growth
    EXPECT_EQ(s.size(), 2u);

    std::unordered_map<ManifestRef, int> m;
    m[ref(1, 1, 1)] = 10;
    m[ref(1, 1, 1)] = 11;           /// same key overwrites
    m[ref(1, 1, 2)] = 20;
    EXPECT_EQ(m.size(), 2u);
    EXPECT_EQ(m.at(ref(1, 1, 1)), 11);

    EXPECT_EQ(std::hash<ManifestId>{}(id("nsA", 1, 1, 1)), std::hash<ManifestId>{}(id("nsA", 1, 1, 1)));
}

TEST(CASManifestId, ManifestOrdinalFileName)
{
    EXPECT_EQ(manifestOrdinalFileName(1), "000001.zst");
    EXPECT_EQ(manifestOrdinalFileName(999999), "999999.zst");
    EXPECT_THROW(manifestOrdinalFileName(0), DB::Exception);
    EXPECT_THROW(manifestOrdinalFileName(1000000), DB::Exception);
}
