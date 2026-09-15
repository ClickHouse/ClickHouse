#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasWriteOnceKey.h>
#include <Disks/tests/cas_test_helpers.h>

#include <type_traits>

/// A `WriteOnceKey` names an object of one of the three families that are written once and never
/// rewritten: a part manifest, a ref log, a ref snapshot. Only `Layout` can mint one, from a typed
/// identity, so a verb that takes the type cannot be handed a mutable control key.

using namespace DB::Cas;

static_assert(!std::is_default_constructible_v<WriteOnceKey>);
static_assert(!std::is_constructible_v<WriteOnceKey, String>);
static_assert(!std::is_constructible_v<WriteOnceKey, const char *>);

TEST(CASWriteOnceKey, FactoriesMintTheSameStringsAsThePlainKeyFunctions)
{
    const Layout layout{"p"};
    const RootNamespace ns{"test/aa@cas@"};
    const ManifestId manifest{ns, ManifestRef{.writer_epoch = 3, .build_sequence = 9, .manifest_ordinal = 2}};
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(ns, DB::UInt128(0x1234));
    const RefTxnId id{5, 7};

    EXPECT_EQ(layout.writeOnceManifestKey(manifest).str(), layout.manifestKey(manifest));
    EXPECT_EQ(layout.writeOnceRefLogKey(life, id).str(), layout.refLogKey(life, id));
    EXPECT_EQ(layout.writeOnceRefSnapshotKey(life, id).str(), layout.refSnapshotKey(life, id));
    EXPECT_TRUE(layout.parseManifestKey(layout.writeOnceManifestKey(manifest).str()).has_value());
    EXPECT_TRUE(layout.parseRefObjectKey(layout.writeOnceRefLogKey(life, id).str()).has_value());
}
