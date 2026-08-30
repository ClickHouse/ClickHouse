#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/TrustedTableUuid.h>

using namespace DB::Iceberg;

namespace
{

/// The size and modification time the listing reports for a metadata file.
std::optional<MetadataFileIdentity> identity(UInt64 size_bytes, Poco::Timestamp::TimeVal last_modified)
{
    return MetadataFileIdentity{size_bytes, last_modified};
}

/// A storage that cannot report size or modification time for the listed file.
constexpr std::optional<MetadataFileIdentity> unknown_identity = std::nullopt;

}

/// A freshly opened table has validated nothing yet, so the first selected metadata file
/// must always have its `table-uuid` validated against storage.
TEST(IcebergTrustedTableUuid, RevalidatesWhenNothingWasValidatedYet)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    EXPECT_TRUE(uuid.needsRevalidation(1, "metadata/v1.metadata.json", identity(100, 1000)));
}

/// A writer appending new metadata files advances the version strictly, which no in-place
/// table replacement restarting the numbering can do. The extra uncached read is skipped.
TEST(IcebergTrustedTableUuid, DoesNotRevalidateWhenVersionStrictlyAdvances)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(1, "metadata/v1.metadata.json", identity(100, 1000));
    EXPECT_FALSE(uuid.needsRevalidation(2, "metadata/v2.metadata.json", identity(200, 2000)));
}

/// The reported bug: the table is dropped and recreated at the same root with a new
/// `table-uuid`, restarting the numbering, so the selected version does not advance.
TEST(IcebergTrustedTableUuid, RevalidatesWhenVersionRepeats)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json", identity(100, 1000));
    EXPECT_TRUE(uuid.needsRevalidation(3, "metadata/v3.metadata.json", identity(300, 3000)));
    EXPECT_TRUE(uuid.needsRevalidation(1, "metadata/v1.metadata.json", identity(100, 1000)));
}

/// The steady state - the same query re-selecting the very same, unchanged file over and over -
/// must not pay for an uncached read, or the metadata content cache would be defeated on every
/// query. Same path, same size, same modification time means the file was not rewritten.
TEST(IcebergTrustedTableUuid, DoesNotRevalidateTheSameUnchangedFile)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json", identity(100, 1000));
    EXPECT_FALSE(uuid.needsRevalidation(3, "metadata/v3.metadata.json", identity(100, 1000)));
}

/// Replacing a table in place rewrites `metadata.json`, which changes its size or its
/// modification time even when the path and the version are reused.
TEST(IcebergTrustedTableUuid, RevalidatesWhenTheFileWasRewritten)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json", identity(100, 1000));
    EXPECT_TRUE(uuid.needsRevalidation(3, "metadata/v3.metadata.json", identity(101, 1000)));
    EXPECT_TRUE(uuid.needsRevalidation(3, "metadata/v3.metadata.json", identity(100, 1001)));
}

/// A file whose identity the storage cannot report is never assumed unchanged.
TEST(IcebergTrustedTableUuid, RevalidatesWhenTheIdentityIsUnknown)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json", identity(100, 1000));
    EXPECT_TRUE(uuid.needsRevalidation(3, "metadata/v3.metadata.json", unknown_identity));

    TrustedTableUuid other("11111111-1111-1111-1111-111111111111");
    other.markValidated(3, "metadata/v3.metadata.json", unknown_identity);
    EXPECT_TRUE(other.needsRevalidation(3, "metadata/v3.metadata.json", identity(100, 1000)));
}

/// The watermark has to follow every *observed* version, not only the revalidated ones.
/// A writer appending metadata files moves the selected version past the last validated one
/// without any revalidation; if the watermark stayed behind, a later in-place replacement
/// reusing that version would still compare against the old, lower watermark and would never
/// be revalidated. This is the interleaving that made the reported bug survive the first fix.
TEST(IcebergTrustedTableUuid, WatermarkFollowsAnAdvancingVersion)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(1, "metadata/v1.metadata.json", identity(100, 1000));

    ASSERT_FALSE(uuid.needsRevalidation(2, "metadata/v2.metadata.json", identity(200, 2000)));
    uuid.markValidated(2, "metadata/v2.metadata.json", identity(200, 2000));

    /// A table recreated in place restarts the numbering and reuses version 2.
    EXPECT_TRUE(uuid.needsRevalidation(2, "metadata/v2.metadata.json", identity(222, 2222)));
}

/// The watermark must never move backwards on its own: two concurrent `update` calls can
/// observe different metadata files, and the older observation must not undo the newer one.
TEST(IcebergTrustedTableUuid, WatermarkNeverMovesBackwardsWithoutAReplacement)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(5, "metadata/v5.metadata.json", identity(500, 5000));
    uuid.markValidated(3, "metadata/v3.metadata.json", identity(300, 3000));

    /// Had the watermark dropped to 3, version 4 would have been trusted without a check.
    EXPECT_TRUE(uuid.needsRevalidation(4, "metadata/v4.metadata.json", identity(400, 4000)));
}

/// A rewrite of the file that currently sits at the watermark refreshes the recorded identity,
/// so the next query compares against what is actually in storage now.
TEST(IcebergTrustedTableUuid, WatermarkRefreshesTheIdentityAtTheSameVersion)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json", identity(100, 1000));
    uuid.markValidated(3, "metadata/v3.metadata.json", identity(300, 3000));

    EXPECT_FALSE(uuid.needsRevalidation(3, "metadata/v3.metadata.json", identity(300, 3000)));
}

/// A confirmed replacement does reset the watermark: the recreated table restarts the
/// numbering, and keeping the previous table's higher watermark would force an uncached read
/// on every query until the new table caught up with it.
TEST(IcebergTrustedTableUuid, ReplacementResetsTheWatermark)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(5, "metadata/v5.metadata.json", identity(500, 5000));

    EXPECT_TRUE(uuid.commitValidated("22222222-2222-2222-2222-222222222222", 1, "metadata/v1.metadata.json", identity(100, 1000)));
    EXPECT_EQ(uuid.get(), std::optional<String>("22222222-2222-2222-2222-222222222222"));
    EXPECT_FALSE(uuid.needsRevalidation(2, "metadata/v2.metadata.json", identity(200, 2000)));
    EXPECT_FALSE(uuid.needsRevalidation(1, "metadata/v1.metadata.json", identity(100, 1000)));
}

/// Selecting a different file at the same version is equally suspicious: the
/// `<V>-<random-uuid>.metadata.json` naming lets a recreated table reuse a version number
/// under a different path.
TEST(IcebergTrustedTableUuid, RevalidatesWhenPathChangesAtSameVersion)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/3-aaaa.metadata.json", identity(100, 1000));
    EXPECT_TRUE(uuid.needsRevalidation(3, "metadata/3-bbbb.metadata.json", identity(100, 1000)));
}

/// `commitValidated` records the trusted value, and `get` publishes it to the cache-key call sites.
TEST(IcebergTrustedTableUuid, PublishesTheRefreshedUuid)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    EXPECT_EQ(uuid.get(), std::optional<String>("11111111-1111-1111-1111-111111111111"));

    EXPECT_TRUE(uuid.commitValidated("22222222-2222-2222-2222-222222222222", 1, "metadata/v1.metadata.json", identity(100, 1000)));
    EXPECT_EQ(uuid.get(), std::optional<String>("22222222-2222-2222-2222-222222222222"));

    /// Committing the same value again is not a change, so callers can tell a genuine
    /// replacement from an ordinary revalidation that confirmed the current value.
    EXPECT_FALSE(uuid.commitValidated("22222222-2222-2222-2222-222222222222", 1, "metadata/v1.metadata.json", identity(100, 1000)));
}

/// A table without a `table-uuid` (format version 1 is allowed to omit it) is never
/// content-cached under a UUID key, so there is nothing to revalidate and no extra read.
TEST(IcebergTrustedTableUuid, NeverRevalidatesWithoutUuid)
{
    TrustedTableUuid uuid(std::nullopt);
    EXPECT_FALSE(uuid.needsRevalidation(1, "metadata/v1.metadata.json", unknown_identity));
    EXPECT_FALSE(uuid.needsRevalidation(1, "metadata/v1.metadata.json", identity(100, 1000)));
}

#endif
