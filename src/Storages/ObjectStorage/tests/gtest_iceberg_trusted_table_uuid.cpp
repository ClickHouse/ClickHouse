#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/TrustedTableUuid.h>

using namespace DB::Iceberg;

namespace
{

/// The strong `etag` the listing reports for a metadata file.
std::optional<MetadataFileIdentity> identity(const String & etag)
{
    return MetadataFileIdentity{etag};
}

/// A storage that reports no strong `etag` for the listed file.
constexpr std::optional<MetadataFileIdentity> unknown_identity = std::nullopt;

}

/// A freshly opened table has validated nothing yet, so the first selected metadata file
/// must always have its `table-uuid` validated against storage.
TEST(IcebergTrustedTableUuid, RevalidatesWhenNothingWasValidatedYet)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    EXPECT_TRUE(uuid.needsRevalidation(1, "metadata/v1.metadata.json", identity("etag-v1")));
}

/// A writer appending new metadata files advances the version strictly, which no in-place
/// table replacement restarting the numbering can do. The extra uncached read is skipped.
TEST(IcebergTrustedTableUuid, DoesNotRevalidateWhenVersionStrictlyAdvances)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(1, "metadata/v1.metadata.json", identity("etag-v1"));
    EXPECT_FALSE(uuid.needsRevalidation(2, "metadata/v2.metadata.json", identity("etag-v2")));
}

/// The reported bug: the table is dropped and recreated at the same root with a new
/// `table-uuid`, restarting the numbering, so the selected version does not advance. The
/// recreated table wrote its own files, so they carry their own `etag`s.
TEST(IcebergTrustedTableUuid, RevalidatesWhenVersionRepeats)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json", identity("etag-v3"));
    EXPECT_TRUE(uuid.needsRevalidation(3, "metadata/v3.metadata.json", identity("etag-recreated-v3")));
    EXPECT_TRUE(uuid.needsRevalidation(1, "metadata/v1.metadata.json", identity("etag-recreated-v1")));
}

/// The steady state - the same query re-selecting the very same, unchanged file over and over -
/// must not pay for an uncached read, or the metadata content cache would be defeated on every
/// query. Same path, same size, same modification time means the file was not rewritten.
TEST(IcebergTrustedTableUuid, DoesNotRevalidateTheSameUnchangedFile)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json", identity("etag-v3"));
    EXPECT_FALSE(uuid.needsRevalidation(3, "metadata/v3.metadata.json", identity("etag-v3")));
}

/// Replacing a table in place rewrites `metadata.json`, which changes its size or its
/// modification time even when the path and the version are reused.
TEST(IcebergTrustedTableUuid, RevalidatesWhenTheFileWasRewritten)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json", identity("etag-v3"));
    EXPECT_TRUE(uuid.needsRevalidation(3, "metadata/v3.metadata.json", identity("etag-v3-rewritten")));
}

/// A pinned `TableStateSnapshot` reopens its metadata file at execution time, and it may only be
/// content-cached while the shared cell still describes the incarnation that was analyzed.
TEST(IcebergTrustedTableUuid, PinnedFileIsCacheKeyedOnlyWhileItIsTheValidatedOne)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json", identity("etag-v3"));

    EXPECT_EQ(uuid.getForValidatedFile("metadata/v3.metadata.json"), std::optional<String>("11111111-1111-1111-1111-111111111111"));
    /// Some other file: nothing says the trusted UUID belongs to it.
    EXPECT_EQ(uuid.getForValidatedFile("metadata/v2.metadata.json"), std::nullopt);

    /// A concurrent query detects an in-place replacement and moves the shared cell. The pinned
    /// file must no longer be keyed by it - the replacement may have put a different file at the
    /// same path.
    ASSERT_TRUE(uuid.commitValidated("22222222-2222-2222-2222-222222222222", 1, "metadata/v1.metadata.json", identity("etag-new-v1")));
    EXPECT_EQ(uuid.getForValidatedFile("metadata/v3.metadata.json"), std::nullopt);
}

/// A table with nothing validated yet keys nothing.
TEST(IcebergTrustedTableUuid, NothingIsCacheKeyedBeforeTheFirstValidation)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    EXPECT_EQ(uuid.getForValidatedFile("metadata/v1.metadata.json"), std::nullopt);
}

/// A file whose identity the storage cannot report is never assumed unchanged.
TEST(IcebergTrustedTableUuid, RevalidatesWhenTheIdentityIsUnknown)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json", identity("etag-v3"));
    EXPECT_TRUE(uuid.needsRevalidation(3, "metadata/v3.metadata.json", unknown_identity));

    TrustedTableUuid other("11111111-1111-1111-1111-111111111111");
    other.markValidated(3, "metadata/v3.metadata.json", unknown_identity);
    EXPECT_TRUE(other.needsRevalidation(3, "metadata/v3.metadata.json", identity("etag-v3")));
}

/// The watermark has to follow every *observed* version, not only the revalidated ones.
/// A writer appending metadata files moves the selected version past the last validated one
/// without any revalidation; if the watermark stayed behind, a later in-place replacement
/// reusing that version would still compare against the old, lower watermark and would never
/// be revalidated. This is the interleaving that made the reported bug survive the first fix.
TEST(IcebergTrustedTableUuid, WatermarkFollowsAnAdvancingVersion)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(1, "metadata/v1.metadata.json", identity("etag-v1"));

    ASSERT_FALSE(uuid.needsRevalidation(2, "metadata/v2.metadata.json", identity("etag-v2")));
    uuid.markValidated(2, "metadata/v2.metadata.json", identity("etag-v2"));

    /// A table recreated in place restarts the numbering and reuses version 2.
    EXPECT_TRUE(uuid.needsRevalidation(2, "metadata/v2.metadata.json", identity("etag-v2-rewritten")));
}

/// The watermark must never move backwards on its own: two concurrent `update` calls can
/// observe different metadata files, and the older observation must not undo the newer one.
TEST(IcebergTrustedTableUuid, WatermarkNeverMovesBackwardsWithoutAReplacement)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(5, "metadata/v5.metadata.json", identity("etag-v5"));
    uuid.markValidated(3, "metadata/v3.metadata.json", identity("etag-v3"));

    /// Had the watermark dropped to 3, version 4 would have been trusted without a check.
    EXPECT_TRUE(uuid.needsRevalidation(4, "metadata/v4.metadata.json", identity("etag-v4")));
}

/// A rewrite of the file that currently sits at the watermark refreshes the recorded identity,
/// so the next query compares against what is actually in storage now.
TEST(IcebergTrustedTableUuid, WatermarkRefreshesTheIdentityAtTheSameVersion)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json", identity("etag-v3"));
    uuid.markValidated(3, "metadata/v3.metadata.json", identity("etag-v3"));

    EXPECT_FALSE(uuid.needsRevalidation(3, "metadata/v3.metadata.json", identity("etag-v3")));
}

/// A confirmed replacement does reset the watermark: the recreated table restarts the
/// numbering, and keeping the previous table's higher watermark would force an uncached read
/// on every query until the new table caught up with it.
TEST(IcebergTrustedTableUuid, ReplacementResetsTheWatermark)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(5, "metadata/v5.metadata.json", identity("etag-v5"));

    EXPECT_TRUE(uuid.commitValidated("22222222-2222-2222-2222-222222222222", 1, "metadata/v1.metadata.json", identity("etag-v1")));
    EXPECT_EQ(uuid.get(), std::optional<String>("22222222-2222-2222-2222-222222222222"));
    EXPECT_FALSE(uuid.needsRevalidation(2, "metadata/v2.metadata.json", identity("etag-v2")));
    EXPECT_FALSE(uuid.needsRevalidation(1, "metadata/v1.metadata.json", identity("etag-v1")));
}

/// Selecting a different file at the same version is equally suspicious: the
/// `<V>-<random-uuid>.metadata.json` naming lets a recreated table reuse a version number
/// under a different path.
TEST(IcebergTrustedTableUuid, RevalidatesWhenPathChangesAtSameVersion)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/3-aaaa.metadata.json", identity("etag-3"));
    EXPECT_TRUE(uuid.needsRevalidation(3, "metadata/3-bbbb.metadata.json", identity("etag-3")));
}

/// `commitValidated` records the trusted value, and `get` publishes it to the cache-key call sites.
TEST(IcebergTrustedTableUuid, PublishesTheRefreshedUuid)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    EXPECT_EQ(uuid.get(), std::optional<String>("11111111-1111-1111-1111-111111111111"));

    EXPECT_TRUE(uuid.commitValidated("22222222-2222-2222-2222-222222222222", 1, "metadata/v1.metadata.json", identity("etag-v1")));
    EXPECT_EQ(uuid.get(), std::optional<String>("22222222-2222-2222-2222-222222222222"));

    /// Committing the same value again is not a change, so callers can tell a genuine
    /// replacement from an ordinary revalidation that confirmed the current value.
    EXPECT_FALSE(uuid.commitValidated("22222222-2222-2222-2222-222222222222", 1, "metadata/v1.metadata.json", identity("etag-v1")));
}

/// A table without a `table-uuid` (format version 1 is allowed to omit it) is never
/// content-cached under a UUID key, so there is nothing to revalidate and no extra read.
TEST(IcebergTrustedTableUuid, NeverRevalidatesWithoutUuid)
{
    TrustedTableUuid uuid(std::nullopt);
    EXPECT_FALSE(uuid.needsRevalidation(1, "metadata/v1.metadata.json", unknown_identity));
    EXPECT_FALSE(uuid.needsRevalidation(1, "metadata/v1.metadata.json", identity("etag-v1")));
}

#endif
