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
    EXPECT_TRUE(uuid.needsRevalidation("metadata/v1.metadata.json", identity("etag-v1")));
}

/// The steady state - the same query re-selecting the very same, unchanged file over and over -
/// must not pay for an uncached read, or the metadata content cache would be defeated on every
/// query. Same path, same identity means the file was not rewritten.
TEST(IcebergTrustedTableUuid, DoesNotRevalidateTheSameUnchangedFile)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json", identity("etag-v3"));
    EXPECT_FALSE(uuid.needsRevalidation("metadata/v3.metadata.json", identity("etag-v3")));
}

/// A newly appended metadata file is a file that was never validated, so it is read and its own
/// `table-uuid` is checked. A higher version number proves nothing on its own: a table recreated
/// at the same root may start above every version validated so far.
TEST(IcebergTrustedTableUuid, RevalidatesWhenTheVersionAdvances)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(1, "metadata/v1.metadata.json", identity("etag-v1"));
    EXPECT_TRUE(uuid.needsRevalidation("metadata/v2.metadata.json", identity("etag-v2")));
}

/// The reported bug: the table is dropped and recreated at the same root with a new
/// `table-uuid`, restarting the numbering, so the selected version does not advance. The
/// recreated table wrote its own files, so they carry their own `etag`s.
TEST(IcebergTrustedTableUuid, RevalidatesWhenVersionRepeats)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json", identity("etag-v3"));
    EXPECT_TRUE(uuid.needsRevalidation("metadata/v3.metadata.json", identity("etag-recreated-v3")));
    EXPECT_TRUE(uuid.needsRevalidation("metadata/v1.metadata.json", identity("etag-recreated-v1")));
}

/// Replacing a table in place rewrites `metadata.json`, which changes its identity even when the
/// path and the version are reused.
TEST(IcebergTrustedTableUuid, RevalidatesWhenTheFileWasRewritten)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json", identity("etag-v3"));
    EXPECT_TRUE(uuid.needsRevalidation("metadata/v3.metadata.json", identity("etag-v3-rewritten")));
}

/// Selecting a different file at the same version is equally suspicious: the
/// `<V>-<random-uuid>.metadata.json` naming lets a recreated table reuse a version number
/// under a different path.
TEST(IcebergTrustedTableUuid, RevalidatesWhenPathChangesAtSameVersion)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/3-aaaa.metadata.json", identity("etag-3"));
    EXPECT_TRUE(uuid.needsRevalidation("metadata/3-bbbb.metadata.json", identity("etag-3")));
}

/// A file whose identity the storage cannot report is never assumed unchanged.
TEST(IcebergTrustedTableUuid, RevalidatesWhenTheIdentityIsUnknown)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json", identity("etag-v3"));
    EXPECT_TRUE(uuid.needsRevalidation("metadata/v3.metadata.json", unknown_identity));

    TrustedTableUuid other("11111111-1111-1111-1111-111111111111");
    other.markValidated(3, "metadata/v3.metadata.json", unknown_identity);
    EXPECT_TRUE(other.needsRevalidation("metadata/v3.metadata.json", identity("etag-v3")));
}

/// A table with no `table-uuid` at all - possible only in format version 1 - is not exempt: it
/// is the one whose replacement can only be seen in the metadata file, so the file has to be
/// read for `commitValidated` to see it.
TEST(IcebergTrustedTableUuid, RevalidatesWithoutUuidToo)
{
    TrustedTableUuid uuid(std::nullopt);
    EXPECT_TRUE(uuid.needsRevalidation("metadata/v1.metadata.json", identity("etag-v1")));

    uuid.markValidated(1, "metadata/v1.metadata.json", identity("etag-v1"));
    EXPECT_FALSE(uuid.needsRevalidation("metadata/v1.metadata.json", identity("etag-v1")));
    EXPECT_TRUE(uuid.needsRevalidation("metadata/v1.metadata.json", identity("etag-rewritten")));
}

/// A pinned `TableStateSnapshot` reopens its metadata file at execution time, and it may only be
/// content-cached while the table is still the incarnation that was analyzed.
TEST(IcebergTrustedTableUuid, PinnedFileIsCacheKeyedOnlyWithinItsOwnIncarnation)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    const auto pinned = uuid.getIncarnation();
    uuid.markValidated(1, "metadata/v1.metadata.json", identity("etag-v1"));

    EXPECT_EQ(uuid.getForPinnedIncarnation(pinned), std::optional<String>("11111111-1111-1111-1111-111111111111"));

    /// A concurrent query detects an in-place replacement and moves the shared cell. The
    /// replacement reuses the very same metadata file path, so the path proves nothing - only the
    /// incarnation does, and the pinned query must stop keying the cache by the moved UUID.
    ASSERT_TRUE(uuid.commitValidated("22222222-2222-2222-2222-222222222222", 1, "metadata/v1.metadata.json", identity("etag-new-v1"), /*content_token=*/1));
    EXPECT_EQ(uuid.getForPinnedIncarnation(pinned), std::nullopt);

    /// A query that pins after the replacement keys the cache by the new UUID again.
    EXPECT_EQ(uuid.getForPinnedIncarnation(uuid.getIncarnation()), std::optional<String>("22222222-2222-2222-2222-222222222222"));
}

/// An ordinary revalidation that confirms the current UUID is not a replacement and must not
/// invalidate the pins taken before it, or every query would pay for an uncached read.
TEST(IcebergTrustedTableUuid, ConfirmingTheSameUuidKeepsThePinsValid)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    const auto pinned = uuid.getIncarnation();

    ASSERT_FALSE(uuid.commitValidated("11111111-1111-1111-1111-111111111111", 2, "metadata/v2.metadata.json", identity("etag-v2"), /*content_token=*/1));
    EXPECT_EQ(uuid.getForPinnedIncarnation(pinned), std::optional<String>("11111111-1111-1111-1111-111111111111"));
}

/// A pin that carries no incarnation - one deserialized on another server, whose cell counts its
/// own incarnations - is never cache-keyed.
TEST(IcebergTrustedTableUuid, APinWithoutAnIncarnationIsNeverCacheKeyed)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(1, "metadata/v1.metadata.json", identity("etag-v1"));
    EXPECT_EQ(uuid.getForPinnedIncarnation(std::nullopt), std::nullopt);
}

/// `commitValidated` records the trusted value, and `get` publishes it to the cache-key call sites.
TEST(IcebergTrustedTableUuid, PublishesTheRefreshedUuid)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    EXPECT_EQ(uuid.get(), std::optional<String>("11111111-1111-1111-1111-111111111111"));

    EXPECT_TRUE(uuid.commitValidated("22222222-2222-2222-2222-222222222222", 1, "metadata/v1.metadata.json", identity("etag-v1"), /*content_token=*/1));
    EXPECT_EQ(uuid.get(), std::optional<String>("22222222-2222-2222-2222-222222222222"));

    /// Committing the same value again is not a change, so callers can tell a genuine
    /// replacement from an ordinary revalidation that confirmed the current value.
    EXPECT_FALSE(uuid.commitValidated("22222222-2222-2222-2222-222222222222", 1, "metadata/v1.metadata.json", identity("etag-v1"), /*content_token=*/1));
}

/// A format-version 1 table that omits `table-uuid` has no identity to compare, so its
/// replacement is seen in the metadata file: the numbering restarts and the file is not the one
/// that was validated.
TEST(IcebergTrustedTableUuid, DetectsAReplacementOfATableWithoutUuid)
{
    TrustedTableUuid uuid(std::nullopt);
    uuid.markValidated(5, "metadata/v5.metadata.json", identity("etag-v5"));

    /// The recreated table restarts at version 1.
    EXPECT_TRUE(uuid.isReplacementOfValidatedFile(1, "metadata/v1.metadata.json", identity("etag-new-v1")));
    /// It may also reuse the very same path, and then only the identity tells the two apart.
    EXPECT_TRUE(uuid.isReplacementOfValidatedFile(5, "metadata/v5.metadata.json", identity("etag-rewritten")));

    /// The unchanged file, and an ordinary append by the same table, are not replacements.
    EXPECT_FALSE(uuid.isReplacementOfValidatedFile(5, "metadata/v5.metadata.json", identity("etag-v5")));
    EXPECT_FALSE(uuid.isReplacementOfValidatedFile(6, "metadata/v6.metadata.json", identity("etag-v6")));
}

/// A table that does carry a `table-uuid` is settled by that UUID, and a storage that cannot
/// report the identity of the listed file leaves nothing to compare. Neither may be reported as
/// a replacement here, or an ordinary statement would be refused.
TEST(IcebergTrustedTableUuid, DoesNotGuessAReplacementWithoutEvidence)
{
    TrustedTableUuid with_uuid("11111111-1111-1111-1111-111111111111");
    with_uuid.markValidated(5, "metadata/v5.metadata.json", identity("etag-v5"));
    EXPECT_FALSE(with_uuid.isReplacementOfValidatedFile(1, "metadata/v1.metadata.json", identity("etag-new-v1")));

    TrustedTableUuid nothing_validated_yet(std::nullopt);
    EXPECT_FALSE(nothing_validated_yet.isReplacementOfValidatedFile(1, "metadata/v1.metadata.json", identity("etag-v1")));
}

#endif

/// The file token a statement validated against is published for the pre-publish reread, which is
/// the only witness a table without `table-uuid` has once its replacement has committed past the
/// validated version.
TEST(IcebergTrustedTableUuid, PublishesTheValidatedFileToken)
{
    TrustedTableUuid uuid(std::nullopt);
    EXPECT_EQ(uuid.getValidatedFileWithToken(), std::nullopt);

    ASSERT_FALSE(uuid.commitValidated(std::nullopt, 1, "metadata/v1.metadata.json", identity("etag-v1"), /*content_token=*/42));
    EXPECT_EQ(uuid.getValidatedFileWithToken(), (std::optional<std::pair<String, UInt64>>({"metadata/v1.metadata.json", 42})));

    /// Recording the very same unchanged file again does not read its content, so the token that
    /// was recorded for it stays.
    uuid.markValidated(1, "metadata/v1.metadata.json", identity("etag-v1"));
    EXPECT_EQ(uuid.getValidatedFileWithToken(), (std::optional<std::pair<String, UInt64>>({"metadata/v1.metadata.json", 42})));

    /// A different file has no token until its content is read.
    uuid.markValidated(2, "metadata/v2.metadata.json", identity("etag-v2"));
    EXPECT_EQ(uuid.getValidatedFileWithToken(), std::nullopt);
}
