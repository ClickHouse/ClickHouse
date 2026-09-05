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

/// The file a statement validated against is published for the pre-publish reread, together with
/// the fingerprint of the content it carried.
TEST(IcebergTrustedTableUuid, PublishesTheValidatedFile)
{
    TrustedTableUuid uuid(std::nullopt);
    EXPECT_FALSE(uuid.getValidatedFile().has_value());

    ASSERT_FALSE(uuid.commitValidated(std::nullopt, 1, "metadata/v1.metadata.json", identity("etag-v1"), /*content_token=*/42));
    ASSERT_TRUE(uuid.getValidatedFile().has_value());
    EXPECT_EQ(uuid.getValidatedFile()->path, "metadata/v1.metadata.json");
    EXPECT_EQ(uuid.getValidatedFile()->content_token, std::optional<UInt64>(42));

    /// Recording the very same unchanged file again does not read its content, so the token that
    /// was recorded for it stays.
    uuid.markValidated(1, "metadata/v1.metadata.json", identity("etag-v1"));
    EXPECT_EQ(uuid.getValidatedFile()->content_token, std::optional<UInt64>(42));

    /// A different file has no token until its content is read.
    uuid.markValidated(2, "metadata/v2.metadata.json", identity("etag-v2"));
    EXPECT_EQ(uuid.getValidatedFile()->path, "metadata/v2.metadata.json");
    EXPECT_EQ(uuid.getValidatedFile()->content_token, std::nullopt);
}

/// A table without `table-uuid` recreated in place over the same metadata file name, on a storage
/// that reports no identity for the object: the content the file answers with is the only proof of
/// the replacement, and `update` has already read it.
TEST(IcebergTrustedTableUuid, DetectsARewrittenFileWithoutStrongIdentity)
{
    TrustedTableUuid uuid(std::nullopt);
    ASSERT_FALSE(uuid.commitValidated(std::nullopt, 1, "metadata/v1.metadata.json", std::nullopt, /*content_token=*/1));
    const auto pinned = uuid.getIncarnation();

    EXPECT_TRUE(uuid.commitValidated(std::nullopt, 1, "metadata/v1.metadata.json", std::nullopt, /*content_token=*/2));
    EXPECT_NE(uuid.getIncarnation(), pinned);
}

/// The same file answering with the same content is not a replacement, or every revalidation of an
/// unchanged table would install a fresh schema processor.
TEST(IcebergTrustedTableUuid, TheSameContentIsNotAReplacement)
{
    TrustedTableUuid uuid(std::nullopt);
    ASSERT_FALSE(uuid.commitValidated(std::nullopt, 1, "metadata/v1.metadata.json", std::nullopt, /*content_token=*/1));
    const auto pinned = uuid.getIncarnation();

    EXPECT_FALSE(uuid.commitValidated(std::nullopt, 1, "metadata/v1.metadata.json", std::nullopt, /*content_token=*/1));
    EXPECT_EQ(uuid.getIncarnation(), pinned);
}

#endif
