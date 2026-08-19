#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/TrustedTableUuid.h>

using namespace DB::Iceberg;

/// A freshly opened table has validated nothing yet, so the first selected metadata file
/// must always have its `table-uuid` validated against storage.
TEST(IcebergTrustedTableUuid, RevalidatesWhenNothingWasValidatedYet)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    EXPECT_TRUE(uuid.needsRevalidation(1, "metadata/v1.metadata.json"));
}

/// A writer appending new metadata files advances the version strictly, which no in-place
/// table replacement restarting the numbering can do. The extra uncached read is skipped.
TEST(IcebergTrustedTableUuid, DoesNotRevalidateWhenVersionStrictlyAdvances)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(1, "metadata/v1.metadata.json");
    EXPECT_FALSE(uuid.needsRevalidation(2, "metadata/v2.metadata.json"));
}

/// The reported bug: the table is dropped and recreated at the same root with a new
/// `table-uuid`, restarting the numbering, so the selected version does not advance.
TEST(IcebergTrustedTableUuid, RevalidatesWhenVersionRepeats)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/v3.metadata.json");
    EXPECT_TRUE(uuid.needsRevalidation(3, "metadata/v3.metadata.json"));
    EXPECT_TRUE(uuid.needsRevalidation(1, "metadata/v1.metadata.json"));
}

/// Selecting a different file at the same version is equally suspicious: the
/// `<V>-<random-uuid>.metadata.json` naming lets a recreated table reuse a version number
/// under a different path.
TEST(IcebergTrustedTableUuid, RevalidatesWhenPathChangesAtSameVersion)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    uuid.markValidated(3, "metadata/3-aaaa.metadata.json");
    EXPECT_TRUE(uuid.needsRevalidation(3, "metadata/3-bbbb.metadata.json"));
}

/// `markValidated` records the trusted value, and `get` publishes it to the cache-key call sites.
TEST(IcebergTrustedTableUuid, PublishesTheRefreshedUuid)
{
    TrustedTableUuid uuid("11111111-1111-1111-1111-111111111111");
    EXPECT_EQ(uuid.get(), std::optional<String>("11111111-1111-1111-1111-111111111111"));

    EXPECT_TRUE(uuid.set("22222222-2222-2222-2222-222222222222"));
    EXPECT_EQ(uuid.get(), std::optional<String>("22222222-2222-2222-2222-222222222222"));

    /// Setting the same value again is not a change, so callers can tell a genuine
    /// replacement from an ordinary revalidation that confirmed the current value.
    EXPECT_FALSE(uuid.set("22222222-2222-2222-2222-222222222222"));
}

/// A table without a `table-uuid` (format version 1 is allowed to omit it) is never
/// content-cached under a UUID key, so there is nothing to revalidate and no extra read.
TEST(IcebergTrustedTableUuid, NeverRevalidatesWithoutUuid)
{
    TrustedTableUuid uuid(std::nullopt);
    EXPECT_FALSE(uuid.needsRevalidation(1, "metadata/v1.metadata.json"));
    EXPECT_FALSE(uuid.needsRevalidation(1, "metadata/v1.metadata.json"));
}

#endif
