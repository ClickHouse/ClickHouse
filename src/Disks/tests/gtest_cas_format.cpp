#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Common/Exception.h>
#include <set>
#include <string_view>

namespace DB::ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int LOGICAL_ERROR;
}

using namespace DB::Cas;

/// Closed-set pin: the registry's complete set of object `type` strings. `allRegisteredFormatIds`
/// is the registry's own enumeration accessor, so this walks the SAME set the codecs and the object
/// header gate see -- a registered class with no test coverage here is a registered class this test
/// cannot see either, which is the point: a 17th, 18th, ... entry the spec's closed set does not
/// name would show up as a set-size mismatch instead of passing unnoticed.
TEST(CASFormat, RegistryTypeStringsArePinnedClosedSet)
{
    const std::set<std::string_view> expected{
        "cas_blob", "cas_blob_meta", "cas_pool_meta", "cas_ref_log", "cas_ref_snap",
        "cas_ref_ckpt", "cas_ref_catalog", "cas_gc_maintenance_state", "cas_part_manifest",
        "cas_run", "cas_fold_seal", "cas_gc_state", "cas_gc_hb", "cas_gc_outcomes",
        "cas_owner", "cas_epoch", "cas_mount_lease"};
    ASSERT_EQ(expected.size(), 17u);

    std::set<std::string_view> actual;
    for (const auto id : allRegisteredFormatIds())
        actual.insert(traitsFor(id).type);
    EXPECT_EQ(actual, expected);

    for (const auto & type : expected)
    {
        const FormatTraits * t = traitsForType(type);
        ASSERT_NE(t, nullptr) << type;
        EXPECT_EQ(t->type, type);
    }
}

/// The generation history is reset to a flat `{1, 1}` baseline for every class: CAS is pre-release and
/// carries no persisted data, so there is no compatibility cost to starting the count over. Pinned
/// because the decision is invisible otherwise — nothing consults `changePoints` at decode time yet, so
/// a wrong entry here would sit unnoticed until the day a per-class reader floor is wired and starts
/// admitting objects it should refuse.
TEST(CASFormat, EveryClassResetToTheBaselineGeneration)
{
    for (auto id : allRegisteredFormatIds())
    {
        const auto cps = changePoints(id);
        ASSERT_EQ(cps.size(), 1u) << "FormatId " << static_cast<int>(id);
        EXPECT_EQ(cps.front().generation, 1u);
        EXPECT_EQ(cps.front().min_reader, 1u);
    }

    const auto roster_cps = changePoints(FormatId::Roster);
    ASSERT_EQ(roster_cps.size(), 1u);
    EXPECT_EQ(roster_cps.front().generation, 1u);
    EXPECT_EQ(roster_cps.front().min_reader, 1u);
}

TEST(CASFormat, CurrentVersionsAreGBuild)
{
    EXPECT_EQ(currentWriterVersion(), G_BUILD);
    EXPECT_EQ(currentCompatibilityVersion(), G_BUILD);
}

TEST(CASFormat, CheckCompatibilityPassesWhenKnown)
{
    EXPECT_NO_THROW(checkCompatibility(1u, "manifest"));
    EXPECT_NO_THROW(checkCompatibility(G_BUILD, "manifest"));
}

TEST(CASFormat, CheckCompatibilityFailsClosedOnFuture)
{
    try
    {
        checkCompatibility(G_BUILD + 1, "manifest");
        FAIL() << "expected UNKNOWN_FORMAT_VERSION";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::UNKNOWN_FORMAT_VERSION);
    }
}

/// The battery's goldens spell their header version as the literal 1 rather than asking production
/// for it, so that a generation bump cannot move the expectation and the encoder output together.
/// The cost of that is a golden set which goes stale silently if nobody notices the bump; this test
/// is what notices. It fails FIRST and says what to do, so the failure that greets a generation bump
/// is one explanatory test rather than every exact-encoding golden at once.
TEST(CASFormat, HeaderVersionIsTheLiteralThisBatteryPins)
{
    EXPECT_EQ(currentCompatibilityVersion(), 1u)
        << "The compatibility version has moved away from the literal 1 that "
           "`currentFormatHeader` in cas_format_test_battery.h writes into every golden header. "
           "That is a deliberate wire change: read the new bytes, agree to them, and update the "
           "literal and the goldens together. Do NOT make the helper derive the version from "
           "production again -- a golden that tracks the code it pins cannot fail.";
}
