#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Core/ProtocolDefines.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

/// The version tests never build a step, so the create function is a placeholder.
QueryPlanStepPtr nullCreate(IQueryPlanStep::Deserialization &)
{
    return nullptr;
}

}

/// The writer serializes at a stable global plan version and must pick the step version pinned for it,
/// so an older peer gets the older step format.
TEST(QueryPlanStepRegistryVersions, WriteVersionFollowsPlanVersion)
{
    QueryPlanStepRegistry registry;
    /// Declared out of order on purpose: registration sorts the write entries by plan version.
    registry.registerStep("Test", nullCreate, {.readable = {3, 4, 5}, .write = {{18, 5}, {16, 4}}});

    EXPECT_EQ(registry.writeStepVersion("Test", 16), 4u);
    EXPECT_EQ(registry.writeStepVersion("Test", 17), 4u);
    EXPECT_EQ(registry.writeStepVersion("Test", 18), 5u);
    EXPECT_EQ(registry.writeStepVersion("Test", 100), 5u);
}

/// A step version this binary does not know is refused up front, so wrong-code bytes are never
/// misparsed. This is the protection the per-step version buys on master.
TEST(QueryPlanStepRegistryVersions, UnknownReadVersionIsRefused)
{
    QueryPlanStepRegistry registry;
    registry.registerStep("Test", nullCreate, {.readable = {3, 4}, .write = {{16, 4}}});

    EXPECT_NO_THROW(registry.checkStepVersionReadable("Test", 3));
    EXPECT_NO_THROW(registry.checkStepVersionReadable("Test", 4));

    try
    {
        registry.checkStepVersionReadable("Test", 5);
        FAIL() << "expected INCORRECT_DATA for an unknown step version";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
    }
}

/// A step whose bytes have never changed stays at version 0 and needs no version declaration.
TEST(QueryPlanStepRegistryVersions, DefaultStepStaysAtVersionZero)
{
    QueryPlanStepRegistry registry;
    registry.registerStep("Plain", nullCreate);

    EXPECT_EQ(registry.writeStepVersion("Plain", DBMS_QUERY_PLAN_SERIALIZATION_VERSION), 0u);
    EXPECT_NO_THROW(registry.checkStepVersionReadable("Plain", 0));
    EXPECT_ANY_THROW(registry.checkStepVersionReadable("Plain", 1));
}
