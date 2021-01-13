#include <gtest/gtest.h>

#include <Coordination/InMemoryLogStore.h>
#include <Coordination/InMemoryStateManager.h>

TEST(CoordinationTest, BuildTest)
{
    DB::InMemoryLogStore store;
    DB::InMemoryStateManager state_manager(1, "localhost:12345");
    EXPECT_EQ(1, 1);
}
