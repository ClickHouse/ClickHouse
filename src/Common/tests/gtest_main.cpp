#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>

class ContextEnvironment : public testing::Environment
{
public:
    void SetUp() override { getContext(); }
};

int main(int argc, char ** argv)
{
    testing::InitGoogleTest(&argc, argv);

    testing::AddGlobalTestEnvironment(new ContextEnvironment);

    return RUN_ALL_TESTS();
}
