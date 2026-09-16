#include <gtest/gtest.h>

#include <base/sanitizer_options.h>

#include <Common/ThreadPool.h>
#include <Common/scope_guard_safe.h>
#include <IO/SharedThreadPools.h>
#include <Common/tests/gtest_global_context.h>

class ContextEnvironment : public testing::Environment
{
public:
    void SetUp() override { getContext(); }
    void TearDown() override { getMutableContext().destroy(); }
};

int main(int argc, char ** argv)
{
    /// Join global-pool threads before the statics they may have accessed are destroyed.
    /// That way, accesses happen-before destruction.
    SCOPE_EXIT_SAFE({
        DB::StaticThreadPool::shutdownAll();
        GlobalThreadPool::shutdown();
    });

    testing::InitGoogleTest(&argc, argv);

    auto & options = getTestCommandLineOptions();
    options.argc = argc;
    options.argv = argv;

    testing::AddGlobalTestEnvironment(new ContextEnvironment);

    return RUN_ALL_TESTS();
}
