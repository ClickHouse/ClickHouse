#include <gtest/gtest.h>
#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_context.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>



TEST(LowPriorityBackgroundProcessingPool, SimpleCase) {
    using namespace DB;
    const auto & context_holder = getContext();
    Context ctx = context_holder.context;

    BackgroundProcessingPool & pool = ctx.getBackgroundLowPriorityPool();
    pool.addTask([this] { return 
