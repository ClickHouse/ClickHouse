#include <QueryPipeline/BlockIO.h>

#include <gtest/gtest.h>

#include <stdexcept>


namespace DB
{
namespace
{

TEST(BlockIO, FinishCallbackExceptionRunsExceptionCallbacksOnce)
{
    BlockIO io;
    io.finalize_query_pipeline = [](QueryPipeline &&)
    {
        return QueryPipelineFinalizedInfo{};
    };

    size_t exception_callback_calls = 0;
    io.exception_callbacks.emplace_back([&](bool) { ++exception_callback_calls; });
    io.finish_callbacks.emplace_back(
        [](const QueryPipelineFinalizedInfo &, std::chrono::system_clock::time_point)
        {
            throw std::runtime_error("finish callback failed");
        });

    EXPECT_THROW(io.onFinish(), std::runtime_error);
    EXPECT_EQ(exception_callback_calls, 1);

    io.onException();
    EXPECT_EQ(exception_callback_calls, 1);
}

}
}
