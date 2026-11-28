#include <base/scope_guard.h>

#include <gtest/gtest.h>

#include <stdexcept>
#include <string>
#include <utility>

TEST(ScopeGuard, ExplicitResetPropagatesDuringUnwinding)
{
    bool recovered = false;
    try
    {
        scope_guard guard([] { throw std::runtime_error("cleanup"); });
        SCOPE_EXIT(
            try
            {
                guard.reset();
            }
            catch (const std::runtime_error &)
            {
                recovered = true;
                guard.release();
            });
        throw std::runtime_error("original");
    }
    catch (const std::runtime_error & error)
    {
        EXPECT_STREQ(error.what(), "original");
    }
    EXPECT_TRUE(recovered);
}

TEST(ScopeGuard, MoveAssignmentPropagatesDuringUnwinding)
{
    bool recovered = false;
    try
    {
        scope_guard target([] { throw std::runtime_error("cleanup"); });
        scope_guard source([] {});
        SCOPE_EXIT(
            try
            {
                target = std::move(source);
            }
            catch (const std::runtime_error &)
            {
                recovered = true;
                target.release();
                source.release();
            });
        throw std::runtime_error("original");
    }
    catch (const std::runtime_error & error)
    {
        EXPECT_STREQ(error.what(), "original");
    }
    EXPECT_TRUE(recovered);
}

TEST(ScopeGuard, ScopeExitExceptionPropagatesOnNormalExit)
{
    EXPECT_THROW(
        {
            SCOPE_EXIT(throw std::runtime_error("cleanup"););
        },
        std::runtime_error);
}

TEST(ScopeGuard, ScopeExitExceptionDoesNotReplaceOriginalException)
{
    bool original_exception_caught = false;
    try
    {
        SCOPE_EXIT(throw std::runtime_error("cleanup"););
        throw std::runtime_error("original");
    }
    catch (const std::runtime_error & error)
    {
        original_exception_caught = std::string(error.what()) == "original";
    }
    EXPECT_TRUE(original_exception_caught);
}
