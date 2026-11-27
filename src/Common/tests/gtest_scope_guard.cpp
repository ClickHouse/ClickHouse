#include <base/scope_guard.h>

#include <gtest/gtest.h>

#include <utility>

TEST(ScopeGuard, ExplicitResetPropagatesDuringUnwinding)
{
    bool recovered = false;
    try
    {
        scope_guard guard([] { throw 2; });
        SCOPE_EXIT(
            try
            {
                guard.reset();
            }
            catch (int)
            {
                recovered = true;
                guard.release();
            });
        throw 1;
    }
    catch (int value)
    {
        EXPECT_EQ(value, 1);
    }
    EXPECT_TRUE(recovered);
}

TEST(ScopeGuard, MoveAssignmentPropagatesDuringUnwinding)
{
    bool recovered = false;
    try
    {
        scope_guard target([] { throw 2; });
        scope_guard source([] {});
        SCOPE_EXIT(
            try
            {
                target = std::move(source);
            }
            catch (int)
            {
                recovered = true;
                target.release();
                source.release();
            });
        throw 1;
    }
    catch (int value)
    {
        EXPECT_EQ(value, 1);
    }
    EXPECT_TRUE(recovered);
}

TEST(ScopeGuard, SCOPE_EXITExceptionPropagatesOnNormalExit)
{
    EXPECT_THROW(
        {
            SCOPE_EXIT(throw 1;);
        },
        int);
}

TEST(ScopeGuard, SCOPE_EXITExceptionDoesNotReplaceOriginalException)
{
    int caught = 0;
    try
    {
        SCOPE_EXIT(throw 2;);
        throw 1;
    }
    catch (int value)
    {
        caught = value;
    }
    EXPECT_EQ(caught, 1);
}
