#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <QueryPipeline/SizeLimits.h>

#include <gtest/gtest.h>

#include <future>
#include <stdexcept>
#include <variant>

using namespace DB;

namespace
{

SetPtr makeDummySet()
{
    return std::make_shared<Set>(SizeLimits{}, /*max_elements_to_fill_=*/ 0, /*transform_null_in_=*/ false);
}

}

/// A successfully built set is reused by later callers instead of being rebuilt.
TEST(PreparedSetsCache, ReusesSuccessfullyBuiltSet)
{
    PreparedSetsCache cache;

    auto first = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(first.index(), 0u) << "The first caller must be asked to build the set";

    auto set = makeDummySet();
    std::get<0>(first).set_value(set);

    auto second = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(second.index(), 1u) << "A later caller must reuse the built set";
    EXPECT_EQ(std::get<1>(second).get(), set);
}

/// A cached null result is deliberately retryable: a later caller rebuilds it.
TEST(PreparedSetsCache, RebuildsNullResult)
{
    PreparedSetsCache cache;

    auto first = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(first.index(), 0u);
    std::get<0>(first).set_value(nullptr);

    auto second = cache.findOrPromiseToBuild("key");
    EXPECT_EQ(second.index(), 0u) << "A null cached result must be rebuilt, not reused";
}

/// Regression for a cancelled mutation set build poisoning the shared cache (issue #51586 follow-up):
/// a builder cancelled mid-flight stores an exception into the shared entry (see the
/// `CreatingSetsTransform` destructor). `findOrPromiseToBuild` must not rethrow that exception to a
/// later, unrelated caller (e.g. a sibling mutation part whose partition was not cancelled); it must
/// drop the poisoned entry and let the caller rebuild the set.
TEST(PreparedSetsCache, RebuildsAfterBuilderException)
{
    PreparedSetsCache cache;

    auto builder = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(builder.index(), 0u);
    std::get<0>(builder).set_exception(
        std::make_exception_ptr(std::runtime_error("Failed to build set, most likely pipeline executor was stopped")));

    /// The next caller must neither throw nor inherit the failure - it must be asked to rebuild.
    std::variant<std::promise<SetPtr>, SharedSet> retry;
    ASSERT_NO_THROW(retry = cache.findOrPromiseToBuild("key"));
    ASSERT_EQ(retry.index(), 0u) << "A poisoned cache entry must be handed out for rebuilding";

    /// The rebuild succeeds and is then reused normally.
    auto set = makeDummySet();
    std::get<0>(retry).set_value(set);

    auto reuse = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(reuse.index(), 1u);
    EXPECT_EQ(std::get<1>(reuse).get(), set);
}

/// A concurrent waiter observes a poisoned build (its `SharedSet::get` throws) and its mutation part
/// task fails once; a later attempt of the same mutation calls `findOrPromiseToBuild` again and is
/// handed a fresh promise to rebuild the set instead of inheriting the poisoned entry forever.
TEST(PreparedSetsCache, WaiterFailureIsRebuildableOnRetry)
{
    PreparedSetsCache cache;

    auto builder = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(builder.index(), 0u);

    auto waiter = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(waiter.index(), 1u) << "A second concurrent caller must wait for the builder";

    std::get<0>(builder).set_exception(
        std::make_exception_ptr(std::runtime_error("Failed to build set, most likely pipeline executor was stopped")));

    /// The waiter sees the builder's failure (the mutation part task fails this attempt).
    EXPECT_THROW((void)std::get<1>(waiter).get(), std::runtime_error);

    /// A later attempt is asked to rebuild rather than getting the poisoned entry again.
    auto retry = cache.findOrPromiseToBuild("key");
    EXPECT_EQ(retry.index(), 0u);
}
