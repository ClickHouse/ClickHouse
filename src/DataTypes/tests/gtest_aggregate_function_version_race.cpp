#include <gtest/gtest.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/tests/gtest_global_register.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace DB;

namespace
{

/// sumMap is a versioned aggregate function (AggregateFunctionMapBase::isVersioned() == true):
/// getVersionFromRevision(revision) is 1 for revision >= 54452 and 0 otherwise; default is 1.
DataTypePtr makeVersionedAggType()
{
    auto type = DataTypeFactory::instance().get("AggregateFunction(sumMap, Array(UInt64), Array(UInt64))");
    /// Guard the test's own premise: if this ever stops being versioned, the setup is invalid.
    EXPECT_TRUE(typeid_cast<const DataTypeAggregateFunction &>(*type).isVersioned());
    return type;
}

const DataTypeAggregateFunction & asAgg(const DataTypePtr & type)
{
    const auto * agg = typeid_cast<const DataTypeAggregateFunction *>(type.get());
    EXPECT_NE(agg, nullptr);
    return *agg;
}

}

/// setVersionToAggregateFunctions must NOT mutate the (possibly shared) type object.
/// It replaces the leaf with a copy carrying the version. Regression for the arm_tsan
/// data race (STID 3977-4818) where two concurrent Native serializations wrote the
/// shared type's `mutable version` field.
GTEST_TEST(DataTypeAggregateFunctionVersion, SetVersionDoesNotMutateSharedType)
{
    tryRegisterAggregateFunctions();

    DataTypePtr shared = makeVersionedAggType();
    const IDataType * shared_ptr_before = shared.get();

    /// Fresh type has no explicit version; getVersion() falls back to the default.
    ASSERT_FALSE(asAgg(shared).getVersionIfExplicit().has_value());

    DataTypePtr assigned = shared;
    setVersionToAggregateFunctions(assigned, /*if_empty=*/true, /*revision=*/54452);

    /// The shared object is untouched: same object, still no explicit version.
    ASSERT_EQ(shared.get(), shared_ptr_before);
    ASSERT_FALSE(asAgg(shared).getVersionIfExplicit().has_value());

    /// The returned type is a different object that carries the resolved version.
    ASSERT_NE(assigned.get(), shared.get());
    ASSERT_TRUE(asAgg(assigned).getVersionIfExplicit().has_value());
    ASSERT_EQ(asAgg(assigned).getVersion(), 1u);
}

/// A revision below the versioning threshold resolves to version 0, still without
/// mutating the shared type.
GTEST_TEST(DataTypeAggregateFunctionVersion, OldRevisionResolvesToZero)
{
    tryRegisterAggregateFunctions();

    DataTypePtr shared = makeVersionedAggType();
    DataTypePtr assigned = shared;
    setVersionToAggregateFunctions(assigned, /*if_empty=*/true, /*revision=*/54451);

    ASSERT_FALSE(asAgg(shared).getVersionIfExplicit().has_value());
    ASSERT_NE(assigned.get(), shared.get());
    ASSERT_EQ(asAgg(assigned).getVersion(), 0u);
}

/// Nested aggregate types inside Array are rebuilt too, without mutating the source.
GTEST_TEST(DataTypeAggregateFunctionVersion, NestedInArrayIsRebuilt)
{
    tryRegisterAggregateFunctions();

    DataTypePtr shared = std::make_shared<DataTypeArray>(makeVersionedAggType());
    DataTypePtr assigned = shared;
    setVersionToAggregateFunctions(assigned, /*if_empty=*/true, /*revision=*/54452);

    const auto & shared_nested = asAgg(typeid_cast<const DataTypeArray &>(*shared).getNestedType());
    ASSERT_FALSE(shared_nested.getVersionIfExplicit().has_value());

    const auto & assigned_nested = asAgg(typeid_cast<const DataTypeArray &>(*assigned).getNestedType());
    ASSERT_TRUE(assigned_nested.getVersionIfExplicit().has_value());
    ASSERT_EQ(assigned_nested.getVersion(), 1u);
}

/// Concurrent setVersionToAggregateFunctions calls over the SAME shared type object.
/// Before the fix each call wrote the shared object's mutable `version`, producing the
/// arm_tsan data race. After the fix nothing shared is mutated, so this is race-free
/// (and additionally verifies correctness under contention).
GTEST_TEST(DataTypeAggregateFunctionVersion, ConcurrentSetVersionIsRaceFree)
{
    tryRegisterAggregateFunctions();

    DataTypePtr shared = makeVersionedAggType();

    constexpr size_t num_threads = 8;
    constexpr size_t iterations = 2000;
    std::atomic<size_t> mismatches{0};

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&, t]
        {
            const size_t revision = (t % 2 == 0) ? 54452 : 54451;
            const size_t expected = (revision >= 54452) ? 1u : 0u;
            for (size_t i = 0; i < iterations; ++i)
            {
                DataTypePtr local = shared;
                setVersionToAggregateFunctions(local, /*if_empty=*/true, revision);
                if (asAgg(local).getVersion() != expected)
                    mismatches.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto & thread : threads)
        thread.join();

    /// Every thread must observe its own revision-derived version, independent of the
    /// others (no cross-thread clobbering of a shared field).
    ASSERT_EQ(mismatches.load(), 0u);
    ASSERT_FALSE(asAgg(shared).getVersionIfExplicit().has_value());
}
