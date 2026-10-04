#include <gtest/gtest.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_register.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace DB;

namespace
{

/// `sumMap` is a versioned aggregate function (`AggregateFunctionMapBase::isVersioned` returns true):
/// `getVersionFromRevision` is 1 for revision >= 54452 and 0 otherwise; the default is 1.
DataTypePtr makeVersionedAggType()
{
    auto type = DataTypeFactory::instance().get("AggregateFunction(sumMap, Array(UInt64), Array(UInt64))");
    /// Guard the test's own premise: if this ever stops being versioned, the setup is invalid.
    EXPECT_TRUE(typeid_cast<const DataTypeAggregateFunction &>(*type).isVersioned());
    return type;
}

/// `count` state is a single counter and its `serialize` ignores the version parameter, so there is
/// no second representation a version could select between: `isVersioned` is the base class's false.
DataTypePtr makeUnversionedAggType()
{
    auto type = DataTypeFactory::instance().get("AggregateFunction(count, UInt64)");
    /// Guard the test's own premise: if this ever becomes versioned, the setup is invalid.
    EXPECT_FALSE(typeid_cast<const DataTypeAggregateFunction &>(*type).isVersioned());
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

    /// Fresh type has no explicit version; `getVersion` falls back to the default.
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

/// A type with no aggregate function must be left untouched, including its custom name
/// (a make_shared rebuild of the wrapper types would drop custom names).
/// Point is a custom-named Tuple(Float64, Float64); stripping its name broke GeoJSON output.
GTEST_TEST(DataTypeAggregateFunctionVersion, PreservesCustomNameOfNonAggregateType)
{
    tryRegisterAggregateFunctions();

    /// Point is registered as a custom-named Tuple(Float64, Float64).
    DataTypePtr point = DataTypeFactory::instance().get("Point");
    ASSERT_EQ(point->getName(), "Point");
    const IDataType * point_ptr_before = point.get();

    DataTypePtr assigned = point;
    setVersionToAggregateFunctions(assigned, /*if_empty=*/true, /*revision=*/54452);

    /// No aggregate function anywhere: the type (and its custom name) must survive intact,
    /// and the object must not even be rebuilt.
    ASSERT_EQ(assigned->getName(), "Point");
    ASSERT_EQ(assigned.get(), point_ptr_before);
    ASSERT_EQ(point->getName(), "Point");
}

/// A wrapper whose only aggregate child is UNVERSIONED must be left untouched:
/// no leaf is replaced, so the outer `Nested`/`Array`/`Tuple` must NOT be rebuilt (that would drop
/// the `Nested` custom name and rewrite the Native type name / ATTACH encoding from `Nested(...)` to
/// plain `Array(Tuple(...))`).
GTEST_TEST(DataTypeAggregateFunctionVersion, UnversionedLeafPreservesNestedCustomName)
{
    tryRegisterAggregateFunctions();

    DataTypePtr nested = createNested({makeUnversionedAggType()}, {"s"});
    const String name_before = nested->getName();
    ASSERT_TRUE(name_before.starts_with("Nested("));
    const IDataType * nested_ptr_before = nested.get();

    DataTypePtr assigned = nested;
    setVersionToAggregateFunctions(assigned, /*if_empty=*/true, /*revision=*/54452);

    /// The custom name survives and the object is not even rebuilt.
    ASSERT_EQ(assigned->getName(), name_before);
    ASSERT_EQ(assigned.get(), nested_ptr_before);
    ASSERT_EQ(nested->getName(), name_before);
}

/// A Nested wrapper whose leaf IS versioned has to be rebuilt, and the rebuild must keep printing
/// as Nested(...): the type is sent to the client over Native and, on ATTACH, is what ends up in the
/// table metadata. Rebuilding the Array/Tuple through make_shared would degrade it to
/// Array(Tuple(...)) and, with it, lose the Nested semantics of the column.
GTEST_TEST(DataTypeAggregateFunctionVersion, VersionedLeafPreservesNestedCustomName)
{
    tryRegisterAggregateFunctions();

    DataTypePtr nested = DataTypeFactory::instance().get("Nested(x AggregateFunction(sumMap, Array(UInt64), Array(UInt64)))");
    ASSERT_TRUE(isNested(nested));

    DataTypePtr assigned = nested;
    setVersionToAggregateFunctions(assigned, /*if_empty=*/true, /*revision=*/54451);

    /// The leaf was versioned (54451 resolves to version 0), so the tree was rebuilt ...
    ASSERT_NE(assigned.get(), nested.get());
    /// ... but it is still a Nested, and its name reflects the new version of the element.
    ASSERT_TRUE(isNested(assigned));
    ASSERT_EQ(assigned->getName(), "Nested(x AggregateFunction(sumMap, Array(UInt64), Array(UInt64)))");

    const auto & nested_elements = typeid_cast<const DataTypeNestedCustomName *>(assigned->getCustomName())->getElements();
    ASSERT_EQ(nested_elements.size(), 1u);
    ASSERT_EQ(asAgg(nested_elements[0]).getVersion(), 0u);

    /// The shared source type is untouched: default version 1, still printed with it.
    ASSERT_EQ(nested->getName(), "Nested(x AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64)))");
}

/// `SimpleAggregateFunction(f, T)` is stored as T plus a DataTypeCustomSimpleAggregateFunction name.
/// When T is itself a versioned AggregateFunction, versioning replaces the leaf - and the custom name
/// has to come along, because the AggregatingMergeTree and SummingMergeTree merge algorithms
/// recognise a simple-aggregate column by dynamic_cast on that name. Losing it silently turns the
/// column into a plain AggregateFunction one, changing how the table merges.
GTEST_TEST(DataTypeAggregateFunctionVersion, VersionedLeafPreservesSimpleAggregateFunctionName)
{
    tryRegisterAggregateFunctions();

    DataTypePtr simple = DataTypeFactory::instance().get(
        "SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(UInt64)))");
    ASSERT_NE(dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(simple->getCustomName()), nullptr);
    const String name_before = simple->getName();
    ASSERT_EQ(asAgg(simple).getVersion(), 1u);

    /// SimpleAggregateFunction resolves its storage type through the type name, which pins the
    /// version explicitly. Only a forced assignment therefore replaces the leaf - which is what the
    /// Native writer does for a client that predates aggregate function versioning.
    DataTypePtr assigned = simple;
    setVersionToAggregateFunctions(assigned, /*if_empty=*/false, /*revision=*/std::nullopt);

    ASSERT_NE(assigned.get(), simple.get());
    ASSERT_EQ(asAgg(assigned).getVersion(), 0u);

    /// The custom name came along, so the column is still recognised as a SimpleAggregateFunction
    /// one - and it announces the version the payload is actually written with: the custom name
    /// keeps its own copy of the argument types, and for `SimpleAggregateFunction` over an
    /// `AggregateFunction` that argument is the state type itself, so it has to be downgraded with
    /// the storage type or the receiver would read one version too many out of the state.
    ASSERT_NE(dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(assigned->getCustomName()), nullptr);
    ASSERT_EQ(assigned->getName(), "SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(UInt64)))");
    ASSERT_NE(assigned->getName(), name_before);

    /// The shared source type is untouched - this is the race that used to force version 0 onto it
    /// for every other query as a side effect of serving one old client.
    ASSERT_EQ(asAgg(simple).getVersion(), 1u);
    ASSERT_EQ(simple->getName(), name_before);
}

/// The custom name does not have to sit on the versioned leaf itself: in
/// `SimpleAggregateFunction(anyLast, Array(AggregateFunction(...)))` it sits on the Array, while the
/// leaf that is replaced is one level below. Both the Array and the custom name's copy of its argument
/// type must be rebuilt, so the type name agrees with the payload sent to an older peer.
GTEST_TEST(DataTypeAggregateFunctionVersion, CustomNameOnWrapperIsRebuilt)
{
    tryRegisterAggregateFunctions();

    DataTypePtr simple = DataTypeFactory::instance().get(
        "SimpleAggregateFunction(anyLast, Array(AggregateFunction(sumMap, Array(UInt64), Array(UInt64))))");
    ASSERT_NE(dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(simple->getCustomName()), nullptr);
    const String name_before = simple->getName();

    DataTypePtr assigned = simple;
    setVersionToAggregateFunctions(assigned, /*if_empty=*/false, /*revision=*/std::nullopt);

    ASSERT_NE(assigned.get(), simple.get());
    ASSERT_NE(dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(assigned->getCustomName()), nullptr);
    ASSERT_EQ(assigned->getName(), "SimpleAggregateFunction(anyLast, Array(AggregateFunction(sumMap, Array(UInt64), Array(UInt64))))");
    ASSERT_NE(assigned->getName(), name_before);

    /// The shared source type is untouched.
    ASSERT_EQ(simple->getName(), name_before);
}

/// `SimpleAggregateFunction` can use any legal storage type, not only Array. Its custom name must
/// therefore be rebuilt when the versioned leaf is below a Tuple as well.
GTEST_TEST(DataTypeAggregateFunctionVersion, CustomNameOnTupleWrapperIsRebuilt)
{
    tryRegisterAggregateFunctions();

    DataTypePtr simple = DataTypeFactory::instance().get(
        "SimpleAggregateFunction(anyLast, Tuple(AggregateFunction(sumMap, Array(UInt64), Array(UInt64))))");
    ASSERT_NE(dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(simple->getCustomName()), nullptr);
    const String name_before = simple->getName();

    DataTypePtr assigned = simple;
    setVersionToAggregateFunctions(assigned, /*if_empty=*/false, /*revision=*/std::nullopt);

    ASSERT_NE(assigned.get(), simple.get());
    ASSERT_NE(dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(assigned->getCustomName()), nullptr);
    ASSERT_EQ(assigned->getName(), "SimpleAggregateFunction(anyLast, Tuple(AggregateFunction(sumMap, Array(UInt64), Array(UInt64))))");
    ASSERT_NE(assigned->getName(), name_before);

    /// The shared source type is untouched.
    ASSERT_EQ(simple->getName(), name_before);
}

/// A state cannot be directly inside Nullable, but a Tuple can be, so
/// `Nullable(Tuple(AggregateFunction(...)))` is reachable (with enable_nullable_tuple_type) and
/// transformTypesRecursively descended into Nullable first of all. Skipping it would leave the leaf
/// unversioned, so an old client would be sent a version-1 payload under an unversioned type name.
GTEST_TEST(DataTypeAggregateFunctionVersion, VersionedLeafUnderNullableIsAssigned)
{
    tryRegisterAggregateFunctions();

    DataTypePtr nullable = DataTypeFactory::instance().get(
        "Nullable(Tuple(AggregateFunction(sumMap, Array(UInt64), Array(UInt64))))");

    DataTypePtr assigned = nullable;
    setVersionToAggregateFunctions(assigned, /*if_empty=*/true, /*revision=*/std::nullopt);

    /// Version 0 was forced onto the leaf under Nullable(Tuple(...)), and it is still a Nullable.
    ASSERT_NE(assigned.get(), nullable.get());
    const auto * assigned_nullable = typeid_cast<const DataTypeNullable *>(assigned.get());
    ASSERT_NE(assigned_nullable, nullptr);
    const auto * assigned_tuple = typeid_cast<const DataTypeTuple *>(assigned_nullable->getNestedType().get());
    ASSERT_NE(assigned_tuple, nullptr);
    ASSERT_EQ(asAgg(assigned_tuple->getElements()[0]).getVersion(), 0u);

    /// The shared source type still resolves to its default version.
    const auto * source_tuple = typeid_cast<const DataTypeTuple *>(
        typeid_cast<const DataTypeNullable *>(nullable.get())->getNestedType().get());
    ASSERT_EQ(asAgg(source_tuple->getElements()[0]).getVersion(), 1u);
}

/// A `Variant` alternative is a normal nested type: it is announced with its own name, so the
/// version has to be assigned there too. The rebuild must keep the original order - the default
/// `DataTypeVariant` constructor sorts alternatives by name, and a spelled-out version changes the
/// name, so sorting again would permute the discriminators of an existing column.
GTEST_TEST(DataTypeAggregateFunctionVersion, VersionedLeafUnderVariantKeepsDiscriminatorOrder)
{
    tryRegisterAggregateFunctions();

    DataTypePtr variant = DataTypeFactory::instance().get(
        "Variant(String, AggregateFunction(sumMap, Array(UInt64), Array(UInt64)))");
    const auto & source_variants = typeid_cast<const DataTypeVariant &>(*variant).getVariants();
    /// The factory sorts the alternatives by name, so the state is the first one here.
    ASSERT_EQ(source_variants[0]->getName(), "AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))");
    ASSERT_EQ(source_variants[1]->getName(), "String");

    DataTypePtr assigned = variant;
    setVersionToAggregateFunctions(assigned, /*if_empty=*/true, /*revision=*/std::nullopt);

    ASSERT_NE(assigned.get(), variant.get());
    const auto & assigned_variants = typeid_cast<const DataTypeVariant &>(*assigned).getVariants();
    ASSERT_EQ(assigned_variants.size(), source_variants.size());

    /// Same discriminators: the state is still the first alternative, now at version 0, and the
    /// `String` one is still the second - the rebuild keeps the original order instead of sorting
    /// the alternatives by their new names.
    ASSERT_EQ(asAgg(assigned_variants[0]).getVersion(), 0u);
    ASSERT_EQ(assigned_variants[1]->getName(), "String");

    /// The shared source type is untouched.
    ASSERT_EQ(asAgg(source_variants[0]).getVersion(), 1u);
}

/// Two alternatives that differ only in a spelled-out version would become the same type once both
/// are re-versioned. There is no way to announce that on the wire, so it has to be an error rather
/// than a silent no-op that announces versions the payload is not written with.
GTEST_TEST(DataTypeAggregateFunctionVersion, VariantAlternativesCollapsingIsAnError)
{
    tryRegisterAggregateFunctions();

    /// Version 0 is not printed, so these two alternatives have different names - and the same name
    /// once both are forced to version 0.
    DataTypePtr variant = DataTypeFactory::instance().get(
        "Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), "
        "AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64)))");
    ASSERT_EQ(typeid_cast<const DataTypeVariant &>(*variant).getVariants().size(), 2u);

    DataTypePtr assigned = variant;
    ASSERT_THROW(
        setVersionToAggregateFunctions(assigned, /*if_empty=*/false, /*revision=*/std::nullopt), DB::Exception);
}

/// `DataTypeObject` does not traverse typed `JSON` paths when assigning aggregate-state versions.
/// This parses a type declaration that `JSON` serialization subsequently rejects, and protects that
/// traversal boundary; it does not describe a `Native` wire-format exception. Binary type encoding
/// does contain an explicit aggregate-state version field.
GTEST_TEST(DataTypeAggregateFunctionVersion, VersionedLeafUnderJSONTypedPathIsNotAssigned)
{
    tryRegisterAggregateFunctions();

    DataTypePtr json = DataTypeFactory::instance().get(
        "JSON(x AggregateFunction(sumMap, Array(UInt64), Array(UInt64)))");

    DataTypePtr assigned = json;
    setVersionToAggregateFunctions(assigned, /*if_empty=*/true, /*revision=*/std::nullopt);

    ASSERT_EQ(assigned.get(), json.get());

    const auto & source_paths = typeid_cast<const DataTypeObject &>(*json).getTypedPaths();
    ASSERT_EQ(asAgg(source_paths.at("x")).getVersion(), 1u);
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
