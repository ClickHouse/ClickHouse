#include <gtest/gtest.h>
#include <Common/Exception.h>
#include <Core/Field.h>

#include <limits>

namespace DB::ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

using namespace DB;

GTEST_TEST(Field, FromBool)
{
    {
        Field f{false};
        ASSERT_EQ(f.getType(), Field::Types::Bool);
        ASSERT_EQ(f.safeGet<UInt64>(), 0);
        ASSERT_EQ(f.safeGet<bool>(), false);
    }

    {
        Field f{true};
        ASSERT_EQ(f.getType(), Field::Types::Bool);
        ASSERT_EQ(f.safeGet<UInt64>(), 1);
        ASSERT_EQ(f.safeGet<bool>(), true);
    }

    {
        Field f;
        f = false;
        ASSERT_EQ(f.getType(), Field::Types::Bool);
        ASSERT_EQ(f.safeGet<UInt64>(), 0);
        ASSERT_EQ(f.safeGet<bool>(), false);
    }

    {
        Field f;
        f = true;
        ASSERT_EQ(f.getType(), Field::Types::Bool);
        ASSERT_EQ(f.safeGet<UInt64>(), 1);
        ASSERT_EQ(f.safeGet<bool>(), true);
    }
}


GTEST_TEST(Field, Move)
{
    Field f;

    f = Field{String{"Hello, world (1)"}};
    ASSERT_EQ(f.safeGet<String>(), "Hello, world (1)");
    f = Field{String{"Hello, world (2)"}};
    ASSERT_EQ(f.safeGet<String>(), "Hello, world (2)");
    f = Field{Array{Field{String{"Hello, world (3)"}}}};
    ASSERT_EQ(f.safeGet<Array>()[0].safeGet<String>(), "Hello, world (3)");
    f = String{"Hello, world (4)"};
    ASSERT_EQ(f.safeGet<String>(), "Hello, world (4)");
    f = Array{Field{String{"Hello, world (5)"}}};
    ASSERT_EQ(f.safeGet<Array>()[0].safeGet<String>(), "Hello, world (5)");
    f = Array{String{"Hello, world (6)"}};
    ASSERT_EQ(f.safeGet<Array>()[0].safeGet<String>(), "Hello, world (6)");
}


/// Copying and destroying a deeply nested Field must not overflow the native stack, both when the
/// source is a Field and when a container lvalue is wrapped/assigned through the templated
/// constructor / assignment operator (which forward to createConcrete / assignConcrete, i.e. the
/// underlying container copy, whose elements are copied via the iterative Field copy). The depth is
/// far beyond what a recursive copy could survive.
GTEST_TEST(Field, DeeplyNestedCopyAndDestroyDoesNotOverflowStack)
{
    static constexpr size_t depth = 100000;

    /// Build the nested value iteratively (moving, never copying) so constructing the test input
    /// is O(depth) and cannot overflow either.
    auto make_deep_array = []
    {
        Array a;
        a.push_back(Field{UInt64{1}});
        for (size_t i = 0; i < depth; ++i)
        {
            Array next;
            next.push_back(Field{std::move(a)});
            a = std::move(next);
        }
        return a;
    };

    /// Field(const Field &): the ASTLiteral::clone path.
    {
        Field src{make_deep_array()};
        Field copy = src;                 // NOLINT(performance-unnecessary-copy-initialization)
        ASSERT_EQ(copy.getType(), Field::Types::Array);
    }

    /// Field(T &&) with a container lvalue: createConcrete -> container copy -> per-element Field copy.
    {
        Array a = make_deep_array();
        Field from_lvalue{a};             // lvalue -> copy
        ASSERT_EQ(from_lvalue.getType(), Field::Types::Array);
    }

    /// operator=(T &&) with a container lvalue: assignConcrete / destroy+createConcrete.
    {
        Array a = make_deep_array();
        Field assigned;
        assigned = a;                     // lvalue -> copy-assign
        ASSERT_EQ(assigned.getType(), Field::Types::Array);
    }

    /// The same for a value nested inside an Object (the std::map-backed container).
    {
        Object obj;
        obj.emplace("k", Field{make_deep_array()});
        Field src{obj};                   // Object lvalue -> copy
        ASSERT_EQ(src.getType(), Field::Types::Object);
    }
}


/// Every ordering relation over a nested container agrees with the others and with `operator==`,
/// including the shorter-prefix and the differing-element-type cases. `operator<=` is covered here
/// rather than through SQL because no query reaches a `Field`-to-`Field` `<=` today.
GTEST_TEST(Field, NestedContainerOrdering)
{
    /// A comparison that probes both directions per level costs 2^levels, so this stays small: a
    /// regression must make the arms below fail rather than hang.
    static constexpr size_t depth = 12;

    auto make_deep = [](UInt64 leaf)
    {
        Array a;
        a.push_back(Field{leaf});
        for (size_t i = 0; i < depth; ++i)
        {
            Array next;
            next.push_back(Field{std::move(a)});
            a = std::move(next);
        }
        return Field{std::move(a)};
    };

    const Field one = make_deep(1);
    const Field one_again = make_deep(1);
    const Field two = make_deep(2);

    ASSERT_FALSE(one < one_again);
    ASSERT_TRUE(one <= one_again);
    ASSERT_FALSE(one > one_again);
    ASSERT_TRUE(one >= one_again);
    ASSERT_TRUE(one == one_again);

    ASSERT_TRUE(one < two);
    ASSERT_TRUE(one <= two);
    ASSERT_FALSE(two <= one);
    ASSERT_TRUE(two > one);

    /// A shorter value sorts before a longer one that shares its prefix, and an element of a
    /// different type is ordered by type tag rather than by value.
    const Field prefix{Array{one_again}};
    const Field prefix_plus{Array{one_again, Field{UInt64{0}}}};
    ASSERT_TRUE(prefix < prefix_plus);
    ASSERT_FALSE(prefix_plus < prefix);
    ASSERT_TRUE(Field(Array{Field{Int64{999}}}) < Field(Array{Field{String{"a"}}}));

    /// `Tuple`, `Map` and the `std::map`-backed `Object` share the same element-wise ordering.
    ASSERT_FALSE(Field(Tuple{one_again}) < Field(Tuple{one}));
    ASSERT_TRUE(Field(Tuple{one_again}) <= Field(Tuple{one}));
    ASSERT_FALSE(Field(Map{one_again}) < Field(Map{one}));

    Object lhs;
    Object rhs;
    lhs.emplace("k", one_again);
    rhs.emplace("k", one);
    ASSERT_FALSE(Field(lhs) < Field(rhs));
    ASSERT_TRUE(Field(lhs) <= Field(rhs));
    rhs.emplace("l", Field{UInt64{0}});
    ASSERT_TRUE(Field(lhs) < Field(rhs));
    ASSERT_FALSE(Field(rhs) < Field(lhs));
}


namespace
{

/// A leaf whose ordering is observable, so how many times a comparison visits it can be asserted
/// directly instead of inferred from how long the comparison takes. `Types::CustomType` is not a
/// container, so a comparison reaches this impl exactly where it reaches a scalar element.
struct ProbeCountingLeaf : public CustomType::CustomTypeImpl
{
    /// The counter lives outside the impl because a `CustomType` holds it through a
    /// `shared_ptr<const CustomTypeImpl>`, so the comparison operators are `const`.
    size_t * probes;

    explicit ProbeCountingLeaf(size_t * probes_) : probes(probes_) { }

    const char * getTypeName() const override { return "ProbeCountingLeaf"; }
    String toString(bool) const override { return "ProbeCountingLeaf"; }
    bool isSecret() const override { return false; }

    bool operator < (const CustomTypeImpl &) const override { ++*probes; return false; }
    bool operator <= (const CustomTypeImpl &) const override { ++*probes; return true; }
    bool operator > (const CustomTypeImpl &) const override { ++*probes; return false; }
    bool operator >= (const CustomTypeImpl &) const override { ++*probes; return true; }
    bool operator == (const CustomTypeImpl &) const override { return true; }
};

template <typename Compare>
void expectOrderingThrows(Compare && compare)
{
    try
    {
        [[maybe_unused]] const bool ignored = compare();
        ADD_FAILURE() << "ordering a container of aggregate states must throw";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
}

}


/// Counting the probes a comparison makes on one leaf pins the linear-visit contract exactly, and
/// separately per container arm: a one-pass comparison asks the leaf for its order once per
/// direction at any depth, so nothing here depends on how long a comparison takes.
GTEST_TEST(Field, DeeplyNestedComparisonProbesEachLeafOnce)
{
    /// Small for the same reason as in `NestedContainerOrdering`; the `Map` chain adds a `Map` and a
    /// `Tuple` level per step, so a doubling comparison costs 2^(2*depth) on that arm.
    static constexpr size_t depth = 12;

    size_t probes = 0;
    /// Two impls sharing one counter, so nothing upstream can halve the count by recognising that
    /// both sides hold the same leaf.
    const Field leaf_lhs{CustomType{std::make_shared<const ProbeCountingLeaf>(&probes)}};
    const Field leaf_rhs{CustomType{std::make_shared<const ProbeCountingLeaf>(&probes)}};

    auto check = [&](const char * arm, auto && wrap_once)
    {
        Field lhs = leaf_lhs;
        Field rhs = leaf_rhs;
        for (size_t i = 0; i < depth; ++i)
        {
            lhs = wrap_once(lhs);
            rhs = wrap_once(rhs);
        }

        probes = 0;
        ASSERT_FALSE(lhs < rhs) << arm;
        ASSERT_EQ(probes, 2u) << arm;

        probes = 0;
        ASSERT_TRUE(lhs <= rhs) << arm;
        ASSERT_EQ(probes, 2u) << arm;
    };

    check("Array", [](const Field & f) { return Field{Array{f}}; });
    check("Tuple", [](const Field & f) { return Field{Tuple{f}}; });
    /// A `Map` element is a (key, value) pair, so this chain alternates `Map` and `Tuple` levels.
    check("Map", [](const Field & f) { return Field{Map{Field{Tuple{Field{UInt64{0}}, f}}}}; });
    check("Object", [](const Field & f) { Object o; o.emplace("k", f); return Field{o}; });
}


/// Equality and ordering of a container are separate relations: an aggregate-function state
/// implements equality while its ordering operators throw, so answering one by asking the other
/// would either break a working query or make a deliberately unordered value orderable.
GTEST_TEST(Field, ContainerOfAggregateStateComparesForEqualityButNotForOrder)
{
    /// Both sides carry the same `name`, because comparing states of two different aggregate
    /// functions for equality throws for an unrelated reason.
    auto state = [] { return Field{AggregateFunctionStateData{.name = "sum(UInt64)", .data = "some_state"}}; };

    const Field flat_lhs{Array{state()}};
    const Field flat_rhs{Array{state()}};
    const Field nested_lhs{Array{Field{Array{state()}}}};
    const Field nested_rhs{Array{Field{Array{state()}}}};

    ASSERT_TRUE(flat_lhs == flat_rhs);
    ASSERT_TRUE(nested_lhs == nested_rhs);

    expectOrderingThrows([&] { return flat_lhs < flat_rhs; });
    expectOrderingThrows([&] { return flat_lhs <= flat_rhs; });
    expectOrderingThrows([&] { return nested_lhs < nested_rhs; });
    expectOrderingThrows([&] { return nested_lhs <= nested_rhs; });
}


GTEST_TEST(Field, CompareFloat64)
{
    const Field one{Float64(1.0)};
    const Field two{Float64(2.0)};
    const Field one_again{Float64(1.0)};

    ASSERT_TRUE(one < two);
    ASSERT_FALSE(two < one);
    ASSERT_FALSE(one < one_again);

    ASSERT_TRUE(one <= two);
    ASSERT_FALSE(two <= one);
    ASSERT_TRUE(one <= one_again);

    ASSERT_TRUE(two > one);
    ASSERT_FALSE(one > two);
    ASSERT_FALSE(one > one_again);

    ASSERT_TRUE(two >= one);
    ASSERT_FALSE(one >= two);
    ASSERT_TRUE(one >= one_again);

    ASSERT_TRUE(one == one_again);
    ASSERT_FALSE(one == two);
    ASSERT_TRUE(one != two);

    /// The same for integers, to make sure the Float64 branch is not the odd one out.
    ASSERT_FALSE(Field(Int64(2)) <= Field(Int64(1)));
    ASSERT_FALSE(Field(Int64(1)) >= Field(Int64(2)));
}


GTEST_TEST(Field, CompareFloat64NaN)
{
    /// NaN is ordered after every number (nan_direction_hint == 1) and is equal to itself.
    const Field nan{std::numeric_limits<Float64>::quiet_NaN()};
    const Field nan_again{std::numeric_limits<Float64>::quiet_NaN()};
    const Field inf{std::numeric_limits<Float64>::infinity()};
    const Field one{Float64(1.0)};

    ASSERT_TRUE(one < nan);
    ASSERT_TRUE(inf < nan);
    ASSERT_FALSE(nan < one);
    ASSERT_FALSE(nan < nan_again);

    ASSERT_TRUE(one <= nan);
    ASSERT_FALSE(nan <= one);
    ASSERT_TRUE(nan <= nan_again);

    ASSERT_TRUE(nan > one);
    ASSERT_FALSE(one > nan);
    ASSERT_FALSE(nan > nan_again);

    ASSERT_TRUE(nan >= one);
    ASSERT_FALSE(one >= nan);
    ASSERT_TRUE(nan >= nan_again);

    ASSERT_TRUE(nan == nan_again);
    ASSERT_FALSE(nan == one);
}


GTEST_TEST(Field, CompareDifferentTypes)
{
    /// Fields of different types are ordered by Types::Which before any value comparison,
    /// so values don't matter across types; operator== / != short-circuit on differing Which.
    const Field i{Int64(999)};   /// Which::Int64 == 2
    const Field s{String("a")};  /// Which::String == 16

    ASSERT_TRUE(i < s);
    ASSERT_FALSE(s < i);

    ASSERT_TRUE(i <= s);
    ASSERT_FALSE(s <= i);

    ASSERT_TRUE(s > i);
    ASSERT_TRUE(s >= i);
    ASSERT_FALSE(i >= s);

    ASSERT_FALSE(i == s);
    ASSERT_TRUE(i != s);
    ASSERT_FALSE(Field(Int64(1)) == Field(UInt64(1)));  /// same value, different Which
    ASSERT_TRUE(Field(Int64(1)) != Field(UInt64(1)));
}


GTEST_TEST(Field, CompareUUID)
{
    /// UUID is a StrongTypedef<UInt128> with operator< but no operator<=, so the <= / >=
    /// branches compare toUnderType(); pin that the two forms stay consistent.
    const Field one{UUID(UInt128(1))};
    const Field two{UUID(UInt128(2))};
    const Field one_again{UUID(UInt128(1))};

    ASSERT_TRUE(one < two);
    ASSERT_FALSE(two < one);

    ASSERT_TRUE(one <= two);
    ASSERT_FALSE(two <= one);
    ASSERT_TRUE(one <= one_again);

    ASSERT_TRUE(two >= one);
    ASSERT_FALSE(one >= two);
    ASSERT_TRUE(one >= one_again);

    ASSERT_TRUE(one == one_again);
    ASSERT_FALSE(one == two);
}
