#include <gtest/gtest.h>
#include <Common/Exception.h>
#include <Core/Field.h>

#include <limits>

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


GTEST_TEST(Field, RestoreFromDumpRoundTripsNestedAndQuotedContainers)
{
    /// The value shapes below cannot be built from SQL: the SET grammar accepts only a literal or a
    /// flat map of string literals, so a stateless test cannot reach a nested container, an array
    /// element or an aggregate state.
    auto round_trip = [](const Field & field) { return Field::restoreFromDump(field.dump()); };

    /// Nested containers. A Map element is always dumped as a nested Tuple_(...), so any non-empty
    /// map exercises this.
    {
        Array inner{Field{UInt64{1}}, Field{UInt64{2}}};
        Field nested_array{Array{Field{std::move(inner)}, Field{Tuple{Field{String{"x"}}, Field{Int64{-3}}}}}};
        ASSERT_EQ(round_trip(nested_array), nested_array);

        Field nested_map{Map{Field{Tuple{Field{String{"k"}}, Field{Map{Field{Tuple{Field{String{"a"}}, Field{String{"b"}}}}}}}}}};
        ASSERT_EQ(round_trip(nested_map), nested_map);
    }

    /// String elements holding the separators and the escape characters of the dump grammar.
    {
        Field hostile{Array{Field{String{"a,b]c)d'e\\f"}}, Field{String{"("}}, Field{String{"'"}}}};
        ASSERT_EQ(round_trip(hostile), hostile);
    }

    /// An aggregate state: the name may be parameterised, and the data is quoted but its brackets
    /// and commas are not escaped.
    {
        Field state{AggregateFunctionStateData{.name = "quantiles(0.5, 0.9)", .data = "a)b,c'd\\e"}};
        ASSERT_EQ(round_trip(state), state);
    }

    /// Empty containers.
    {
        ASSERT_EQ(round_trip(Field{Array{}}), Field{Array{}});
        ASSERT_EQ(round_trip(Field{Tuple{}}), Field{Tuple{}});
        ASSERT_EQ(round_trip(Field{Map{}}), Field{Map{}});
    }

    /// An element that parses only partially is an error, not a silently truncated container: a
    /// hand written users.xml value of Map_('k':'v') must not restore as Map(String 'k').
    ASSERT_THROW(Field::restoreFromDump("Map_('k':'v')"), DB::Exception);

    /// A dump nests as deep as its text says, so the depth has to be bounded. Built as text
    /// directly: dumping a Field this deep would recurse before the parser is reached.
    {
        static constexpr size_t depth = 100000;
        String deep;
        deep.reserve(depth * 8 + 16);
        for (size_t i = 0; i < depth; ++i)
            deep += "Array_[";
        deep += "UInt64_1";
        deep.append(depth, ']');
        ASSERT_THROW(Field::restoreFromDump(deep), DB::Exception);
    }
}
