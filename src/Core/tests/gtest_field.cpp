#include <gtest/gtest.h>
#include <Core/Field.h>

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
