#include <gtest/gtest.h>
#include <Common/Exception.h>
#include <Core/Field.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace DB::ErrorCodes
{
    extern const int TOO_DEEP_RECURSION;
    extern const int ARGUMENT_OUT_OF_BOUND;
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


/// A malformed binary Field that declares an arbitrarily deep nesting must be rejected with
/// TOO_DEEP_RECURSION, not run the reader past the stack guard page. The bytes are hand-built
/// because the write side also bounds depth, so a legitimately serialized value cannot be this deep.
GTEST_TEST(Field, ReadBinaryDeeplyNestedThrowsNotOverflow)
{
    WriteBufferFromOwnString wb;
    for (size_t i = 0; i < 500000; ++i)
    {
        writeBinary(UInt8(Field::Types::Array), wb);
        writeBinary(size_t(1), wb); /// one child
    }
    writeBinary(UInt8(Field::Types::Null), wb); /// leaf
    wb.finalize();

    ReadBufferFromString rb(wb.str());
    try
    {
        readFieldBinary(rb);
        FAIL() << "expected TOO_DEEP_RECURSION";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::TOO_DEEP_RECURSION);
    }

    /// A shallow array still round-trips.
    WriteBufferFromOwnString ok_wb;
    writeFieldBinary(Field(Array{Field(UInt64(1)), Field(UInt64(2))}), ok_wb);
    ok_wb.finalize();
    ReadBufferFromString ok_rb(ok_wb.str());
    Field roundtrip = readFieldBinary(ok_rb);
    ASSERT_EQ(roundtrip.safeGet<Array>().size(), 2u);
}


/// A binary Decimal whose scale exceeds the type's maximum precision is rejected at the
/// deserialization boundary; otherwise the out-of-range scale overruns a fixed-size buffer in
/// the decimal text writer.
GTEST_TEST(Field, ReadBinaryDecimalOutOfRangeScaleThrows)
{
    WriteBufferFromOwnString wb;
    writeBinary(UInt8(Field::Types::Decimal64), wb);
    writeBinary(Int64(123), wb); /// value (Decimal<Int64> is a POD 8-byte payload)
    writeBinary(UInt32(1000), wb); /// scale far above max_precision<Decimal64> = 18
    wb.finalize();

    ReadBufferFromString rb(wb.str());
    try
    {
        readFieldBinary(rb);
        FAIL() << "expected ARGUMENT_OUT_OF_BOUND";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    /// A valid scale round-trips.
    WriteBufferFromOwnString ok_wb;
    writeFieldBinary(Field(DecimalField<Decimal64>(Decimal64(12345), 4)), ok_wb);
    ok_wb.finalize();
    ReadBufferFromString ok_rb(ok_wb.str());
    Field roundtrip = readFieldBinary(ok_rb);
    ASSERT_EQ(roundtrip.safeGet<DecimalField<Decimal64>>().getScale(), 4u);
}


/// The text `Field` dump reader `restoreFromDump` shares the out-of-range-scale hazard with the
/// binary reader: it is reachable from the pre-auth native settings path for a `CUSTOM`-flagged
/// setting. The exponent form `Decimal64_'1e-1000'` builds a `DecimalField` with scale 1000 (the
/// `e`/`E` branch of `readDigits` bypasses the closing-quote check the plain-fraction form hits),
/// which an unfixed reader would hand to the decimal text writer. The scale must be rejected.
GTEST_TEST(Field, RestoreFromDumpDecimalOutOfRangeScaleThrows)
{
    try
    {
        Field::restoreFromDump("Decimal64_'1e-1000'");
        FAIL() << "expected ARGUMENT_OUT_OF_BOUND";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    /// An in-range exponent-form scale still round-trips.
    Field restored = Field::restoreFromDump("Decimal64_'1e-5'");
    ASSERT_EQ(restored.safeGet<DecimalField<Decimal64>>().getScale(), 5u);
}
