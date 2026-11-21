#include <gtest/gtest.h>
#include <Common/Exception.h>
#include <Core/DecimalFunctions.h>
#include <Core/Field.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace DB::ErrorCodes
{
    extern const int TOO_DEEP_RECURSION;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int DECIMAL_OVERFLOW;
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

static void expectRestoreFromDumpError(const std::string & dump, int code)
{
    try
    {
        Field::restoreFromDump(dump);
        FAIL() << "expected error " << code << " for " << dump;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), code) << dump;
    }
}

GTEST_TEST(Field, RestoreFromDumpDecimalExtremeExponent)
{
    /// Negating `INT32_MIN` must not be reached.
    expectRestoreFromDumpError("Decimal64_'1e-2147483648'", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    /// The exponent sum overflows `int32_t` (only digits after the point decrease it).
    expectRestoreFromDumpError("Decimal64_'0.1e-2147483648'", ErrorCodes::CANNOT_PARSE_NUMBER);
    /// `scaleMultiplier` saturates beyond the precision, so `1 * max` must not be accepted as the value.
    expectRestoreFromDumpError("Decimal64_'1e100'", ErrorCodes::DECIMAL_OVERFLOW);
    expectRestoreFromDumpError("Decimal32_'1e2147483647'", ErrorCodes::DECIMAL_OVERFLOW);
    expectRestoreFromDumpError("Decimal256_'1e100'", ErrorCodes::DECIMAL_OVERFLOW);
    /// No digits at all.
    expectRestoreFromDumpError("Decimal64_'.'", ErrorCodes::CANNOT_PARSE_NUMBER);
    expectRestoreFromDumpError("Decimal64_'-'", ErrorCodes::CANNOT_PARSE_NUMBER);
    expectRestoreFromDumpError("Decimal64_''", ErrorCodes::CANNOT_PARSE_NUMBER);

    /// The largest exponents that fit still work, and zero takes any positive exponent.
    ASSERT_EQ(Field::restoreFromDump("Decimal64_'1e18'").safeGet<DecimalField<Decimal64>>().getValue(), Decimal64(1000000000000000000LL));
    ASSERT_EQ(Field::restoreFromDump("Decimal64_'1e-18'").safeGet<DecimalField<Decimal64>>().getScale(), 18u);
    ASSERT_EQ(Field::restoreFromDump("Decimal64_'0e2147483647'").safeGet<DecimalField<Decimal64>>().getValue(), Decimal64(0));
}

GTEST_TEST(Field, RestoreFromDumpDecimalMantissaOverflow)
{
    /// A mantissa that does not fit the native integer must not wrap (here to zero, skipping the exponent check).
    expectRestoreFromDumpError("Decimal32_'4294967296e2147483647'", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    expectRestoreFromDumpError("Decimal64_'18446744073709551616e2147483647'", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    expectRestoreFromDumpError("Decimal32_'2147483648'", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    expectRestoreFromDumpError("Decimal128_'170141183460469231731687303715884105728'", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    expectRestoreFromDumpError("Decimal256_'-57896044618658097711785492504343953926634992332820282019728792003956564819969'", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    /// The exponent must not wrap the wide types either.
    expectRestoreFromDumpError("Decimal128_'2e38'", ErrorCodes::DECIMAL_OVERFLOW);
    expectRestoreFromDumpError("Decimal128_'-18e37'", ErrorCodes::DECIMAL_OVERFLOW);
    expectRestoreFromDumpError("Decimal256_'6e76'", ErrorCodes::DECIMAL_OVERFLOW);
    expectRestoreFromDumpError("Decimal256_'-58e75'", ErrorCodes::DECIMAL_OVERFLOW);
    ASSERT_EQ(Field::restoreFromDump("Decimal128_'17e37'").safeGet<DecimalField<Decimal128>>().getValue(), Decimal128(Int128(17) * DecimalUtils::scaleMultiplier<Int128>(37)));
    ASSERT_EQ(Field::restoreFromDump("Decimal256_'-57e75'").safeGet<DecimalField<Decimal256>>().getValue().value, Int256(-57) * DecimalUtils::scaleMultiplier<Int256>(75));

    /// The full native range and the scale (trailing zeroes) round-trip through a dump.
    auto round_trip = []<typename T>(const DecimalField<T> & decimal)
    {
        const String dump = Field(decimal).dump();
        const auto restored = Field::restoreFromDump(dump).safeGet<DecimalField<T>>();
        EXPECT_EQ(restored.getValue(), decimal.getValue()) << dump;
        EXPECT_EQ(restored.getScale(), decimal.getScale()) << dump;
    };
    round_trip(DecimalField<Decimal32>(std::numeric_limits<Int32>::max(), 0));
    round_trip(DecimalField<Decimal32>(std::numeric_limits<Int32>::min(), 3));
    round_trip(DecimalField<Decimal64>(std::numeric_limits<Int64>::max(), 18));
    round_trip(DecimalField<Decimal64>(std::numeric_limits<Int64>::min(), 0));
    round_trip(DecimalField<Decimal128>(std::numeric_limits<Int128>::max(), 38));
    round_trip(DecimalField<Decimal128>(std::numeric_limits<Int128>::min(), 1));
    round_trip(DecimalField<Decimal256>(std::numeric_limits<Int256>::max(), 0));
    round_trip(DecimalField<Decimal256>(std::numeric_limits<Int256>::min(), 76));
    round_trip(DecimalField<Decimal64>(100, 2));
    round_trip(DecimalField<Decimal64>(100, 1));
    round_trip(DecimalField<Decimal64>(0, 3));
    round_trip(DecimalField<Decimal64>(-1000, 18));
}
