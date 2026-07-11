#include <gtest/gtest.h>
#include <Common/FieldBinaryEncoding.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}


void check(const Field & field)
{
//    std::cerr << "Check " << toString(field) << "\n";
    WriteBufferFromOwnString ostr;
    encodeField(field, ostr);
    ReadBufferFromString istr(ostr.str());
    Field decoded_field = decodeField(istr);
    ASSERT_TRUE(istr.eof());
    ASSERT_EQ(field, decoded_field);
}

GTEST_TEST(FieldBinaryEncoding, EncodeAndDecode)
{
    check(Null());
    check(POSITIVE_INFINITY);
    check(NEGATIVE_INFINITY);
    check(true);
    check(UInt64(42));
    check(Int64(-42));
    check(UInt128(42));
    check(Int128(-42));
    check(UInt256(42));
    check(Int256(-42));
    check(UUID(42));
    check(IPv4(42));
    check(IPv6(42));
    check(Float64(42.42));
    check(String("Hello, World!"));
    check(Array({Field(UInt64(42)), Field(UInt64(43))}));
    check(Tuple({Field(UInt64(42)), Field(Null()), Field(UUID(42)), Field(String("Hello, World!"))}));
    check(Map({Tuple{Field(UInt64(42)), Field(String("str_42"))}, Tuple{Field(UInt64(43)), Field(String("str_43"))}}));
    check(Object({{String("key_1"), Field(UInt64(42))}, {String("key_2"), Field(UInt64(43))}}));
    check(DecimalField<Decimal32>(4242, 3));
    check(DecimalField<Decimal64>(4242, 3));
    check(DecimalField<Decimal128>(Int128(4242), 3));
    check(DecimalField<Decimal256>(Int256(4242), 3));
    check(AggregateFunctionStateData{.name="some_name", .data="some_data"});
    try
    {
        check(CustomType());
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::UNSUPPORTED_METHOD);
    }

    check(Array({
        Tuple({Field(UInt64(42)), Map({Tuple{Field(UInt64(42)), Field(String("str_42"))}, Tuple{Field(UInt64(43)), Field(String("str_43"))}}), Field(UUID(42)), Field(String("Hello, World!"))}),
        Tuple({Field(UInt64(43)), Map({Tuple{Field(UInt64(43)), Field(String("str_43"))}, Tuple{Field(UInt64(44)), Field(String("str_44"))}}), Field(UUID(43)), Field(String("Hello, World 2!"))})
    }));
}

