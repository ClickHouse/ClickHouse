#include <gtest/gtest.h>

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <Common/Exception.h>

using namespace std::literals;


TEST(PocoJSON, roundtrip)
{
    /** We patched Poco library to support certain invalid JSONs
      * in favor of perfect roundtrip of binary data, including zero bytes and invalid UTF-8.
      *
      * This is needed for consistency with ClickHouse's JSONEachRow format,
      * and to allow storing SQL queries (which can contain binary data) inside serialized JSONs
      * without extra encoding.
      *
      * Keep in mind that binary data inside string literals still has to be escaped, at least characters \ and "
      */
    try
    {
        std::string source_str("{\"hello\0ʏᑫᘈᶆᴋᾰ\\\"\": \"world\\n\\t\\rᖴᘍ᎐᙮ᗝᾴ\xFFwtf\xAA\xBB\xCC\xDD\"}"sv);
        std::string formatted_str("{\"hello\\u0000ʏᑫᘈᶆᴋᾰ\\\"\":\"world\\n\\t\\rᖴᘍ᎐᙮ᗝᾴ\xFFwtf\xAA\xBB\xCC\xDD\"}"sv);

        Poco::JSON::Parser parser;
        Poco::Dynamic::Var res_json = parser.parse(source_str);
        const Poco::JSON::Object::Ptr & object = res_json.extract<Poco::JSON::Object::Ptr>();

        std::stringstream destination;
        Poco::JSON::Stringifier::stringify(*object, destination);

        EXPECT_EQ(formatted_str, destination.str());

        Poco::Dynamic::Var res_json2 = parser.parse(destination.str());
        const Poco::JSON::Object::Ptr & object2 = res_json.extract<Poco::JSON::Object::Ptr>();

        std::stringstream destination2;
        Poco::JSON::Stringifier::stringify(*object2, destination2);

        EXPECT_EQ(destination.str(), destination2.str());
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
    }
}


TEST(PocoNumberParser, integerOverflow)
{
    /** `strToInt` accumulated digits checking only `result > max / base`, which still leaves room for
      * `max % base` in the last digit. Appending a larger digit overflowed the accumulator - undefined
      * behavior for a signed type - and the wrapped value was reported as successfully parsed.
      */
    Poco::Int64 signed_value = 0;
    EXPECT_TRUE(Poco::strToInt<Poco::Int64>("9223372036854775807", signed_value, 10));
    EXPECT_EQ(std::numeric_limits<Poco::Int64>::max(), signed_value);
    EXPECT_FALSE(Poco::strToInt<Poco::Int64>("9223372036854775808", signed_value, 10));
    EXPECT_FALSE(Poco::strToInt<Poco::Int64>("18446744073709551617", signed_value, 10));
    EXPECT_TRUE(Poco::strToInt<Poco::Int64>("7fffffffffffffff", signed_value, 16));
    EXPECT_EQ(std::numeric_limits<Poco::Int64>::max(), signed_value);
    EXPECT_FALSE(Poco::strToInt<Poco::Int64>("8000000000000000", signed_value, 16));

    /// The magnitude of the most negative value does not fit into the type itself, so it is
    /// accumulated as unsigned; it must still parse, and one below it must not.
    EXPECT_TRUE(Poco::strToInt<Poco::Int64>("-9223372036854775808", signed_value, 10));
    EXPECT_EQ(std::numeric_limits<Poco::Int64>::min(), signed_value);
    EXPECT_FALSE(Poco::strToInt<Poco::Int64>("-9223372036854775809", signed_value, 10));

    Poco::UInt64 unsigned_value = 0;
    EXPECT_TRUE(Poco::strToInt<Poco::UInt64>("18446744073709551615", unsigned_value, 10));
    EXPECT_EQ(std::numeric_limits<Poco::UInt64>::max(), unsigned_value);
    EXPECT_FALSE(Poco::strToInt<Poco::UInt64>("18446744073709551616", unsigned_value, 10));

    Poco::Int16 narrow_value = 0;
    EXPECT_TRUE(Poco::strToInt<Poco::Int16>("32767", narrow_value, 10));
    EXPECT_EQ(std::numeric_limits<Poco::Int16>::max(), narrow_value);
    EXPECT_FALSE(Poco::strToInt<Poco::Int16>("32768", narrow_value, 10));
    EXPECT_TRUE(Poco::strToInt<Poco::Int16>("-32768", narrow_value, 10));
    EXPECT_EQ(std::numeric_limits<Poco::Int16>::min(), narrow_value);
    EXPECT_FALSE(Poco::strToInt<Poco::Int16>("-32769", narrow_value, 10));

    /// A number out of the `Int64` range is a valid `UInt64`, and `Poco::JSON` falls back to it.
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var parsed = parser.parse(std::string(R"({"value": 18446744073709551615})"));
    EXPECT_EQ(
        std::numeric_limits<Poco::UInt64>::max(),
        parsed.extract<Poco::JSON::Object::Ptr>()->get("value").convert<Poco::UInt64>());

    /// A number out of the `UInt64` range is an error, not a wrapped-around value.
    EXPECT_THROW(parser.parse(std::string(R"({"value": 18446744073709551617})")), Poco::SyntaxException);
}
