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
