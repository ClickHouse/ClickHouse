#include <gtest/gtest.h>

#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergJSON.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <boost/algorithm/string/replace.hpp>
#include <fmt/format.h>

#include <optional>
#include <sstream>

using namespace DB::Iceberg;

namespace
{

const std::vector<std::string> documents = {
    R"JSON({})JSON",
    R"JSON([])JSON",
    R"JSON({"a":{},"b":[],"c":[{}],"d":[[]],"e":{"x":[]}})JSON",
    R"JSON({"z":1,"a":2,"m":{"y":true,"b":false,"n":null}})JSON",
    R"JSON({"location":"s3://bucket/path/to/table/","metadata-file":"s3://b/metadata/v1.metadata.json"})JSON",
    R"JSON({"esc":"quote\" backslash\\ tab\t nl\n cr\r bs\b ff\f vt\u000b bell\u0007 nul\u0000 ctl\u0001\u001f del\u007f"})JSON",
    R"JSON({"unicode":"é 中 😀 é 中","empty":""})JSON",
    R"JSON({"ints":[0,-1,1,-9223372036854775808,9223372036854775807,9223372036854775808,18446744073709551615]})JSON",
    R"JSON({"floats":[0.0,-0.0,1.0,1.5,0.1,-2.25,1e300,1e-300,123456789012345678.0,1.5e15,1.5e16,0.000001,0.0000001]})JSON",
    R"JSON({"format-version":2,"table-uuid":"9f1c5a8e-1111-2222-3333-444455556666","snapshots":[{"snapshot-id":7764534597429801706,"parent-snapshot-id":1,"summary":{"operation":"append","added-records":"10"},"manifest-list":"/t/metadata/snap-1.avro","schema-id":0}],"schemas":[{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"},{"id":2,"name":"m","required":false,"type":{"type":"map","key-id":3,"key":"string","value-id":4,"value":{"type":"list","element-id":5,"element":"decimal(10, 2)","element-required":false},"value-required":false}}]}],"properties":{}})JSON",
    R"JSON([1,"x",{"k":[1,[2,[3,{}]]]},null,true])JSON",
};

std::string pocoStringify(const std::string & json, unsigned indent)
{
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var parsed = parser.parse(json);
    std::ostringstream out; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(parsed, out, indent);
    return boost::replace_all_copy(out.str(), "\\/", "/");
}

}

TEST(IcebergJSON, StringifyMatchesPoco)
{
    for (const auto & document : documents)
    {
        for (unsigned indent : {0u, 2u, 4u})
        {
            SCOPED_TRACE(fmt::format("indent {}: {}", indent, document));
            EXPECT_EQ(stringifyJSON(parseJSON(document), indent), pocoStringify(document, indent));
        }
    }
}

TEST(IcebergJSON, RoundTrip)
{
    for (const auto & document : documents)
    {
        auto parsed = parseJSON(document);
        auto text = stringifyJSON(parsed, 4);
        EXPECT_EQ(stringifyJSON(parseJSON(text), 4), text);
        EXPECT_EQ(parsed.clone(), parsed);
    }
}

TEST(IcebergJSON, BuildMatchesPoco)
{
    auto object = JSONObject::create();
    object->set("b", 1);
    object->set("a", "s3://x/y");
    object->set("size", static_cast<size_t>(5));
    object->set("flag", false);
    auto array = JSONArray::create();
    array->add(JSONObject::create());
    array->add(2.5);
    array->add(nullptr);
    object->set("arr", array);

    Poco::JSON::Object::Ptr poco_object = new Poco::JSON::Object;
    poco_object->set("b", 1);
    poco_object->set("a", "s3://x/y");
    poco_object->set("size", static_cast<size_t>(5));
    poco_object->set("flag", false);
    Poco::JSON::Array::Ptr poco_array = new Poco::JSON::Array;
    poco_array->add(Poco::JSON::Object::Ptr(new Poco::JSON::Object));
    poco_array->add(2.5);
    poco_array->add(Poco::Dynamic::Var());
    poco_object->set("arr", poco_array);

    std::ostringstream out; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(poco_object, out, 4);
    EXPECT_EQ(stringifyJSON(object, 4), boost::replace_all_copy(out.str(), "\\/", "/"));
}

namespace
{

template <typename T>
std::optional<T> pocoConvert(const Poco::Dynamic::Var & value)
{
    try
    {
        return value.convert<T>();
    }
    catch (...)
    {
        return std::nullopt;
    }
}

template <typename T>
std::optional<T> icebergConvert(const JSONValue & value)
{
    try
    {
        return value.getValue<T>();
    }
    catch (...)
    {
        return std::nullopt;
    }
}

}

TEST(IcebergJSON, ConversionsMatchPoco)
{
    const std::string document = R"JSON({"values":["123","-5","abc","","true","false","FALSE","0","1.5"," 7 ",
        0,1,-1,42,4294967296,9223372036854775807,18446744073709551615,1.0,2.5,-3.75,true,false]})JSON";

    Poco::JSON::Parser parser;
    auto poco_values = parser.parse(document).extract<Poco::JSON::Object::Ptr>()->getArray("values");
    auto values = parseJSONObject(document)->getArray("values");
    ASSERT_EQ(values->size(), poco_values->size());

    for (size_t i = 0; i < values->size(); ++i)
    {
        const auto & value = values->get(i);
        auto poco_value = poco_values->get(static_cast<unsigned>(i));
        SCOPED_TRACE(poco_value.toString());
        EXPECT_EQ(icebergConvert<Int64>(value), pocoConvert<Int64>(poco_value));
        EXPECT_EQ(icebergConvert<Int32>(value), pocoConvert<Int32>(poco_value));
        EXPECT_EQ(icebergConvert<UInt64>(value), pocoConvert<UInt64>(poco_value));
        EXPECT_EQ(icebergConvert<bool>(value), pocoConvert<bool>(poco_value));
        EXPECT_EQ(icebergConvert<std::string>(value), pocoConvert<std::string>(poco_value));
    }
}

TEST(IcebergJSON, ObjectAccess)
{
    auto object = parseJSONObject(R"({"a":1,"n":null,"o":{"x":2},"arr":[{"y":3},4]})");
    EXPECT_TRUE(object->has("a"));
    EXPECT_FALSE(object->has("missing"));
    EXPECT_TRUE(object->isNull("n"));
    EXPECT_TRUE(object->isNull("missing"));
    EXPECT_FALSE(object->isNull("a"));
    EXPECT_TRUE(object->get("missing").isNull());
    EXPECT_EQ(object->getObject("a"), nullptr);
    EXPECT_EQ(object->getArray("o"), nullptr);
    EXPECT_EQ(object->getObject("o")->getValue<Int32>("x"), 2);
    EXPECT_EQ(object->getArray("arr")->getObject(0)->getValue<Int64>("y"), 3);
    EXPECT_EQ(object->getArray("arr")->getObject(1), nullptr);
    EXPECT_EQ(object->getArray("arr")->getElement<Int64>(1), 4);
    EXPECT_ANY_THROW(object->getValue<Int64>("missing"));
    EXPECT_ANY_THROW(object->getValue<Int64>("n"));
    EXPECT_ANY_THROW(object->get("o").getArray());
    EXPECT_ANY_THROW(parseJSON("{"));
    EXPECT_ANY_THROW(parseJSONObject("[]"));
}

#endif
