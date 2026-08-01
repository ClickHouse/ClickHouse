#include <memory>
#include <DataTypes/convertYTsaurusDataType.h>
#include <Storages/ColumnsDescription.h>
#include <gtest/gtest.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/IntervalKind.h>
#include <Common/Exception.h>
#include <fmt/format.h>

namespace DB::ErrorCodes {
    extern const int UNKNOWN_TYPE;
}

#define CH_TYPE(type) DB::DataTypeFactory::instance().get(type)

std::string createSimpleTypeJson(const std::string & simple_type, bool required) {
    return fmt::format(
        R"({{"name": "id", "type": "{}", "required": {}}})",
        simple_type,
        required);
}

std::string createComplexTypeJson(const std::string & type_v3) {
    return fmt::format(
        R"({{"name": "id", "type": "any", "required": false, "type_v3": {}}})",
        type_v3);
}

bool checkColumnType(const Poco::JSON::Object::Ptr & json, const DB::DataTypePtr & correct_type) {
    return correct_type->equals(*DB::convertYTSchema(json));
}

bool checkColumnType(const String & yt_json_str, const DB::DataTypePtr & correct_type) {
    Poco::JSON::Parser parser;
    Poco::JSON::Object::Ptr json = parser.parse(yt_json_str).extract<Poco::JSON::Object::Ptr>();
    return checkColumnType(json, correct_type);
}


TEST(YTDataType, CheckSimpleTypeConversation) {

    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("uint64", false), CH_TYPE("Nullable(UInt64)")));

    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("uint32", true), CH_TYPE("UInt32")));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("uint16", false), CH_TYPE("Nullable(UInt16)")));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("uint8", true), CH_TYPE("UInt8")));

    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("int64", false), CH_TYPE("Nullable(Int64)")));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("int32", true), CH_TYPE("Int32")));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("int16", false), CH_TYPE("Nullable(Int16)")));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("int8", true), CH_TYPE("Int8")));

    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("float", true), CH_TYPE("Float32")));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("double", true), CH_TYPE("Float64")));

    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("boolean", true), CH_TYPE("Bool")));

    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("string", true), CH_TYPE("String")));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("utf8", true), CH_TYPE("String")));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("json", true), CH_TYPE("JSON")));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("uuid", true), CH_TYPE("UUID")));

    // // Dates
    // Commented until https://github.com/ClickHouse/ClickHouse/issues/82003 not resolved
    // ASSERT_TRUE(checkColumnType(createSimpleTypeJson("date32", true), CH_TYPE("Date")));
    // ASSERT_TRUE(checkColumnType(createSimpleTypeJson("date", true), CH_TYPE("Date")));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("datetime64", true), CH_TYPE("DateTime64(0)")));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("datetime", true), CH_TYPE("DateTime64(0)")));

    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("interval64", true), CH_TYPE("Int64")));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("interval", true), CH_TYPE("Int64")));

    ASSERT_THROW(checkColumnType(createSimpleTypeJson("any", true), CH_TYPE("JSON")), DB::Exception);

    ASSERT_THROW(checkColumnType(createSimpleTypeJson("null", true), CH_TYPE("Nothing")), DB::Exception);
    ASSERT_THROW(checkColumnType(createSimpleTypeJson("void", true), CH_TYPE("Nothing")), DB::Exception);
    ASSERT_THROW(checkColumnType(createSimpleTypeJson("incorrect", false), CH_TYPE("Nothing")), DB::Exception); // wrong typename

    ASSERT_TRUE(checkColumnType("{\"type_v3\": \"bool\"}", CH_TYPE("Bool")));
}

TEST(YTDataType, CheckDecimal) {
    { // Decimal
        // {
        //     "type_v3": {
        //         "type_name": "decimal",
        //         "precision": 1,
        //         "scale": 0
        //     }
        // }
        Poco::JSON::Object::Ptr json(new Poco::JSON::Object());
        Poco::JSON::Object::Ptr decimal(new Poco::JSON::Object());

        decimal->set("type_name", "decimal");
        decimal->set("precision", size_t(1));
        decimal->set("scale", size_t(0));
        json->set("type_v3", decimal);
        ASSERT_TRUE(checkColumnType(json, CH_TYPE("Decimal(1, 0)")));
    }
}

TEST(YTDataType, CheckOptional) {

    { // Optional
        {
            // {
            //     "type_v3": {
            //         "type_name": "optional",
            //         "item": "boolean"
            //     }
            // }
            Poco::JSON::Object::Ptr json(new Poco::JSON::Object());
            Poco::JSON::Object::Ptr optional(new Poco::JSON::Object());
            optional->set("type_name", "optional");
            optional->set("item", "boolean");
            json->set("type_v3", optional);
            ASSERT_TRUE(checkColumnType(json, CH_TYPE("Nullable(UInt8)")));
        }
        {
            // {
            //     "type_v3": {
            //         "type_name": "optional",
            //         "item": {
            //             "type_name": "optional",
            //             "item": "double"
            //         }
            //     }
            // }

            Poco::JSON::Object::Ptr json(new Poco::JSON::Object());
            Poco::JSON::Object::Ptr optional1(new Poco::JSON::Object());
            Poco::JSON::Object::Ptr optional2(new Poco::JSON::Object());
            optional2->set("type_name", "optional");
            optional2->set("item", "double");
            optional1->set("type_name", "optional");
            optional1->set("item", optional2);
            json->set("type_v3", optional1);
            ASSERT_THROW(checkColumnType(json, CH_TYPE("Nullable(Nullable(Float64))")), DB::Exception);
        }

        {
            // {
            //     "type_v3": {
            //         "type_name": "optional",
            //         "item": {
            //             "type_name": "list",
            //             "item": "double"
            //         }
            //     }
            // }
            Poco::JSON::Object::Ptr json(new Poco::JSON::Object());
            Poco::JSON::Object::Ptr optional1(new Poco::JSON::Object());
            Poco::JSON::Object::Ptr optional2(new Poco::JSON::Object());
            optional2->set("type_name", "list");
            optional2->set("item", "double");
            optional1->set("type_name", "optional");
            optional1->set("item", optional2);
            json->set("type_v3", optional1);
            ASSERT_THROW(checkColumnType(json, CH_TYPE("Nullable(Array(Float64))")), DB::Exception);
        }

        {
            // {
            //     "type_v3": {
            //         "type_name": "optional",
            //         "item": {
            //             "type_name": "tuple",
            //             "elements": [
            //                 {
            //                     "type": "string"
            //                 },
            //                 {
            //                     "type": "utf8"
            //                 }
            //             ]
            //         }
            //     }
            // }
            Poco::JSON::Object::Ptr json(new Poco::JSON::Object());
            Poco::JSON::Object::Ptr optional(new Poco::JSON::Object());
            Poco::JSON::Object::Ptr tuple(new Poco::JSON::Object());
            Poco::JSON::Array::Ptr elements(new Poco::JSON::Array());
            {
                Poco::JSON::Object::Ptr element(new Poco::JSON::Object());
                element->set("type", "string");
                elements->add(element);
            }
            {
                Poco::JSON::Object::Ptr element(new Poco::JSON::Object());
                element->set("type", "utf8");
                elements->add(element);
            }

            tuple->set("type_name", "tuple");
            tuple->set("elements", elements);
            optional->set("type_name", "optional");
            optional->set("item", tuple);
            json->set("type_v3", optional);
            
            ASSERT_THROW(checkColumnType(json, CH_TYPE("Nullable(Tuple(String, String))")), DB::Exception);
        }
    }
}
TEST(YTDataType, CheckList) {
    { // List
        // {
        //     "type_v3": {
        //         "type_name": "list",
        //         "item": {
        //             "type_name": "list",
        //             "item": "double"
        //         }
        //     }
        // }
        Poco::JSON::Object::Ptr json(new Poco::JSON::Object());
        Poco::JSON::Object::Ptr list1(new Poco::JSON::Object());
        Poco::JSON::Object::Ptr list2(new Poco::JSON::Object());
        list2->set("type_name", "list");
        list2->set("item", "double");
        list1->set("type_name", "list");
        list1->set("item", list2);
        json->set("type_v3", list1);
        ASSERT_TRUE(checkColumnType(json, CH_TYPE("Array(Array(Float64))")));
    }
}

TEST(YTDataType, CheckStruct) {
    { // Struct
        // {
        //     "type_v3": {
        //         "type_name": "struct",
        //         "members": [
        //             {
        //                 "name": "key",
        //                 "type": "int32"
        //             },
        //             {
        //                 "name": "value",
        //                 "type": "string"
        //             }
        //         ]
        //     }
        // }
        Poco::JSON::Object::Ptr json(new Poco::JSON::Object());
        Poco::JSON::Object::Ptr s(new Poco::JSON::Object());
        Poco::JSON::Array::Ptr members(new Poco::JSON::Array());
        {
            Poco::JSON::Object::Ptr member(new Poco::JSON::Object());
            member->set("name", "key");
            member->set("type", "int32");
            members->add(member);
        }
        {
            Poco::JSON::Object::Ptr member(new Poco::JSON::Object());
            member->set("name", "value");
            member->set("type", "string");
            members->add(member);
        }
        s->set("type_name", "struct");
        s->set("members", members);
        json->set("type_v3", s);
        ASSERT_TRUE(checkColumnType(
            json,
            CH_TYPE("Tuple(key Int32, value String)")
        ));
    }
}

TEST(YTDataType, CheckTuple) {

    { // Tuple
        // {
        //     "type_v3": {
        //         "type_name": "tuple",
        //         "elements": [
        //             {
        //                 "type": "int16"
        //             },
        //             {
        //                 "type": "uint32"
        //             },
        //             {
        //                 "type": "int16"
        //             }
        //         ]
        //     }
        // }
        Poco::JSON::Object::Ptr json(new Poco::JSON::Object());
        Poco::JSON::Object::Ptr tuple(new Poco::JSON::Object());
        Poco::JSON::Array::Ptr elements(new Poco::JSON::Array());

        {
            Poco::JSON::Object::Ptr element(new Poco::JSON::Object());
            element->set("type", "int16");
            elements->add(element);
        }
        {
            Poco::JSON::Object::Ptr element(new Poco::JSON::Object());
            element->set("type", "uint32");
            elements->add(element);
        }
        {
            Poco::JSON::Object::Ptr element(new Poco::JSON::Object());
            element->set("type", "int16");
            elements->add(element);
        }

        tuple->set("type_name", "tuple");
        tuple->set("elements", elements);
        json->set("type_v3", tuple);
        ASSERT_TRUE(checkColumnType(json, CH_TYPE("Tuple(Int16, UInt32, Int16)")));
    }
}

TEST(YTDataType, CheckVariant) {

    { // Variant
        // {
        //     "type_v3": {
        //         "type_name": "variant",
        //         "elements": [
        //             {
        //                 "type": "string"
        //             },
        //             {
        //                 "type": "int64"
        //             }
        //         ]
        //     }
        // }

        Poco::JSON::Object::Ptr json(new Poco::JSON::Object());
        Poco::JSON::Object::Ptr variant(new Poco::JSON::Object());
        Poco::JSON::Array::Ptr elements(new Poco::JSON::Array());

        {
            Poco::JSON::Object::Ptr element(new Poco::JSON::Object());
            element->set("type", "string");
            elements->add(element);
        }
        {
            Poco::JSON::Object::Ptr element(new Poco::JSON::Object());
            element->set("type", "int64");
            elements->add(element);
        }
        variant->set("type_name", "variant");
        variant->set("elements", elements);
        json->set("type_v3", variant);


        ASSERT_TRUE(checkColumnType(json, CH_TYPE("Variant(String, Int64)")));
    }
}
TEST(YTDataType, CheckTagged) {
    
    { // Tagged
        
        // {
        //     "type_v3": {
        //         "type_name": "tagged",
        //         "tag": "image/svg",
        //         "item": "string"
        //     }
        // }

        Poco::JSON::Object::Ptr json(new Poco::JSON::Object());
        Poco::JSON::Object::Ptr tagged(new Poco::JSON::Object());
        tagged->set("type_name", "tagged");
        tagged->set("tag", "image/svg");
        tagged->set("type", "string");
        json->set("type_v3", tagged);

        ASSERT_TRUE(checkColumnType(json, CH_TYPE("String")));

    }
}
