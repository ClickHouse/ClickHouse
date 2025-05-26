#include <memory>
#include <DataTypes/convertYTsaurusDataType.h>
#include <DataTypes/DataTypeString.h>
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
#include "Common/IntervalKind.h"
#include <Common/Exception.h>
#include <fmt/format.h>

namespace DB::ErrorCodes {
    extern const int UNKNOWN_TYPE;
}

std::string createSimpleTypeJson(const std::string & simple_type, bool required) {
    std::string json = fmt::format("{{\"name\": \"id\", \"type\": \"{}\", \"required\": {}}}", simple_type, required);
    return json;
}

std::string createComplexTypeJson(const std::string & type_v3) {
    std::string json = fmt::format("{{\"name\": \"id\", \"type\": \"any\", \"required\": false, \"type_v3\": {}}}", type_v3);
    return json;
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

    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("uint64", false), std::make_shared<DB::DataTypeNullable>(std::make_shared<DB::DataTypeUInt64>())));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("uint32", true), std::make_shared<DB::DataTypeUInt32>()));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("uint16", false), std::make_shared<DB::DataTypeNullable>(std::make_shared<DB::DataTypeUInt16>())));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("uint8", true), std::make_shared<DB::DataTypeUInt8>()));

    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("int64", false), std::make_shared<DB::DataTypeNullable>(std::make_shared<DB::DataTypeInt64>())));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("int32", true), std::make_shared<DB::DataTypeInt32>()));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("int16", false), std::make_shared<DB::DataTypeNullable>(std::make_shared<DB::DataTypeInt16>())));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("int8", true), std::make_shared<DB::DataTypeInt8>()));

    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("float", true), std::make_shared<DB::DataTypeFloat32>()));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("double", true), std::make_shared<DB::DataTypeFloat64>()));

    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("boolean", true), std::make_shared<DB::DataTypeUInt8>()));

    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("string", true), std::make_shared<DB::DataTypeString>()));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("utf8", true), std::make_shared<DB::DataTypeString>()));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("json", true), std::make_shared<DB::DataTypeObject>(DB::DataTypeObject::SchemaFormat::JSON)));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("uuid", true), std::make_shared<DB::DataTypeUUID>()));

    // Dates
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("date32", true), std::make_shared<DB::DataTypeDate>()));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("date", true), std::make_shared<DB::DataTypeDate>()));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("datetime64", true), std::make_shared<DB::DataTypeDateTime>()));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("datetime", true), std::make_shared<DB::DataTypeDateTime>()));

    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("interval64", true), std::make_shared<DB::DataTypeInterval>(DB::IntervalKind::Kind::Microsecond)));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("interval", true), std::make_shared<DB::DataTypeInterval>(DB::IntervalKind::Kind::Microsecond)));

    ASSERT_THROW(checkColumnType(createSimpleTypeJson("any", false), std::make_shared<DB::DataTypeNothing>()), DB::Exception); // need to specify type_v3

    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("null", true), std::make_shared<DB::DataTypeNothing>()));

    ASSERT_THROW(checkColumnType(createSimpleTypeJson("incorrect", false), std::make_shared<DB::DataTypeNothing>()), DB::Exception); // wrong typename

    ASSERT_TRUE(checkColumnType("{\"type_v3\": \"bool\"}", std::make_shared<DB::DataTypeUInt8>()));
    ASSERT_TRUE(checkColumnType("{\"type_v3\": \"yson\"}", std::make_shared<DB::DataTypeObject>(DB::DataTypeObject::SchemaFormat::JSON)));
}

TEST(YTDataType, CheckComplexTypeConversation) {
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
        ASSERT_TRUE(checkColumnType(json, std::make_shared<DB::DataTypeDecimal<DB::Decimal32>>(1, 0)));
    }

    { // Optional
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
        ASSERT_TRUE(checkColumnType(json, std::make_shared<DB::DataTypeNullable>(std::make_shared<DB::DataTypeUInt8>())));
    }

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
        ASSERT_TRUE(checkColumnType(json, std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeFloat64>()))));
    }

    { // Struct
        // {
        //     "type_v3": {
        //         "type_name": "struct",
        //         "members": [
        //             {
        //                 "name": "some_name",
        //                 "type": "int32"
        //             }
        //         ]
        //     }
        // }
        Poco::JSON::Object::Ptr json(new Poco::JSON::Object());
        Poco::JSON::Object::Ptr s(new Poco::JSON::Object());
        Poco::JSON::Array::Ptr members(new Poco::JSON::Array());
        Poco::JSON::Object::Ptr member(new Poco::JSON::Object());
        member->set("name", "some_name");
        member->set("type", "int32");
        members->add(member);
        s->set("type_name", "struct");
        s->set("members", members);
        json->set("type_v3", s);
        ASSERT_THROW(checkColumnType(json, std::make_shared<DB::DataTypeNothing>()), DB::Exception);
    }

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
        std::vector<DB::DataTypePtr> data_types{std::make_shared<DB::DataTypeInt16>(), std::make_shared<DB::DataTypeUInt32>(), std::make_shared<DB::DataTypeInt16>()};
        ASSERT_TRUE(checkColumnType(json, std::make_shared<DB::DataTypeTuple>(data_types)));
    }

    { // Variant
        // {
        //     "type_v3": {
        //         "type_name": "variant",
        //         "elements": [
        //             {
        //                 "type": "string"
        //             },
        //             {
        //                 "type": "utf8"
        //             }
        //         ]
        //     }
        // }
    }


}
