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

bool checkColumnType(const String & yt_json_str, const DB::DataTypePtr & correct_type) {
    Poco::JSON::Parser parser;
    Poco::JSON::Object::Ptr json = parser.parse(yt_json_str).extract<Poco::JSON::Object::Ptr>();
    return correct_type->equals(*DB::convertYTSchema(json));
}

TEST(YTDataType, CheckSimpleTypeConversation) {

    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("string", true), std::make_shared<DB::DataTypeString>()));
    ASSERT_TRUE(checkColumnType(createSimpleTypeJson("int64", false), std::make_shared<DB::DataTypeNullable>(std::make_shared<DB::DataTypeInt64>())));

    ASSERT_THROW(checkColumnType(createSimpleTypeJson("any", false), std::make_shared<DB::DataTypeNothing>()), DB::Exception);
}

TEST(YTDataType, CheckComplexTypeConversation) {

    ASSERT_TRUE(checkColumnType(createComplexTypeJson("{\"type_name\": \"decimal\", \"precision\": 1, \"scale\": 0}"), std::make_shared<DB::DataTypeDecimal<DB::Decimal32>>(1, 0)));

    ASSERT_TRUE(checkColumnType(createComplexTypeJson("{\"type_name\": \"optional\", \"item\": \"boolean\"}"), std::make_shared<DB::DataTypeNullable>(std::make_shared<DB::DataTypeUInt8>())));
}
