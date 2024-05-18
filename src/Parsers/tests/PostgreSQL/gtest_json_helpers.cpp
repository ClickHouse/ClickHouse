#include <gtest/gtest.h>
#include <Parsers/PostgreSQL/Common/Types.h>
#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>

using namespace DB::PostgreSQL;

TEST(TestJSONHelper, TestPrimitiveNode)
{
    Value val(123);
    const auto& n = std::make_shared<Node>(val);
    EXPECT_EQ(n->GetInt64Value(), 123);
}

TEST(TestJSONHelper, TestArrayNode)
{
    Value val(123);
    const auto& primitiveNode = std::make_shared<Node>("key", val);
    NodeArray arr = {primitiveNode};
    Value arrVal(arr, NodeType::Object);

    const auto& objNode = std::make_shared<Node>(arrVal);
    EXPECT_EQ(objNode->Size(), 1);
    
    const auto& childNode = (*objNode)["key"];
    EXPECT_EQ(childNode->GetInt64Value(), 123);
}

TEST(TestJSONHelper, TestBuildingJSONTree)
{
    String json = "{\"version\":160001,\"stmts\":[{\"stmt\":{\"SelectStmt\":{\"targetList\":[{\"ResTarget\":{\"val\":{\"A_Const\":{\"boolval\":{\"boolval\":true},\"location\":7}},\"location\":7}}],\"limitOption\":\"LIMIT_OPTION_DEFAULT\",\"op\":\"SETOP_NONE\"}}}]}";
    JSON::Element json_root;
    JSON parser;

    ASSERT_TRUE(parser.parse(json, json_root));

    const auto root = buildJSONTree(json_root);


    std::vector<std::string> expectedKeys = {"version", "stmts"};
    std::vector<std::string> childrenKeys = root->ListChildKeys();
    EXPECT_TRUE(childrenKeys == expectedKeys);

    const auto& stmtsNode = (*root)["stmts"];
    EXPECT_EQ(stmtsNode->Size(), 1);

    const auto& versionNode = (*root)["version"];
    EXPECT_EQ(versionNode->GetInt64Value(), 160001);
}
