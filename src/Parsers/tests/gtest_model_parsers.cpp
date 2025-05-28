#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTCreateModelQuery.h>
#include <Parsers/ASTDropModelQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserCreateModelQuery.h>
#include <Parsers/ParserDropModelQuery.h>
#include <Parsers/parseQuery.h>
#include <base/types.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(ParserModel, Create)
{
    String input = "CREATE MODEL clf ALGORITHM 'xgboost' TARGET 'ttt' FROM TABLE my_table";

    ParserCreateModelQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0, 0);

    EXPECT_TRUE(ast);
    auto * model = ast->as<ASTCreateModelQuery>();
    EXPECT_TRUE(model);

    EXPECT_TRUE(model->model_name);
    auto * model_name = model->model_name->as<ASTIdentifier>();
    EXPECT_TRUE(model_name);
    EXPECT_EQ(model_name->name(), "clf");

    EXPECT_TRUE(model->algorithm);
    auto * algorithm = model->algorithm->as<ASTLiteral>();
    EXPECT_TRUE(algorithm);
    EXPECT_EQ(algorithm->value, "xgboost");

    EXPECT_FALSE(model->options);

    EXPECT_TRUE(model->target);
    auto * target = model->target->as<ASTLiteral>();
    EXPECT_TRUE(target);
    EXPECT_EQ(target->value, "ttt");

    EXPECT_TRUE(model->table_name);
    auto* from = model->table_name->as<ASTIdentifier>();
    EXPECT_TRUE(from);
    EXPECT_EQ(from->name(), "my_table");
}

TEST(ParserModel, CreateWithOptions)
{
    String input =
        "CREATE MODEL booster "
        "ALGORITHM 'lightgbm' "
        "OPTIONS("
            "lr = 0.1,"
            "max_depth = 6,"
            "comment = 'my_comment'"
        ")"
        "TARGET 'xyz' "
        "FROM TABLE tbl";

    ParserCreateModelQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0, 0);

    EXPECT_TRUE(ast);
    auto * model = ast->as<ASTCreateModelQuery>();
    EXPECT_TRUE(model);

    EXPECT_TRUE(model->model_name);
    auto * model_name = model->model_name->as<ASTIdentifier>();
    EXPECT_TRUE(model_name);
    EXPECT_EQ(model_name->name(), "booster");

    EXPECT_TRUE(model->algorithm);
    auto * algorithm = model->algorithm->as<ASTLiteral>();
    EXPECT_TRUE(algorithm);
    EXPECT_EQ(algorithm->value, "lightgbm");

    EXPECT_TRUE(model->options);
    auto * options = model->options->as<ASTSetQuery>();
    EXPECT_TRUE(options);

    const auto& changes = options->changes;
    EXPECT_EQ(changes.size(), 3);
    EXPECT_EQ(changes[0].name, "lr");
    EXPECT_EQ(changes[0].value.safeGet<Float64>(), 0.1);
    EXPECT_EQ(changes[1].name, "max_depth");
    EXPECT_EQ(changes[1].value.safeGet<UInt64>(), 6);
    EXPECT_EQ(changes[2].name, "comment");
    EXPECT_EQ(changes[2].value.safeGet<String>(), "my_comment");

    EXPECT_TRUE(model->target);
    auto * target = model->target->as<ASTLiteral>();
    EXPECT_TRUE(target);
    EXPECT_EQ(target->value, "xyz");

    EXPECT_TRUE(model->table_name);
    auto * from = model->table_name->as<ASTIdentifier>();
    EXPECT_TRUE(from);
    EXPECT_EQ(from->name(), "tbl");
}

TEST(ParserModel, Drop)
{
    String input = "DROP MODEL mdl";

    ParserDropModelQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0, 0);

    EXPECT_TRUE(ast);
    auto * model = ast->as<ASTDropModelQuery>();
    EXPECT_TRUE(model);

    EXPECT_TRUE(model->model_name);
    auto * model_name = model->model_name->as<ASTIdentifier>();
    EXPECT_TRUE(model_name);
    EXPECT_EQ(model_name->name(), "mdl");
}
