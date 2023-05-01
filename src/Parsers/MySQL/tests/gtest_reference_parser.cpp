#include <gtest/gtest.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/MySQL/ASTDeclareReference.h>

using namespace DB;
using namespace DB::MySQLParser;

TEST(ParserReference, SimpleReference)
{
    ParserDeclareReference p_reference;

    String reference_01 = "REFERENCES table_name (ref_col_01)";
    ASTPtr ast_reference_01 = parseQuery(p_reference, reference_01.data(), reference_01.data() + reference_01.size(), "", 0, 0);
    EXPECT_EQ(ast_reference_01->as<ASTDeclareReference>()->reference_table_name, "table_name");
    EXPECT_EQ(ast_reference_01->as<ASTDeclareReference>()->reference_expression->as<ASTIdentifier>()->name(), "ref_col_01");

    String reference_02 = "REFERENCES table_name (ref_col_01, ref_col_02)";
    ASTPtr ast_reference_02 = parseQuery(p_reference, reference_02.data(), reference_02.data() + reference_02.size(), "", 0, 0);
    EXPECT_EQ(ast_reference_02->as<ASTDeclareReference>()->reference_table_name, "table_name");
    ASTPtr arguments = ast_reference_02->as<ASTDeclareReference>()->reference_expression->as<ASTFunction>()->arguments;
    EXPECT_EQ(arguments->children[0]->as<ASTIdentifier>()->name(), "ref_col_01");
    EXPECT_EQ(arguments->children[1]->as<ASTIdentifier>()->name(), "ref_col_02");
}

TEST(ParserReference, ReferenceDifferenceKind)
{
    ParserDeclareReference p_reference;
    String reference_01 = "REFERENCES table_name (ref_col_01) MATCH FULL";
    ASTPtr ast_reference_01 = parseQuery(p_reference, reference_01.data(), reference_01.data() + reference_01.size(), "", 0, 0);
    EXPECT_EQ(ast_reference_01->as<ASTDeclareReference>()->reference_table_name, "table_name");
    EXPECT_EQ(ast_reference_01->as<ASTDeclareReference>()->reference_expression->as<ASTIdentifier>()->name(), "ref_col_01");
    EXPECT_EQ(ast_reference_01->as<ASTDeclareReference>()->kind, ASTDeclareReference::MATCH_FULL);

    String reference_02 = "REFERENCES table_name (ref_col_01) MATCH PARTIAL";
    ASTPtr ast_reference_02 = parseQuery(p_reference, reference_02.data(), reference_02.data() + reference_02.size(), "", 0, 0);
    EXPECT_EQ(ast_reference_02->as<ASTDeclareReference>()->reference_table_name, "table_name");
    EXPECT_EQ(ast_reference_02->as<ASTDeclareReference>()->reference_expression->as<ASTIdentifier>()->name(), "ref_col_01");
    EXPECT_EQ(ast_reference_02->as<ASTDeclareReference>()->kind, ASTDeclareReference::MATCH_PARTIAL);

    String reference_03 = "REFERENCES table_name (ref_col_01) MATCH SIMPLE";
    ASTPtr ast_reference_03 = parseQuery(p_reference, reference_03.data(), reference_03.data() + reference_03.size(), "", 0, 0);
    EXPECT_EQ(ast_reference_03->as<ASTDeclareReference>()->reference_table_name, "table_name");
    EXPECT_EQ(ast_reference_03->as<ASTDeclareReference>()->reference_expression->as<ASTIdentifier>()->name(), "ref_col_01");
    EXPECT_EQ(ast_reference_03->as<ASTDeclareReference>()->kind, ASTDeclareReference::MATCH_SIMPLE);
}

TEST(ParserReference, ReferenceDifferenceOption)
{
    ParserDeclareReference p_reference;
    String reference_01 = "REFERENCES table_name (ref_col_01) MATCH FULL ON DELETE RESTRICT ON UPDATE RESTRICT";
    ASTPtr ast_reference_01 = parseQuery(p_reference, reference_01.data(), reference_01.data() + reference_01.size(), "", 0, 0);
    EXPECT_EQ(ast_reference_01->as<ASTDeclareReference>()->reference_table_name, "table_name");
    EXPECT_EQ(ast_reference_01->as<ASTDeclareReference>()->reference_expression->as<ASTIdentifier>()->name(), "ref_col_01");
    EXPECT_EQ(ast_reference_01->as<ASTDeclareReference>()->kind, ASTDeclareReference::MATCH_FULL);
    EXPECT_EQ(ast_reference_01->as<ASTDeclareReference>()->on_delete_option, ASTDeclareReference::RESTRICT);
    EXPECT_EQ(ast_reference_01->as<ASTDeclareReference>()->on_update_option, ASTDeclareReference::RESTRICT);

    String reference_02 = "REFERENCES table_name (ref_col_01) MATCH FULL ON DELETE CASCADE ON UPDATE CASCADE";
    ASTPtr ast_reference_02 = parseQuery(p_reference, reference_02.data(), reference_02.data() + reference_02.size(), "", 0, 0);
    EXPECT_EQ(ast_reference_02->as<ASTDeclareReference>()->reference_table_name, "table_name");
    EXPECT_EQ(ast_reference_02->as<ASTDeclareReference>()->reference_expression->as<ASTIdentifier>()->name(), "ref_col_01");
    EXPECT_EQ(ast_reference_02->as<ASTDeclareReference>()->kind, ASTDeclareReference::MATCH_FULL);
    EXPECT_EQ(ast_reference_02->as<ASTDeclareReference>()->on_delete_option, ASTDeclareReference::CASCADE);
    EXPECT_EQ(ast_reference_02->as<ASTDeclareReference>()->on_update_option, ASTDeclareReference::CASCADE);

    String reference_03 = "REFERENCES table_name (ref_col_01) MATCH FULL ON DELETE SET NULL ON UPDATE SET NULL";
    ASTPtr ast_reference_03 = parseQuery(p_reference, reference_03.data(), reference_03.data() + reference_03.size(), "", 0, 0);
    EXPECT_EQ(ast_reference_03->as<ASTDeclareReference>()->reference_table_name, "table_name");
    EXPECT_EQ(ast_reference_03->as<ASTDeclareReference>()->reference_expression->as<ASTIdentifier>()->name(), "ref_col_01");
    EXPECT_EQ(ast_reference_03->as<ASTDeclareReference>()->kind, ASTDeclareReference::MATCH_FULL);
    EXPECT_EQ(ast_reference_03->as<ASTDeclareReference>()->on_delete_option, ASTDeclareReference::SET_NULL);
    EXPECT_EQ(ast_reference_03->as<ASTDeclareReference>()->on_update_option, ASTDeclareReference::SET_NULL);

    String reference_04 = "REFERENCES table_name (ref_col_01) MATCH FULL ON UPDATE NO ACTION ON DELETE NO ACTION";
    ASTPtr ast_reference_04 = parseQuery(p_reference, reference_04.data(), reference_04.data() + reference_04.size(), "", 0, 0);
    EXPECT_EQ(ast_reference_04->as<ASTDeclareReference>()->reference_table_name, "table_name");
    EXPECT_EQ(ast_reference_04->as<ASTDeclareReference>()->reference_expression->as<ASTIdentifier>()->name(), "ref_col_01");
    EXPECT_EQ(ast_reference_04->as<ASTDeclareReference>()->kind, ASTDeclareReference::MATCH_FULL);
    EXPECT_EQ(ast_reference_04->as<ASTDeclareReference>()->on_delete_option, ASTDeclareReference::NO_ACTION);
    EXPECT_EQ(ast_reference_04->as<ASTDeclareReference>()->on_update_option, ASTDeclareReference::NO_ACTION);

    String reference_05 = "REFERENCES table_name (ref_col_01) MATCH FULL ON UPDATE SET DEFAULT ON DELETE SET DEFAULT";
    ASTPtr ast_reference_05 = parseQuery(p_reference, reference_05.data(), reference_05.data() + reference_05.size(), "", 0, 0);
    EXPECT_EQ(ast_reference_05->as<ASTDeclareReference>()->reference_table_name, "table_name");
    EXPECT_EQ(ast_reference_05->as<ASTDeclareReference>()->reference_expression->as<ASTIdentifier>()->name(), "ref_col_01");
    EXPECT_EQ(ast_reference_05->as<ASTDeclareReference>()->kind, ASTDeclareReference::MATCH_FULL);
    EXPECT_EQ(ast_reference_05->as<ASTDeclareReference>()->on_delete_option, ASTDeclareReference::SET_DEFAULT);
    EXPECT_EQ(ast_reference_05->as<ASTDeclareReference>()->on_update_option, ASTDeclareReference::SET_DEFAULT);
}

