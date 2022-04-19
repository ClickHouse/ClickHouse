#include <gtest/gtest.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/MySQL/ASTAlterCommand.h>
#include <Parsers/MySQL/ASTDeclareOption.h>

using namespace DB;
using namespace DB::MySQLParser;

static inline ASTPtr tryParserQuery(IParser & parser, const String & query)  // -V1071
{
    return parseQuery(parser, query.data(), query.data() + query.size(), "", 0, 0);
}

TEST(ParserAlterCommand, AddAlterCommand)
{
    ParserAlterCommand alter_p;

    ASTPtr ast = tryParserQuery(alter_p, "ADD column_name INT");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::ADD_COLUMN);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children.size(), 1);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children[0]->as<ASTDeclareColumn>()->name, "column_name");

    ast = tryParserQuery(alter_p, "ADD COLUMN column_name INT");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::ADD_COLUMN);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children.size(), 1);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children[0]->as<ASTDeclareColumn>()->name, "column_name");

    ast = tryParserQuery(alter_p, "ADD (column_name INT)");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::ADD_COLUMN);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children.size(), 1);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children[0]->as<ASTDeclareColumn>()->name, "column_name");

    ast = tryParserQuery(alter_p, "ADD COLUMN (column_name INT, column_name_1 INT)");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::ADD_COLUMN);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children.size(), 2);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children[0]->as<ASTDeclareColumn>()->name, "column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children[1]->as<ASTDeclareColumn>()->name, "column_name_1");

    ast = tryParserQuery(alter_p, "ADD INDEX (col_01, col_02(100), col_03 DESC) KEY_BLOCK_SIZE 3");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::ADD_INDEX);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->index_decl->index_columns->children.size(), 3);
    EXPECT_EQ(getIdentifierName(ast->as<ASTAlterCommand>()->index_decl->index_columns->children[0]), "col_01");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->index_decl->index_columns->children[1]->as<ASTFunction>()->name, "col_02");
    EXPECT_EQ(getIdentifierName(ast->as<ASTAlterCommand>()->index_decl->index_columns->children[2]), "col_03");
}

TEST(ParserAlterCommand, DropAlterCommand)
{
    ParserAlterCommand alter_p;

    ASTPtr ast = tryParserQuery(alter_p, "DROP CHECK constraint_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::DROP_CHECK);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->constraint_name, "constraint_name");

    ast = tryParserQuery(alter_p, "DROP CONSTRAINT constraint_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::DROP_CHECK);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->constraint_name, "constraint_name");

    ast = tryParserQuery(alter_p, "DROP KEY index_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::DROP_INDEX);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->index_type, "KEY");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->index_name, "index_name");

    ast = tryParserQuery(alter_p, "DROP INDEX index_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::DROP_INDEX);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->index_type, "KEY");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->index_name, "index_name");

    ast = tryParserQuery(alter_p, "DROP PRIMARY KEY");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::DROP_INDEX);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->index_type, "PRIMARY_KEY");

    ast = tryParserQuery(alter_p, "DROP FOREIGN KEY fk_symbol");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::DROP_INDEX);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->index_name, "fk_symbol");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->index_type, "FOREIGN");

    ast = tryParserQuery(alter_p, "DROP column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::DROP_COLUMN);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->column_name, "column_name");

    ast = tryParserQuery(alter_p, "DROP COLUMN column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::DROP_COLUMN);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->column_name, "column_name");
}

TEST(ParserAlterCommand, AlterAlterCommand)
{
    ParserAlterCommand alter_p;

    ASTPtr ast = tryParserQuery(alter_p, "ALTER CHECK constraint_name NOT ENFORCED");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_CHECK);
    EXPECT_TRUE(ast->as<ASTAlterCommand>()->not_check_enforced);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->constraint_name, "constraint_name");

    ast = tryParserQuery(alter_p, "ALTER CHECK constraint_name ENFORCED");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_CHECK);
    EXPECT_FALSE(ast->as<ASTAlterCommand>()->not_check_enforced);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->constraint_name, "constraint_name");

    ast = tryParserQuery(alter_p, "ALTER CONSTRAINT constraint_name NOT ENFORCED");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_CHECK);
    EXPECT_TRUE(ast->as<ASTAlterCommand>()->not_check_enforced);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->constraint_name, "constraint_name");

    ast = tryParserQuery(alter_p, "ALTER CONSTRAINT constraint_name ENFORCED");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_CHECK);
    EXPECT_FALSE(ast->as<ASTAlterCommand>()->not_check_enforced);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->constraint_name, "constraint_name");

    ast = tryParserQuery(alter_p, "ALTER INDEX index_name VISIBLE");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_INDEX_VISIBLE);
    EXPECT_TRUE(ast->as<ASTAlterCommand>()->index_visible);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->index_name, "index_name");

    ast = tryParserQuery(alter_p, "ALTER INDEX index_name INVISIBLE");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_INDEX_VISIBLE);
    EXPECT_FALSE(ast->as<ASTAlterCommand>()->index_visible);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->index_name, "index_name");

    ast = tryParserQuery(alter_p, "ALTER column_name SET DEFAULT other_column");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_COLUMN_DEFAULT);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->column_name, "column_name");
    EXPECT_EQ(getIdentifierName(ast->as<ASTAlterCommand>()->default_expression), "other_column");

    ast = tryParserQuery(alter_p, "ALTER column_name DROP DEFAULT");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::DROP_COLUMN_DEFAULT);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->column_name, "column_name");

    ast = tryParserQuery(alter_p, "ALTER COLUMN column_name SET DEFAULT other_column");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_COLUMN_DEFAULT);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->column_name, "column_name");
    EXPECT_EQ(getIdentifierName(ast->as<ASTAlterCommand>()->default_expression), "other_column");

    ast = tryParserQuery(alter_p, "ALTER COLUMN column_name DROP DEFAULT");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::DROP_COLUMN_DEFAULT);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->column_name, "column_name");
}

TEST(ParserAlterCommand, RenameAlterCommand)
{
    ParserAlterCommand alter_p;

    ASTPtr ast = tryParserQuery(alter_p, "RENAME COLUMN old_column_name TO new_column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::RENAME_COLUMN);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->old_name, "old_column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->column_name, "new_column_name");

    ast = tryParserQuery(alter_p, "RENAME KEY old_index_name TO new_index_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::RENAME_INDEX);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->old_name, "old_index_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->index_name, "new_index_name");

    ast = tryParserQuery(alter_p, "RENAME INDEX old_index_name TO new_index_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::RENAME_INDEX);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->old_name, "old_index_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->index_name, "new_index_name");

    ast = tryParserQuery(alter_p, "RENAME TO new_table_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::RENAME_TABLE);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->new_table_name, "new_table_name");

    ast = tryParserQuery(alter_p, "RENAME TO new_database_name.new_table_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::RENAME_TABLE);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->new_table_name, "new_table_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->new_database_name, "new_database_name");
}

TEST(ParserAlterCommand, ModifyAlterCommand)
{
    ParserAlterCommand alter_p;

    ASTPtr ast = tryParserQuery(alter_p, "MODIFY column_name INT");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_COLUMN);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->column_name, "");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->old_name, "");
    EXPECT_FALSE(ast->as<ASTAlterCommand>()->first);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children.size(), 1);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children[0]->as<ASTDeclareColumn>()->name, "column_name");

    ast = tryParserQuery(alter_p, "MODIFY column_name INT FIRST");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_COLUMN);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->column_name, "");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->old_name, "");
    EXPECT_TRUE(ast->as<ASTAlterCommand>()->first);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children.size(), 1);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children[0]->as<ASTDeclareColumn>()->name, "column_name");

    ast = tryParserQuery(alter_p, "MODIFY column_name INT AFTER other_column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_COLUMN);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->old_name, "");
    EXPECT_FALSE(ast->as<ASTAlterCommand>()->first);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->column_name, "other_column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children.size(), 1);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children[0]->as<ASTDeclareColumn>()->name, "column_name");

    ast = tryParserQuery(alter_p, "MODIFY COLUMN column_name INT AFTER other_column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_COLUMN);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->old_name, "");
    EXPECT_FALSE(ast->as<ASTAlterCommand>()->first);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->column_name, "other_column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children.size(), 1);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children[0]->as<ASTDeclareColumn>()->name, "column_name");
}

TEST(ParserAlterCommand, ChangeAlterCommand)
{
    ParserAlterCommand alter_p;

    ASTPtr ast = tryParserQuery(alter_p, "CHANGE old_column_name new_column_name INT");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_COLUMN);
    EXPECT_FALSE(ast->as<ASTAlterCommand>()->first);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->column_name, "");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->old_name, "old_column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children.size(), 1);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children[0]->as<ASTDeclareColumn>()->name, "new_column_name");

    ast = tryParserQuery(alter_p, "CHANGE old_column_name new_column_name INT FIRST");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_COLUMN);
    EXPECT_TRUE(ast->as<ASTAlterCommand>()->first);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->column_name, "");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->old_name, "old_column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children.size(), 1);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children[0]->as<ASTDeclareColumn>()->name, "new_column_name");

    ast = tryParserQuery(alter_p, "CHANGE old_column_name new_column_name INT AFTER other_column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_COLUMN);
    EXPECT_FALSE(ast->as<ASTAlterCommand>()->first);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->column_name, "other_column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->old_name, "old_column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children.size(), 1);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children[0]->as<ASTDeclareColumn>()->name, "new_column_name");

    ast = tryParserQuery(alter_p, "CHANGE COLUMN old_column_name new_column_name INT AFTER other_column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_COLUMN);
    EXPECT_FALSE(ast->as<ASTAlterCommand>()->first);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->column_name, "other_column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->old_name, "old_column_name");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children.size(), 1);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->additional_columns->children[0]->as<ASTDeclareColumn>()->name, "new_column_name");
}

TEST(ParserAlterCommand, AlterOptionsCommand)
{
    ParserAlterCommand alter_p;

    ASTPtr ast = tryParserQuery(alter_p, "ALGORITHM DEFAULT");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_PROPERTIES);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->properties->as<ASTDeclareOptions>()->changes.size(), 1);
    EXPECT_EQ(getIdentifierName(ast->as<ASTAlterCommand>()->properties->as<ASTDeclareOptions>()->changes["algorithm"]), "DEFAULT");

    ast = tryParserQuery(alter_p, "AUTO_INCREMENT 1 CHECKSUM 1");
    EXPECT_EQ(ast->as<ASTAlterCommand>()->type, ASTAlterCommand::MODIFY_PROPERTIES);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->properties->as<ASTDeclareOptions>()->changes.size(), 2);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->properties->as<ASTDeclareOptions>()->changes["checksum"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);
    EXPECT_EQ(ast->as<ASTAlterCommand>()->properties->as<ASTDeclareOptions>()->changes["auto_increment"]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);

    EXPECT_THROW(tryParserQuery(alter_p, "FORCE ALGORITHM DEFAULT"), Exception);
    EXPECT_THROW(tryParserQuery(alter_p, "ALGORITHM DEFAULT AUTO_INCREMENT 1"), Exception);
}

