#include <Parsers/ParserAlterQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{

bool ParserAlterCommand::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto command = std::make_shared<ASTAlterCommand>();
    node = command;

    ParserKeyword s_add_column("ADD COLUMN");
    ParserKeyword s_drop_column("DROP COLUMN");
    ParserKeyword s_clear_column("CLEAR COLUMN");
    ParserKeyword s_modify_column("MODIFY COLUMN");
    ParserKeyword s_modify_primary_key("MODIFY PRIMARY KEY");

    ParserKeyword s_attach_partition("ATTACH PARTITION");
    ParserKeyword s_detach_partition("DETACH PARTITION");
    ParserKeyword s_drop_partition("DROP PARTITION");
    ParserKeyword s_attach_part("ATTACH PART");
    ParserKeyword s_fetch_partition("FETCH PARTITION");
    ParserKeyword s_freeze_partition("FREEZE PARTITION");
    ParserKeyword s_partition("PARTITION");

    ParserKeyword s_after("AFTER");
    ParserKeyword s_from("FROM");
    ParserKeyword s_in_partition("IN PARTITION");
    ParserKeyword s_with("WITH");
    ParserKeyword s_name("NAME");

    ParserKeyword s_delete_where("DELETE WHERE");
    ParserKeyword s_update("UPDATE");
    ParserKeyword s_where("WHERE");

    ParserCompoundIdentifier parser_name;
    ParserStringLiteral parser_string_literal;
    ParserCompoundColumnDeclaration parser_col_decl;
    ParserPartition parser_partition;
    ParserExpression parser_exp_elem;
    ParserList parser_assignment_list(
        std::make_unique<ParserAssignment>(), std::make_unique<ParserToken>(TokenType::Comma),
        /* allow_empty = */ false);

    if (s_add_column.ignore(pos, expected))
    {
        if (!parser_col_decl.parse(pos, command->col_decl, expected))
            return false;

        if (s_after.ignore(pos, expected))
        {
            if (!parser_name.parse(pos, command->column, expected))
                return false;
        }

        command->type = ASTAlterCommand::ADD_COLUMN;
    }
    else if (s_drop_partition.ignore(pos, expected))
    {
        if (!parser_partition.parse(pos, command->partition, expected))
            return false;

        command->type = ASTAlterCommand::DROP_PARTITION;
    }
    else if (s_drop_column.ignore(pos, expected))
    {
        if (!parser_name.parse(pos, command->column, expected))
            return false;

        command->type = ASTAlterCommand::DROP_COLUMN;
        command->detach = false;
    }
    else if (s_clear_column.ignore(pos, expected))
    {
        if (!parser_name.parse(pos, command->column, expected))
            return false;

        command->type = ASTAlterCommand::DROP_COLUMN;
        command->clear_column = true;
        command->detach = false;

        if (s_in_partition.ignore(pos, expected))
        {
            if (!parser_partition.parse(pos, command->partition, expected))
                return false;
        }
    }
    else if (s_detach_partition.ignore(pos, expected))
    {
        if (!parser_partition.parse(pos, command->partition, expected))
            return false;

        command->type = ASTAlterCommand::DROP_PARTITION;
        command->detach = true;
    }
    else if (s_attach_partition.ignore(pos, expected))
    {
        if (!parser_partition.parse(pos, command->partition, expected))
            return false;

        if (s_from.ignore(pos))
        {
            if (!parseDatabaseAndTableName(pos, expected, command->from_database, command->from_table))
                return false;

            command->replace = false;
            command->type = ASTAlterCommand::REPLACE_PARTITION;
        }
        else
        {
            command->type = ASTAlterCommand::ATTACH_PARTITION;
        }
    }
    else if (ParserKeyword{"REPLACE PARTITION"}.ignore(pos, expected))
    {
        if (!parser_partition.parse(pos, command->partition, expected))
            return false;

        if (!s_from.ignore(pos, expected))
            return false;

        if (!parseDatabaseAndTableName(pos, expected, command->from_database, command->from_table))
            return false;

        command->replace = true;
        command->type = ASTAlterCommand::REPLACE_PARTITION;
    }
    else if (s_attach_part.ignore(pos, expected))
    {
        if (!parser_string_literal.parse(pos, command->partition, expected))
            return false;

        command->part = true;
        command->type = ASTAlterCommand::ATTACH_PARTITION;
    }
    else if (s_fetch_partition.ignore(pos, expected))
    {
        if (!parser_partition.parse(pos, command->partition, expected))
            return false;

        if (!s_from.ignore(pos, expected))
            return false;

        ASTPtr ast_from;
        if (!parser_string_literal.parse(pos, ast_from, expected))
            return false;

        command->from = typeid_cast<const ASTLiteral &>(*ast_from).value.get<const String &>();
        command->type = ASTAlterCommand::FETCH_PARTITION;
    }
    else if (s_freeze_partition.ignore(pos, expected))
    {
        if (!parser_partition.parse(pos, command->partition, expected))
            return false;

        /// WITH NAME 'name' - place local backup to directory with specified name
        if (s_with.ignore(pos, expected))
        {
            if (!s_name.ignore(pos, expected))
                return false;

            ASTPtr ast_with_name;
            if (!parser_string_literal.parse(pos, ast_with_name, expected))
                return false;

            command->with_name = typeid_cast<const ASTLiteral &>(*ast_with_name).value.get<const String &>();
        }

        command->type = ASTAlterCommand::FREEZE_PARTITION;
    }
    else if (s_modify_column.ignore(pos, expected))
    {
        if (!parser_col_decl.parse(pos, command->col_decl, expected))
            return false;

        command->type = ASTAlterCommand::MODIFY_COLUMN;
    }
    else if (s_modify_primary_key.ignore(pos, expected))
    {
        if (pos->type != TokenType::OpeningRoundBracket)
            return false;
        ++pos;

        if (!ParserNotEmptyExpressionList(false).parse(pos, command->primary_key, expected))
            return false;

        if (pos->type != TokenType::ClosingRoundBracket)
            return false;
        ++pos;

        command->type = ASTAlterCommand::MODIFY_PRIMARY_KEY;
    }
    else if (s_delete_where.ignore(pos, expected))
    {
        if (!parser_exp_elem.parse(pos, command->predicate, expected))
            return false;

        command->type = ASTAlterCommand::DELETE;
    }
    else if (s_update.ignore(pos, expected))
    {
        if (!parser_assignment_list.parse(pos, command->update_assignments, expected))
            return false;

        if (!s_where.ignore(pos, expected))
            return false;

        if (!parser_exp_elem.parse(pos, command->predicate, expected))
            return false;

        command->type = ASTAlterCommand::UPDATE;
    }
    else
        return false;

    if (command->col_decl)
        command->children.push_back(command->col_decl);
    if (command->column)
        command->children.push_back(command->column);
    if (command->primary_key)
        command->children.push_back(command->primary_key);
    if (command->partition)
        command->children.push_back(command->partition);
    if (command->predicate)
        command->children.push_back(command->predicate);
    if (command->update_assignments)
        command->children.push_back(command->update_assignments);

    return true;
}


bool ParserAlterCommandList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto command_list = std::make_shared<ASTAlterCommandList>();
    node = command_list;

    ParserToken s_comma(TokenType::Comma);
    ParserAlterCommand p_command;

    do
    {
        ASTPtr command;
        if (!p_command.parse(pos, command, expected))
            return false;

        command_list->add(command);
    }
    while (s_comma.ignore(pos, expected));

    return true;
}


bool ParserAssignment::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto assignment = std::make_shared<ASTAssignment>();
    node = assignment;

    ParserIdentifier p_identifier;
    ParserToken s_equals(TokenType::Equals);
    ParserExpression p_expression;

    ASTPtr column;
    if (!p_identifier.parse(pos, column, expected))
        return false;

    if (!s_equals.ignore(pos, expected))
        return false;

    if (!p_expression.parse(pos, assignment->expression, expected))
        return false;

    assignment->column_name = typeid_cast<const ASTIdentifier &>(*column).name;
    if (assignment->expression)
        assignment->children.push_back(assignment->expression);

    return true;
}


bool ParserAlterQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTAlterQuery>();
    node = query;

    ParserKeyword s_alter_table("ALTER TABLE");
    if (!s_alter_table.ignore(pos, expected))
        return false;

    if (!parseDatabaseAndTableName(pos, expected, query->database, query->table))
        return false;

    String cluster_str;
    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }
    query->cluster = cluster_str;

    ParserAlterCommandList p_command_list;
    ASTPtr command_list;
    if (!p_command_list.parse(pos, command_list, expected))
        return false;

    query->set(query->command_list, command_list);

    return true;
}

}
