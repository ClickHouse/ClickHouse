#include <Common/typeid_cast.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
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
    ParserKeyword s_rename_column("RENAME COLUMN");
    ParserKeyword s_comment_column("COMMENT COLUMN");
    ParserKeyword s_modify_order_by("MODIFY ORDER BY");
    ParserKeyword s_modify_ttl("MODIFY TTL");
    ParserKeyword s_materialize_ttl("MATERIALIZE TTL");
    ParserKeyword s_modify_setting("MODIFY SETTING");
    ParserKeyword s_modify_query("MODIFY QUERY");

    ParserKeyword s_add_index("ADD INDEX");
    ParserKeyword s_drop_index("DROP INDEX");
    ParserKeyword s_clear_index("CLEAR INDEX");
    ParserKeyword s_materialize_index("MATERIALIZE INDEX");

    ParserKeyword s_add_constraint("ADD CONSTRAINT");
    ParserKeyword s_drop_constraint("DROP CONSTRAINT");

    ParserKeyword s_add("ADD");
    ParserKeyword s_drop("DROP");
    ParserKeyword s_suspend("SUSPEND");
    ParserKeyword s_resume("RESUME");
    ParserKeyword s_refresh("REFRESH");
    ParserKeyword s_modify("MODIFY");

    ParserKeyword s_attach_partition("ATTACH PARTITION");
    ParserKeyword s_detach_partition("DETACH PARTITION");
    ParserKeyword s_drop_partition("DROP PARTITION");
    ParserKeyword s_move_partition("MOVE PARTITION");
    ParserKeyword s_drop_detached_partition("DROP DETACHED PARTITION");
    ParserKeyword s_drop_detached_part("DROP DETACHED PART");
    ParserKeyword s_attach_part("ATTACH PART");
    ParserKeyword s_move_part("MOVE PART");
    ParserKeyword s_fetch_partition("FETCH PARTITION");
    ParserKeyword s_replace_partition("REPLACE PARTITION");
    ParserKeyword s_freeze("FREEZE");
    ParserKeyword s_partition("PARTITION");

    ParserKeyword s_first("FIRST");
    ParserKeyword s_after("AFTER");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_if_exists("IF EXISTS");
    ParserKeyword s_from("FROM");
    ParserKeyword s_in_partition("IN PARTITION");
    ParserKeyword s_with("WITH");
    ParserKeyword s_name("NAME");

    ParserKeyword s_to_disk("TO DISK");
    ParserKeyword s_to_volume("TO VOLUME");
    ParserKeyword s_to_table("TO TABLE");

    ParserKeyword s_delete_where("DELETE WHERE");
    ParserKeyword s_update("UPDATE");
    ParserKeyword s_where("WHERE");
    ParserKeyword s_to("TO");

    ParserCompoundIdentifier parser_name;
    ParserStringLiteral parser_string_literal;
    ParserCompoundColumnDeclaration parser_col_decl;
    ParserIndexDeclaration parser_idx_decl;
    ParserConstraintDeclaration parser_constraint_decl;
    ParserCompoundColumnDeclaration parser_modify_col_decl(false);
    ParserPartition parser_partition;
    ParserExpression parser_exp_elem;
    ParserList parser_assignment_list(
        std::make_unique<ParserAssignment>(), std::make_unique<ParserToken>(TokenType::Comma),
        /* allow_empty = */ false);
    ParserSetQuery parser_settings(true);
    ParserNameList values_p;
    ParserSelectWithUnionQuery select_p;
    ParserTTLExpressionList parser_ttl_list;

    if (is_live_view)
    {
        if (s_refresh.ignore(pos, expected))
        {
            command->type = ASTAlterCommand::LIVE_VIEW_REFRESH;
        }
        else
            return false;
    }
    else
    {
        if (s_add_column.ignore(pos, expected))
        {
            if (s_if_not_exists.ignore(pos, expected))
                command->if_not_exists = true;

            if (!parser_col_decl.parse(pos, command->col_decl, expected))
                return false;

            if (s_first.ignore(pos, expected))
                command->first = true;
            else if (s_after.ignore(pos, expected))
            {
                if (!parser_name.parse(pos, command->column, expected))
                    return false;
            }

            command->type = ASTAlterCommand::ADD_COLUMN;
        }
        else if (s_rename_column.ignore(pos, expected))
        {
            if (s_if_exists.ignore(pos, expected))
                command->if_exists = true;

            if (!parser_name.parse(pos, command->column, expected))
                return false;

            if (!s_to.ignore(pos, expected))
                return false;

            if (!parser_name.parse(pos, command->rename_to, expected))
                return false;

            command->type = ASTAlterCommand::RENAME_COLUMN;
        }
        else if (s_drop_partition.ignore(pos, expected))
        {
            if (!parser_partition.parse(pos, command->partition, expected))
                return false;

            command->type = ASTAlterCommand::DROP_PARTITION;
        }
        else if (s_drop_detached_partition.ignore(pos, expected))
        {
            if (!parser_partition.parse(pos, command->partition, expected))
                return false;

            command->type = ASTAlterCommand::DROP_DETACHED_PARTITION;
        }
        else if (s_drop_detached_part.ignore(pos, expected))
        {
            if (!parser_string_literal.parse(pos, command->partition, expected))
                return false;

            command->type = ASTAlterCommand::DROP_DETACHED_PARTITION;
            command->part = true;
        }
        else if (s_drop_column.ignore(pos, expected))
        {
            if (s_if_exists.ignore(pos, expected))
                command->if_exists = true;

            if (!parser_name.parse(pos, command->column, expected))
                return false;

            command->type = ASTAlterCommand::DROP_COLUMN;
            command->detach = false;
        }
        else if (s_clear_column.ignore(pos, expected))
        {
            if (s_if_exists.ignore(pos, expected))
                command->if_exists = true;

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
        else if (s_add_index.ignore(pos, expected))
        {
            if (s_if_not_exists.ignore(pos, expected))
                command->if_not_exists = true;

            if (!parser_idx_decl.parse(pos, command->index_decl, expected))
                return false;

            if (s_after.ignore(pos, expected))
            {
                if (!parser_name.parse(pos, command->index, expected))
                    return false;
            }

            command->type = ASTAlterCommand::ADD_INDEX;
        }
        else if (s_drop_index.ignore(pos, expected))
        {
            if (s_if_exists.ignore(pos, expected))
                command->if_exists = true;

            if (!parser_name.parse(pos, command->index, expected))
                return false;

            command->type = ASTAlterCommand::DROP_INDEX;
            command->detach = false;
        }
        else if (s_clear_index.ignore(pos, expected))
        {
            if (s_if_exists.ignore(pos, expected))
                command->if_exists = true;

            if (!parser_name.parse(pos, command->index, expected))
                return false;

            command->type = ASTAlterCommand::DROP_INDEX;
            command->clear_index = true;
            command->detach = false;

            if (!s_in_partition.ignore(pos, expected))
                return false;
            if (!parser_partition.parse(pos, command->partition, expected))
                return false;
        }
        else if (s_materialize_index.ignore(pos, expected))
        {
            if (s_if_exists.ignore(pos, expected))
                command->if_exists = true;

            if (!parser_name.parse(pos, command->index, expected))
                return false;

            command->type = ASTAlterCommand::MATERIALIZE_INDEX;
            command->detach = false;

            if (s_in_partition.ignore(pos, expected))
            {
                if (!parser_partition.parse(pos, command->partition, expected))
                    return false;
            }
        }
        else if (s_move_part.ignore(pos, expected))
        {
            if (!parser_string_literal.parse(pos, command->partition, expected))
                return false;

            command->type = ASTAlterCommand::MOVE_PARTITION;
            command->part = true;

            if (s_to_disk.ignore(pos))
                command->move_destination_type = DataDestinationType::DISK;
            else if (s_to_volume.ignore(pos))
                command->move_destination_type = DataDestinationType::VOLUME;
            else if (s_to_table.ignore(pos))
            {
                if (!parseDatabaseAndTableName(pos, expected, command->to_database, command->to_table))
                    return false;
                command->move_destination_type = DataDestinationType::TABLE;
            }
            else
                return false;

            if (command->move_destination_type != DataDestinationType::TABLE)
            {
                ASTPtr ast_space_name;
                if (!parser_string_literal.parse(pos, ast_space_name, expected))
                    return false;

                command->move_destination_name = ast_space_name->as<ASTLiteral &>().value.get<const String &>();
            }
        }
        else if (s_move_partition.ignore(pos, expected))
        {
            if (!parser_partition.parse(pos, command->partition, expected))
                return false;

            command->type = ASTAlterCommand::MOVE_PARTITION;

            if (s_to_disk.ignore(pos))
                command->move_destination_type = DataDestinationType::DISK;
            else if (s_to_volume.ignore(pos))
                command->move_destination_type = DataDestinationType::VOLUME;
            else if (s_to_table.ignore(pos))
            {
                if (!parseDatabaseAndTableName(pos, expected, command->to_database, command->to_table))
                    return false;
                command->move_destination_type = DataDestinationType::TABLE;
            }
            else
                return false;

            if (command->move_destination_type != DataDestinationType::TABLE)
            {
                ASTPtr ast_space_name;
                if (!parser_string_literal.parse(pos, ast_space_name, expected))
                    return false;

                command->move_destination_name = ast_space_name->as<ASTLiteral &>().value.get<const String &>();
            }
        }
        else if (s_add_constraint.ignore(pos, expected))
        {
            if (s_if_not_exists.ignore(pos, expected))
                command->if_not_exists = true;

            if (!parser_constraint_decl.parse(pos, command->constraint_decl, expected))
                return false;

            command->type = ASTAlterCommand::ADD_CONSTRAINT;
        }
        else if (s_drop_constraint.ignore(pos, expected))
        {
            if (s_if_exists.ignore(pos, expected))
                command->if_exists = true;

            if (!parser_name.parse(pos, command->constraint, expected))
                return false;

            command->type = ASTAlterCommand::DROP_CONSTRAINT;
            command->detach = false;
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
        else if (s_replace_partition.ignore(pos, expected))
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

            command->from = ast_from->as<ASTLiteral &>().value.get<const String &>();
            command->type = ASTAlterCommand::FETCH_PARTITION;
        }
        else if (s_freeze.ignore(pos, expected))
        {
            if (s_partition.ignore(pos, expected))
            {
                if (!parser_partition.parse(pos, command->partition, expected))
                    return false;

                command->type = ASTAlterCommand::FREEZE_PARTITION;
            }
            else
            {
                command->type = ASTAlterCommand::FREEZE_ALL;
            }

            /// WITH NAME 'name' - place local backup to directory with specified name
            if (s_with.ignore(pos, expected))
            {
                if (!s_name.ignore(pos, expected))
                    return false;

                ASTPtr ast_with_name;
                if (!parser_string_literal.parse(pos, ast_with_name, expected))
                    return false;

                command->with_name = ast_with_name->as<ASTLiteral &>().value.get<const String &>();
            }
        }
        else if (s_modify_column.ignore(pos, expected))
        {
            if (s_if_exists.ignore(pos, expected))
                command->if_exists = true;

            if (!parser_modify_col_decl.parse(pos, command->col_decl, expected))
                return false;

            if (s_first.ignore(pos, expected))
                command->first = true;
            else if (s_after.ignore(pos, expected))
            {
                if (!parser_name.parse(pos, command->column, expected))
                    return false;
            }

            command->type = ASTAlterCommand::MODIFY_COLUMN;
        }
        else if (s_modify_order_by.ignore(pos, expected))
        {
            if (!parser_exp_elem.parse(pos, command->order_by, expected))
                return false;

            command->type = ASTAlterCommand::MODIFY_ORDER_BY;
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
        else if (s_comment_column.ignore(pos, expected))
        {
            if (s_if_exists.ignore(pos, expected))
                command->if_exists = true;

            if (!parser_name.parse(pos, command->column, expected))
                return false;

            if (!parser_string_literal.parse(pos, command->comment, expected))
                return false;

            command->type = ASTAlterCommand::COMMENT_COLUMN;
        }
        else if (s_modify_ttl.ignore(pos, expected))
        {
            if (!parser_ttl_list.parse(pos, command->ttl, expected))
                return false;
            command->type = ASTAlterCommand::MODIFY_TTL;
        }
        else if (s_materialize_ttl.ignore(pos, expected))
        {
            command->type = ASTAlterCommand::MATERIALIZE_TTL;

            if (s_in_partition.ignore(pos, expected))
            {
                if (!parser_partition.parse(pos, command->partition, expected))
                    return false;
            }
        }
        else if (s_modify_setting.ignore(pos, expected))
        {
            if (!parser_settings.parse(pos, command->settings_changes, expected))
                return false;
            command->type = ASTAlterCommand::MODIFY_SETTING;
        }
        else if (s_modify_query.ignore(pos, expected))
        {
            if (!select_p.parse(pos, command->select, expected))
                return false;
            command->type = ASTAlterCommand::MODIFY_QUERY;
        }
        else
            return false;

    }

    if (command->col_decl)
        command->children.push_back(command->col_decl);
    if (command->column)
        command->children.push_back(command->column);
    if (command->partition)
        command->children.push_back(command->partition);
    if (command->order_by)
        command->children.push_back(command->order_by);
    if (command->predicate)
        command->children.push_back(command->predicate);
    if (command->update_assignments)
        command->children.push_back(command->update_assignments);
    if (command->values)
        command->children.push_back(command->values);
    if (command->comment)
        command->children.push_back(command->comment);
    if (command->ttl)
        command->children.push_back(command->ttl);
    if (command->settings_changes)
        command->children.push_back(command->settings_changes);

    return true;
}


bool ParserAlterCommandList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto command_list = std::make_shared<ASTAlterCommandList>();
    node = command_list;

    ParserToken s_comma(TokenType::Comma);
    ParserAlterCommand p_command(is_live_view);

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

    tryGetIdentifierNameInto(column, assignment->column_name);
    if (assignment->expression)
        assignment->children.push_back(assignment->expression);

    return true;
}


bool ParserAlterQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTAlterQuery>();
    node = query;

    ParserKeyword s_alter_table("ALTER TABLE");
    ParserKeyword s_alter_live_view("ALTER LIVE VIEW");

    bool is_live_view = false;

    if (!s_alter_table.ignore(pos, expected))
    {
        if (!s_alter_live_view.ignore(pos, expected))
            return false;
        else
            is_live_view = true;
    }

    if (is_live_view)
        query->is_live_view = true;

    if (!parseDatabaseAndTableName(pos, expected, query->database, query->table))
        return false;

    String cluster_str;
    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }
    query->cluster = cluster_str;

    ParserAlterCommandList p_command_list(is_live_view);
    ASTPtr command_list;
    if (!p_command_list.parse(pos, command_list, expected))
        return false;

    query->set(query->command_list, command_list);

    return true;
}

}
