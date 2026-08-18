#include <Parsers/ParserAlterQuery.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ParserRefreshStrategy.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserStringAndSubstitution.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

bool ParserAlterCommand::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto command = make_intrusive<ASTAlterCommand>();
    node = command;

    ParserKeyword s_add_column(Keyword::ADD_COLUMN);
    ParserKeyword s_drop_column(Keyword::DROP_COLUMN);
    ParserKeyword s_clear_column(Keyword::CLEAR_COLUMN);
    ParserKeyword s_modify_column(Keyword::MODIFY_COLUMN);
    ParserKeyword s_alter_column(Keyword::ALTER_COLUMN);
    ParserKeyword s_rename_column(Keyword::RENAME_COLUMN);
    ParserKeyword s_comment_column(Keyword::COMMENT_COLUMN);
    ParserKeyword s_materialize_column(Keyword::MATERIALIZE_COLUMN);

    ParserKeyword s_modify_order_by(Keyword::MODIFY_ORDER_BY);
    ParserKeyword s_modify_sample_by(Keyword::MODIFY_SAMPLE_BY);
    ParserKeyword s_materialize(Keyword::MATERIALIZE);
    ParserKeyword s_modify_ttl(Keyword::MODIFY_TTL);
    ParserKeyword s_materialize_ttl(Keyword::MATERIALIZE_TTL);
    ParserKeyword s_rewrite_parts(Keyword::REWRITE_PARTS);
    ParserKeyword s_modify_setting(Keyword::MODIFY_SETTING);
    ParserKeyword s_add_enum_values(Keyword::ADD_ENUM_VALUES);
    ParserKeyword s_reset_setting(Keyword::RESET_SETTING);
    ParserKeyword s_modify_query(Keyword::MODIFY_QUERY);
    ParserKeyword s_modify_sql_security(Keyword::MODIFY_SQL_SECURITY);
    ParserKeyword s_modify_definer(Keyword::MODIFY_DEFINER);
    ParserKeyword s_modify_refresh(Keyword::MODIFY_REFRESH);

    ParserKeyword s_add_index(Keyword::ADD_INDEX);
    ParserKeyword s_drop_index(Keyword::DROP_INDEX);
    ParserKeyword s_clear_index(Keyword::CLEAR_INDEX);
    ParserKeyword s_materialize_index(Keyword::MATERIALIZE_INDEX);

    ParserKeyword s_add_statistics(Keyword::ADD_STATISTICS);
    ParserKeyword s_drop_statistics(Keyword::DROP_STATISTICS);
    ParserKeyword s_modify_statistics(Keyword::MODIFY_STATISTICS);
    ParserKeyword s_clear_statistics(Keyword::CLEAR_STATISTICS);
    ParserKeyword s_materialize_statistics(Keyword::MATERIALIZE_STATISTICS);

    ParserKeyword s_add_constraint(Keyword::ADD_CONSTRAINT);
    ParserKeyword s_drop_constraint(Keyword::DROP_CONSTRAINT);
    ParserKeyword s_modify_constraint(Keyword::MODIFY_CONSTRAINT);

    ParserKeyword s_add_projection(Keyword::ADD_PROJECTION);
    ParserKeyword s_drop_projection(Keyword::DROP_PROJECTION);
    ParserKeyword s_clear_projection(Keyword::CLEAR_PROJECTION);
    ParserKeyword s_materialize_projection(Keyword::MATERIALIZE_PROJECTION);
    ParserKeyword s_modify_comment(Keyword::MODIFY_COMMENT);

    ParserKeyword s_add(Keyword::ADD);
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_modify(Keyword::MODIFY);

    ParserKeyword s_attach_partition(Keyword::ATTACH_PARTITION);
    ParserKeyword s_attach_part(Keyword::ATTACH_PART);
    ParserKeyword s_detach_partition(Keyword::DETACH_PARTITION);
    ParserKeyword s_detach_part(Keyword::DETACH_PART);
    ParserKeyword s_drop_partition(Keyword::DROP_PARTITION);
    ParserKeyword s_drop_part(Keyword::DROP_PART);
    ParserKeyword s_forget_partition(Keyword::FORGET_PARTITION);
    ParserKeyword s_move_partition(Keyword::MOVE_PARTITION);
    ParserKeyword s_move_part(Keyword::MOVE_PART);
    ParserKeyword s_drop_detached_partition(Keyword::DROP_DETACHED_PARTITION);
    ParserKeyword s_drop_detached_part(Keyword::DROP_DETACHED_PART);
    ParserKeyword s_fetch_partition(Keyword::FETCH_PARTITION);
    ParserKeyword s_fetch_part(Keyword::FETCH_PART);
    ParserKeyword s_replace_partition(Keyword::REPLACE_PARTITION);
    ParserKeyword s_freeze(Keyword::FREEZE);
    ParserKeyword s_unfreeze(Keyword::UNFREEZE);
    ParserKeyword s_unlock_snapshot(Keyword::UNLOCK_SNAPSHOT);
    ParserKeyword s_partition(Keyword::PARTITION);

    ParserKeyword s_first(Keyword::FIRST);
    ParserKeyword s_after(Keyword::AFTER);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_from(Keyword::FROM);
    ParserKeyword s_in_partition(Keyword::IN_PARTITION);
    ParserKeyword s_with(Keyword::WITH);
    ParserKeyword s_name(Keyword::NAME);

    ParserKeyword s_to_disk(Keyword::TO_DISK);
    ParserKeyword s_to_volume(Keyword::TO_VOLUME);
    ParserKeyword s_to_table(Keyword::TO_TABLE);
    ParserKeyword s_to_shard(Keyword::TO_SHARD);

    ParserKeyword s_delete(Keyword::DELETE);
    ParserKeyword s_update(Keyword::UPDATE);
    ParserKeyword s_where(Keyword::WHERE);
    ParserKeyword s_to(Keyword::TO);

    ParserKeyword s_remove(Keyword::REMOVE);
    ParserKeyword s_default(Keyword::DEFAULT);
    ParserKeyword s_materialized(Keyword::MATERIALIZED);
    ParserKeyword s_alias(Keyword::ALIAS);
    ParserKeyword s_comment(Keyword::COMMENT);
    ParserKeyword s_codec(Keyword::CODEC);
    ParserKeyword s_ttl(Keyword::TTL);
    ParserKeyword s_settings(Keyword::SETTINGS);

    ParserKeyword s_remove_ttl(Keyword::REMOVE_TTL);
    ParserKeyword s_remove_sample_by(Keyword::REMOVE_SAMPLE_BY);
    ParserKeyword s_apply_deleted_mask(Keyword::APPLY_DELETED_MASK);
    ParserKeyword s_apply_patches(Keyword::APPLY_PATCHES);
    ParserKeyword s_execute(Keyword::EXECUTE);
    ParserKeyword s_all(Keyword::ALL);

    ParserToken parser_opening_round_bracket(TokenType::OpeningRoundBracket);
    ParserToken parser_closing_round_bracket(TokenType::ClosingRoundBracket);

    ParserCompoundIdentifier parser_name;
    ParserStringLiteral parser_string_literal;
    ParserStringAndSubstitution parser_string_and_substituion;
    ParserCompoundColumnDeclaration parser_col_decl(/* require_type = */ true, /* allow_null_modifiers = */ true);
    ParserIndexDeclaration parser_idx_decl;
    ParserStatisticsDeclaration parser_stat_decl;
    ParserStatisticsDeclarationWithoutTypes parser_stat_decl_without_types;
    ParserConstraintDeclaration parser_constraint_decl;
    ParserProjectionDeclaration parser_projection_decl;
    ParserCompoundColumnDeclaration parser_modify_col_decl(/* require_type = */ false, /* allow_null_modifiers = */ true, /* check_keywords_after_name = */ true);
    ParserPartition parser_partition;
    ParserExpressionWithOptionalAlias parser_exp_elem(false);
    ParserList parser_assignment_list(
        std::make_unique<ParserAssignment>(), std::make_unique<ParserToken>(TokenType::Comma),
        /* allow_empty = */ false);
    ParserSetQuery parser_settings(true);
    ParserList parser_reset_setting(
        std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma),
        /* allow_empty = */ false);

    ParserExpressionList parser_add_enum_values(false);
    ParserSelectWithUnionQuery select_p;
    ParserSQLSecurity sql_security_p;
    ParserRefreshStrategy refresh_p;
    ParserTTLExpressionList parser_ttl_list;

    ASTPtr command_col_decl;
    ASTPtr command_column;
    ASTPtr command_order_by;
    ASTPtr command_sample_by;
    ASTPtr command_index_decl;
    ASTPtr command_index;
    ASTPtr command_constraint_decl;
    ASTPtr command_constraint;
    ASTPtr command_projection_decl;
    ASTPtr command_projection;
    ASTPtr command_statistics_decl;
    ASTPtr command_partition;
    ASTPtr command_predicate;
    ASTPtr command_update_assignments;
    ASTPtr command_comment;
    ASTPtr command_ttl;
    ASTPtr command_settings_changes;
    ASTPtr command_settings_resets;
    ASTPtr command_add_enum_values;
    ASTPtr command_select;
    ASTPtr command_rename_to;
    ASTPtr command_sql_security;
    ASTPtr command_snapshot_desc;
    ASTPtr command_refresh;

    if (with_round_bracket)
    {
        if (!parser_opening_round_bracket.ignore(pos, expected))
            return false;
    }

    switch (alter_object)
    {
        case ASTAlterQuery::AlterObjectType::DATABASE:
        {
            if (s_modify_setting.ignore(pos, expected))
            {
                if (!parser_settings.parse(pos, command_settings_changes, expected))
                    return false;
                command->type = ASTAlterCommand::MODIFY_DATABASE_SETTING;
            }
            else if (s_modify_comment.ignore(pos, expected))
            {
                if (!parser_string_literal.parse(pos, command_comment, expected))
                    return false;

                command->type = ASTAlterCommand::MODIFY_DATABASE_COMMENT;
            }
            else
                return false;
            break;
        }
        case ASTAlterQuery::AlterObjectType::TABLE:
        {
            if (s_add_column.ignore(pos, expected))
            {
                if (s_if_not_exists.ignore(pos, expected))
                    command->if_not_exists = true;

                if (!parser_col_decl.parse(pos, command_col_decl, expected))
                    return false;

                if (s_first.ignore(pos, expected))
                    command->first = true;
                else if (s_after.ignore(pos, expected))
                {
                    if (!parser_name.parse(pos, command_column, expected))
                        return false;
                }

                command->type = ASTAlterCommand::ADD_COLUMN;
            }
            else if (s_rename_column.ignore(pos, expected))
            {
                if (s_if_exists.ignore(pos, expected))
                    command->if_exists = true;

                if (!parser_name.parse(pos, command_column, expected))
                    return false;

                if (!s_to.ignore(pos, expected))
                    return false;

                if (!parser_name.parse(pos, command_rename_to, expected))
                    return false;

                command->type = ASTAlterCommand::RENAME_COLUMN;
            }
            else if (s_materialize_column.ignore(pos, expected))
            {
                if (!parser_name.parse(pos, command_column, expected))
                    return false;

                command->type = ASTAlterCommand::MATERIALIZE_COLUMN;
                command->detach = false;

                if (s_in_partition.ignore(pos, expected))
                {
                    if (!parser_partition.parse(pos, command_partition, expected))
                        return false;
                }
            }
            else if (s_drop_partition.ignore(pos, expected))
            {
                if (!parser_partition.parse(pos, command_partition, expected))
                    return false;

                command->type = ASTAlterCommand::DROP_PARTITION;
            }
            else if (s_drop_part.ignore(pos, expected))
            {
                if (!parser_string_and_substituion.parse(pos, command_partition, expected))
                    return false;

                command->type = ASTAlterCommand::DROP_PARTITION;
                command->part = true;
            }
            else if (s_forget_partition.ignore(pos, expected))
            {
                if (!parser_partition.parse(pos, command_partition, expected))
                    return false;

                command->type = ASTAlterCommand::FORGET_PARTITION;
            }
            else if (s_drop_detached_partition.ignore(pos, expected))
            {
                if (!parser_partition.parse(pos, command_partition, expected))
                    return false;

                command->type = ASTAlterCommand::DROP_DETACHED_PARTITION;
            }
            else if (s_drop_detached_part.ignore(pos, expected))
            {
                if (!parser_string_and_substituion.parse(pos, command_partition, expected))
                    return false;

                command->type = ASTAlterCommand::DROP_DETACHED_PARTITION;
                command->part = true;
            }
            else if (s_drop_column.ignore(pos, expected))
            {
                if (s_if_exists.ignore(pos, expected))
                    command->if_exists = true;

                if (!parser_name.parse(pos, command_column, expected))
                    return false;

                command->type = ASTAlterCommand::DROP_COLUMN;
                command->detach = false;
            }
            else if (s_clear_column.ignore(pos, expected))
            {
                if (s_if_exists.ignore(pos, expected))
                    command->if_exists = true;

                if (!parser_name.parse(pos, command_column, expected))
                    return false;

                command->type = ASTAlterCommand::DROP_COLUMN;
                command->clear_column = true;
                command->detach = false;

                if (s_in_partition.ignore(pos, expected))
                {
                    if (!parser_partition.parse(pos, command_partition, expected))
                        return false;
                }
            }
            else if (s_add_index.ignore(pos, expected))
            {
                if (s_if_not_exists.ignore(pos, expected))
                    command->if_not_exists = true;

                if (!parser_idx_decl.parse(pos, command_index_decl, expected))
                    return false;

                if (s_first.ignore(pos, expected))
                    command->first = true;
                else if (s_after.ignore(pos, expected))
                {
                    if (!parser_name.parse(pos, command_index, expected))
                        return false;
                }

                command->type = ASTAlterCommand::ADD_INDEX;
            }
            else if (s_drop_index.ignore(pos, expected))
            {
                if (s_if_exists.ignore(pos, expected))
                    command->if_exists = true;

                if (!parser_name.parse(pos, command_index, expected))
                    return false;

                command->type = ASTAlterCommand::DROP_INDEX;
                command->detach = false;
            }
            else if (s_clear_index.ignore(pos, expected))
            {
                if (s_if_exists.ignore(pos, expected))
                    command->if_exists = true;

                if (!parser_name.parse(pos, command_index, expected))
                    return false;

                command->type = ASTAlterCommand::DROP_INDEX;
                command->clear_index = true;
                command->detach = false;

                if (s_in_partition.ignore(pos, expected))
                {
                    if (!parser_partition.parse(pos, command_partition, expected))
                        return false;
                }
            }
            else if (s_materialize_index.ignore(pos, expected))
            {
                if (s_if_exists.ignore(pos, expected))
                    command->if_exists = true;

                if (!parser_name.parse(pos, command_index, expected))
                    return false;

                command->type = ASTAlterCommand::MATERIALIZE_INDEX;
                command->detach = false;

                if (s_in_partition.ignore(pos, expected))
                {
                    if (!parser_partition.parse(pos, command_partition, expected))
                        return false;
                }
            }
            else if (s_add_statistics.ignore(pos, expected))
            {
                if (s_if_not_exists.ignore(pos, expected))
                    command->if_not_exists = true;

                if (!parser_stat_decl.parse(pos, command_statistics_decl, expected))
                    return false;

                command->type = ASTAlterCommand::ADD_STATISTICS;
            }
            else if (s_modify_statistics.ignore(pos, expected))
            {
                if (!parser_stat_decl.parse(pos, command_statistics_decl, expected))
                    return false;

                command->type = ASTAlterCommand::MODIFY_STATISTICS;
            }
            else if (s_drop_statistics.ignore(pos, expected))
            {
                if (s_if_exists.ignore(pos, expected))
                    command->if_exists = true;

                if (!parser_stat_decl_without_types.parse(pos, command_statistics_decl, expected))
                    return false;

                command->type = ASTAlterCommand::DROP_STATISTICS;
            }
            else if (s_clear_statistics.ignore(pos, expected))
            {
                command->type = ASTAlterCommand::DROP_STATISTICS;
                command->clear_statistics = true;
                command->detach = false;

                if (!s_all.ignore(pos, expected))
                {
                    if (s_if_exists.ignore(pos, expected))
                        command->if_exists = true;

                    if (!parser_stat_decl_without_types.parse(pos, command_statistics_decl, expected))
                        return false;

                    if (s_in_partition.ignore(pos, expected))
                    {
                        if (!parser_partition.parse(pos, command_partition, expected))
                            return false;
                    }
                }
            }
            else if (s_materialize_statistics.ignore(pos, expected))
            {
                command->type = ASTAlterCommand::MATERIALIZE_STATISTICS;
                command->detach = false;
                if (!ParserKeyword(Keyword::ALL).ignore(pos, expected))
                {
                    if (s_if_exists.ignore(pos, expected))
                        command->if_exists = true;

                    if (!parser_stat_decl_without_types.parse(pos, command_statistics_decl, expected))
                        return false;

                    if (s_in_partition.ignore(pos, expected))
                    {
                        if (!parser_partition.parse(pos, command_partition, expected))
                            return false;
                    }
                }
            }
            else if (s_add_projection.ignore(pos, expected))
            {
                if (s_if_not_exists.ignore(pos, expected))
                    command->if_not_exists = true;

                if (!parser_projection_decl.parse(pos, command_projection_decl, expected))
                    return false;

                if (s_first.ignore(pos, expected))
                    command->first = true;
                else if (s_after.ignore(pos, expected))
                {
                    if (!parser_name.parse(pos, command_projection, expected))
                        return false;
                }

                command->type = ASTAlterCommand::ADD_PROJECTION;
            }
            else if (s_drop_projection.ignore(pos, expected))
            {
                if (s_if_exists.ignore(pos, expected))
                    command->if_exists = true;

                if (!parser_name.parse(pos, command_projection, expected))
                    return false;

                command->type = ASTAlterCommand::DROP_PROJECTION;
                command->detach = false;
            }
            else if (s_clear_projection.ignore(pos, expected))
            {
                if (s_if_exists.ignore(pos, expected))
                    command->if_exists = true;

                if (!parser_name.parse(pos, command_projection, expected))
                    return false;

                command->type = ASTAlterCommand::DROP_PROJECTION;
                command->clear_projection = true;
                command->detach = false;

                if (s_in_partition.ignore(pos, expected))
                {
                    if (!parser_partition.parse(pos, command_partition, expected))
                        return false;
                }
            }
            else if (s_materialize_projection.ignore(pos, expected))
            {
                if (s_if_exists.ignore(pos, expected))
                    command->if_exists = true;

                if (!parser_name.parse(pos, command_projection, expected))
                    return false;

                command->type = ASTAlterCommand::MATERIALIZE_PROJECTION;
                command->detach = false;

                if (s_in_partition.ignore(pos, expected))
                {
                    if (!parser_partition.parse(pos, command_partition, expected))
                        return false;
                }
            }
            else if (s_move_part.ignore(pos, expected))
            {
                if (!parser_string_and_substituion.parse(pos, command_partition, expected))
                    return false;

                command->type = ASTAlterCommand::MOVE_PARTITION;
                command->part = true;

                if (s_to_disk.ignore(pos, expected))
                    command->move_destination_type = DataDestinationType::DISK;
                else if (s_to_volume.ignore(pos, expected))
                    command->move_destination_type = DataDestinationType::VOLUME;
                else if (s_to_shard.ignore(pos, expected))
                {
                    command->move_destination_type = DataDestinationType::SHARD;
                }
                else
                    return false;

                ASTPtr ast_space_name;
                if (!parser_string_literal.parse(pos, ast_space_name, expected))
                    return false;

                command->move_destination_name = ast_space_name->as<ASTLiteral &>().value.safeGet<String>();
            }
            else if (s_move_partition.ignore(pos, expected))
            {
                if (!parser_partition.parse(pos, command_partition, expected))
                    return false;

                command->type = ASTAlterCommand::MOVE_PARTITION;

                if (s_to_disk.ignore(pos, expected))
                    command->move_destination_type = DataDestinationType::DISK;
                else if (s_to_volume.ignore(pos, expected))
                    command->move_destination_type = DataDestinationType::VOLUME;
                else if (s_to_table.ignore(pos, expected))
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

                    command->move_destination_name = ast_space_name->as<ASTLiteral &>().value.safeGet<String>();
                }
            }
            else if (s_add_constraint.ignore(pos, expected))
            {
                if (s_if_not_exists.ignore(pos, expected))
                    command->if_not_exists = true;

                if (!parser_constraint_decl.parse(pos, command_constraint_decl, expected))
                    return false;

                command->type = ASTAlterCommand::ADD_CONSTRAINT;
            }
            else if (s_modify_constraint.ignore(pos, expected))
            {
                if (s_if_exists.ignore(pos, expected))
                    command->if_exists = true;

                if (!parser_constraint_decl.parse(pos, command_constraint_decl, expected))
                    return false;

                command->type = ASTAlterCommand::MODIFY_CONSTRAINT;
            }
            else if (s_drop_constraint.ignore(pos, expected))
            {
                if (s_if_exists.ignore(pos, expected))
                    command->if_exists = true;

                if (!parser_name.parse(pos, command_constraint, expected))
                    return false;

                command->type = ASTAlterCommand::DROP_CONSTRAINT;
                command->detach = false;
            }
            else if (s_detach_partition.ignore(pos, expected))
            {
                if (!parser_partition.parse(pos, command_partition, expected))
                    return false;

                command->type = ASTAlterCommand::DROP_PARTITION;
                command->detach = true;
            }
            else if (s_detach_part.ignore(pos, expected))
            {
                if (!parser_string_and_substituion.parse(pos, command_partition, expected))
                    return false;

                command->type = ASTAlterCommand::DROP_PARTITION;
                command->part = true;
                command->detach = true;
            }
            else if (s_attach_partition.ignore(pos, expected))
            {
                if (!parser_partition.parse(pos, command_partition, expected))
                    return false;

                if (s_from.ignore(pos, expected))
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
                if (!parser_partition.parse(pos, command_partition, expected))
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
                if (!parser_string_and_substituion.parse(pos, command_partition, expected))
                    return false;

                if (s_from.ignore(pos, expected))
                {
                    ASTPtr ast_from;
                    if (!parser_string_literal.parse(pos, ast_from, expected))
                        return false;

                    command->from = ast_from->as<ASTLiteral &>().value.safeGet<String>();
                }

                command->part = true;
                command->type = ASTAlterCommand::ATTACH_PARTITION;
            }
            else if (s_fetch_partition.ignore(pos, expected))
            {
                if (!parser_partition.parse(pos, command_partition, expected))
                    return false;

                if (!s_from.ignore(pos, expected))
                    return false;

                ASTPtr ast_from;
                if (!parser_string_literal.parse(pos, ast_from, expected))
                    return false;

                command->from = ast_from->as<ASTLiteral &>().value.safeGet<String>();
                command->type = ASTAlterCommand::FETCH_PARTITION;
            }
            else if (s_fetch_part.ignore(pos, expected))
            {
                if (!parser_string_and_substituion.parse(pos, command_partition, expected))
                    return false;

                if (!s_from.ignore(pos, expected))
                    return false;

                ASTPtr ast_from;
                if (!parser_string_literal.parse(pos, ast_from, expected))
                    return false;
                command->from = ast_from->as<ASTLiteral &>().value.safeGet<String>();
                command->part = true;
                command->type = ASTAlterCommand::FETCH_PARTITION;
            }
            else if (s_freeze.ignore(pos, expected))
            {
                if (s_partition.ignore(pos, expected))
                {
                    if (!parser_partition.parse(pos, command_partition, expected))
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

                    command->with_name = ast_with_name->as<ASTLiteral &>().value.safeGet<String>();
                }
            }
            else if (s_unfreeze.ignore(pos, expected))
            {
                if (s_partition.ignore(pos, expected))
                {
                    if (!parser_partition.parse(pos, command_partition, expected))
                        return false;

                    command->type = ASTAlterCommand::UNFREEZE_PARTITION;
                }
                else
                {
                    command->type = ASTAlterCommand::UNFREEZE_ALL;
                }

                /// WITH NAME 'name' - remove local backup to directory with specified name
                if (s_with.ignore(pos, expected))
                {
                    if (!s_name.ignore(pos, expected))
                        return false;

                    ASTPtr ast_with_name;
                    if (!parser_string_literal.parse(pos, ast_with_name, expected))
                        return false;

                    command->with_name = ast_with_name->as<ASTLiteral &>().value.safeGet<String>();
                }
                else
                {
                    return false;
                }
            }
            else if (s_unlock_snapshot.ignore(pos, expected))
            {
                ASTPtr ast_snapshot_name;
                if (!parser_string_literal.parse(pos, ast_snapshot_name, expected))
                    return false;

                command->snapshot_name = ast_snapshot_name->as<ASTLiteral &>().value.safeGet<String>();
                command->type = ASTAlterCommand::UNLOCK_SNAPSHOT;
                /// unlock snapshot <uuid> from s3(...), but `from (s3...)` is optional
                if (s_from.ignore(pos, expected))
                {
                    if (!ParserIdentifierWithOptionalParameters{}.parse(pos, command_snapshot_desc, expected))
                        return false;
                    command_snapshot_desc->as<ASTFunction &>().setKind(ASTFunction::Kind::BACKUP_NAME);
                }
            }
            else if (bool is_modify = s_modify_column.ignore(pos, expected); is_modify || s_alter_column.ignore(pos, expected))
            {
                if (s_if_exists.ignore(pos, expected))
                    command->if_exists = true;

                if (!is_modify)
                    parser_modify_col_decl.enableCheckTypeKeyword();

                if (!parser_modify_col_decl.parse(pos, command_col_decl, expected))
                    return false;

                /// A trailing NULL / NOT NULL modifier needs an explicit column type to apply it
                /// to, the same way ADD COLUMN / CREATE TABLE do. A type-less MODIFY / ALTER COLUMN
                /// has no type, so reject it here instead of silently ignoring the modifier.
                if (const auto & col_decl = command_col_decl->as<const ASTColumnDeclaration &>();
                    col_decl.null_modifier.has_value() && !col_decl.getType())
                {
                    throw Exception(
                        ErrorCodes::SYNTAX_ERROR,
                        "NULL / NOT NULL modifier requires an explicit column type");
                }

                auto check_no_type = [&](const std::string_view keyword)
                {
                    const auto & column_decl = command_col_decl->as<const ASTColumnDeclaration &>();

                    if (column_decl.hasChildren() || column_decl.null_modifier.has_value() || column_decl.default_specifier != ColumnDefaultSpecifier::Empty
                        || column_decl.ephemeral_default || column_decl.primary_key_specifier)
                    {
                        throw Exception(ErrorCodes::SYNTAX_ERROR, "Cannot specify column properties before '{}'", keyword);
                    }
                };

                if (s_remove.ignore(pos, expected))
                {
                    check_no_type(s_remove.getName());

                    if (s_default.ignore(pos, expected))
                        command->remove_property = toStringView(Keyword::DEFAULT);
                    else if (s_materialized.ignore(pos, expected))
                        command->remove_property = toStringView(Keyword::MATERIALIZED);
                    else if (s_alias.ignore(pos, expected))
                        command->remove_property = toStringView(Keyword::ALIAS);
                    else if (s_comment.ignore(pos, expected))
                        command->remove_property = toStringView(Keyword::COMMENT);
                    else if (s_codec.ignore(pos, expected))
                        command->remove_property = toStringView(Keyword::CODEC);
                    else if (s_ttl.ignore(pos, expected))
                        command->remove_property = toStringView(Keyword::TTL);
                    else if (s_settings.ignore(pos, expected))
                        command->remove_property = toStringView(Keyword::SETTINGS);
                    else
                        return false;
                }
                else if (s_modify_setting.ignore(pos, expected))
                {
                    check_no_type(s_modify_setting.getName());

                    if (!parser_settings.parse(pos, command_settings_changes, expected))
                        return false;
                }
                else if (s_reset_setting.ignore(pos, expected))
                {
                    check_no_type(s_reset_setting.getName());

                    if (!parser_reset_setting.parse(pos, command_settings_resets, expected))
                        return false;
                }
                else if (s_add_enum_values.ignore(pos, expected))
                {
                    check_no_type(s_add_enum_values.getName());

                    ParserToken open(TokenType::OpeningRoundBracket);
                    ParserToken close(TokenType::ClosingRoundBracket);

                    if (!open.ignore(pos, expected) || !parser_add_enum_values.parse(pos, command_add_enum_values, expected)
                        || !close.ignore(pos, expected))
                        return false;
                }
                else
                {
                    if (s_first.ignore(pos, expected))
                        command->first = true;
                    else if (s_after.ignore(pos, expected))
                    {
                        if (!parser_name.parse(pos, command_column, expected))
                            return false;
                    }
                }
                command->type = ASTAlterCommand::MODIFY_COLUMN;

                /// Make sure that type is not populated when REMOVE/MODIFY SETTING/RESET SETTING/ADD ENUM VALUES is used,
                /// because we wouldn't modify the type, which can be confusing
                chassert(
                    nullptr == command_col_decl->as<const ASTColumnDeclaration &>().getType()
                    || (command->remove_property.empty() && nullptr == command_settings_changes
                        && nullptr == command_settings_resets && nullptr == command_add_enum_values));
            }
            else if (s_modify_order_by.ignore(pos, expected))
            {
                if (!parser_exp_elem.parse(pos, command_order_by, expected))
                    return false;

                command->type = ASTAlterCommand::MODIFY_ORDER_BY;
            }
            else if (s_modify_sample_by.ignore(pos, expected))
            {
                if (!parser_exp_elem.parse(pos, command_sample_by, expected))
                    return false;

                command->type = ASTAlterCommand::MODIFY_SAMPLE_BY;
            }
            else if (s_remove_sample_by.ignore(pos, expected))
            {
                command->type = ASTAlterCommand::REMOVE_SAMPLE_BY;
            }
            else if (s_delete.ignore(pos, expected))
            {
                if (s_in_partition.ignore(pos, expected))
                {
                    if (!parser_partition.parse(pos, command_partition, expected))
                        return false;
                }

                if (!s_where.ignore(pos, expected))
                    return false;

                if (!parser_exp_elem.parse(pos, command_predicate, expected))
                    return false;

                /// ParserExpression, in contrast to ParserExpressionWithOptionalAlias,
                /// does not expect an alias after the expression. However, in certain cases,
                /// it uses ParserExpressionWithOptionalAlias recursively, and use its result.
                /// This is the case when it parses a single expression in parentheses, e.g.,
                /// it does not allow
                /// 1 AS x
                /// but it can parse
                /// (1 AS x)
                /// which we should not allow as well.
                if (!command_predicate->tryGetAlias().empty())
                    return false;

                command->type = ASTAlterCommand::DELETE;
            }
            else if (s_update.ignore(pos, expected))
            {
                if (!parser_assignment_list.parse(pos, command_update_assignments, expected))
                    return false;

                if (s_in_partition.ignore(pos, expected))
                {
                    if (!parser_partition.parse(pos, command_partition, expected))
                        return false;
                }

                if (!s_where.ignore(pos, expected))
                    return false;

                if (!parser_exp_elem.parse(pos, command_predicate, expected))
                    return false;

                /// ParserExpression, in contrast to ParserExpressionWithOptionalAlias,
                /// does not expect an alias after the expression. However, in certain cases,
                /// it uses ParserExpressionWithOptionalAlias recursively, and use its result.
                /// This is the case when it parses a single expression in parentheses, e.g.,
                /// it does not allow
                /// 1 AS x
                /// but it can parse
                /// (1 AS x)
                /// which we should not allow as well.
                if (!command_predicate->tryGetAlias().empty())
                    return false;

                command->type = ASTAlterCommand::UPDATE;
            }
            else if (s_comment_column.ignore(pos, expected))
            {
                if (s_if_exists.ignore(pos, expected))
                    command->if_exists = true;

                if (!parser_name.parse(pos, command_column, expected))
                    return false;

                if (!parser_string_literal.parse(pos, command_comment, expected))
                    return false;

                command->type = ASTAlterCommand::COMMENT_COLUMN;
            }
            else if (s_modify_ttl.ignore(pos, expected))
            {
                /// MODIFY TTL MATERIALIZE|REMOVE|MODIFY is illegal
                /// because MATERIALIZE|REMOVE|MODIFY TTL is used instead.
                if (s_materialize.checkWithoutMoving(pos, expected) ||
                    s_remove.checkWithoutMoving(pos, expected) ||
                    s_modify.checkWithoutMoving(pos, expected))
                    return false;

                if (!parser_ttl_list.parse(pos, command_ttl, expected))
                    return false;

                command->type = ASTAlterCommand::MODIFY_TTL;
            }
            else if (s_remove_ttl.ignore(pos, expected))
            {
                command->type = ASTAlterCommand::REMOVE_TTL;
            }
            else if (s_materialize_ttl.ignore(pos, expected))
            {
                command->type = ASTAlterCommand::MATERIALIZE_TTL;

                if (s_in_partition.ignore(pos, expected))
                {
                    if (!parser_partition.parse(pos, command_partition, expected))
                        return false;
                }
            }
            else if (s_rewrite_parts.ignore(pos, expected))
            {
                command->type = ASTAlterCommand::REWRITE_PARTS;

                if (s_in_partition.ignore(pos, expected))
                {
                    if (!parser_partition.parse(pos, command_partition, expected))
                        return false;
                }
            }
            else if (s_modify_setting.ignore(pos, expected))
            {
                if (!parser_settings.parse(pos, command_settings_changes, expected))
                    return false;
                command->type = ASTAlterCommand::MODIFY_SETTING;
            }
            else if (s_reset_setting.ignore(pos, expected))
            {
                if (!parser_reset_setting.parse(pos, command_settings_resets, expected))
                    return false;
                command->type = ASTAlterCommand::RESET_SETTING;
            }
            else if (s_modify_query.ignore(pos, expected))
            {
                if (!select_p.parse(pos, command_select, expected))
                    return false;
                command->type = ASTAlterCommand::MODIFY_QUERY;
            }
            else if (s_modify_sql_security.checkWithoutMoving(pos, expected))
            {
                s_modify.ignore(pos, expected);
                if (!sql_security_p.parse(pos, command_sql_security, expected))
                    return false;
                command->type = ASTAlterCommand::MODIFY_SQL_SECURITY;
            }
            else if (s_modify_definer.checkWithoutMoving(pos, expected))
            {
                s_modify.ignore(pos, expected);
                if (!sql_security_p.parse(pos, command_sql_security, expected))
                    return false;
                command->type = ASTAlterCommand::MODIFY_SQL_SECURITY;
            }
            else if (s_modify_refresh.ignore(pos, expected))
            {
                if (!refresh_p.parse(pos, command_refresh, expected))
                    return false;
                command->type = ASTAlterCommand::MODIFY_REFRESH;
            }
            else if (s_modify_comment.ignore(pos, expected))
            {
                if (!parser_string_literal.parse(pos, command_comment, expected))
                    return false;

                command->type = ASTAlterCommand::MODIFY_COMMENT;
            }
            else if (s_apply_deleted_mask.ignore(pos, expected))
            {
                command->type = ASTAlterCommand::APPLY_DELETED_MASK;

                if (s_in_partition.ignore(pos, expected))
                {
                    if (!parser_partition.parse(pos, command_partition, expected))
                        return false;
                }
            }
            else if (s_apply_patches.ignore(pos, expected))
            {
                command->type = ASTAlterCommand::APPLY_PATCHES;

                if (s_in_partition.ignore(pos, expected))
                {
                    if (!parser_partition.parse(pos, command_partition, expected))
                        return false;
                }
            }
            else if (s_execute.ignore(pos, expected))
            {
                command->type = ASTAlterCommand::EXECUTE_COMMAND;

                ParserIdentifier command_name_parser;
                ASTPtr command_name_ast;
                if (!command_name_parser.parse(pos, command_name_ast, expected))
                    return false;
                command->execute_command_name = getIdentifierName(command_name_ast);

                if (!parser_opening_round_bracket.ignore(pos, expected))
                    return false;

                ASTPtr execute_args_list;
                ParserList args_parser(
                    std::make_unique<ParserExpressionWithOptionalAlias>(false),
                    std::make_unique<ParserToken>(TokenType::Comma),
                    /* allow_empty = */ true);
                if (!args_parser.parse(pos, execute_args_list, expected))
                    return false;

                if (!parser_closing_round_bracket.ignore(pos, expected))
                    return false;

                if (execute_args_list)
                    command->execute_args = command->children.emplace_back(std::move(execute_args_list)).get();
            }
            else
                return false;
            break;
        }
        default:
            break;
    }

    if (with_round_bracket)
    {
        if (!parser_closing_round_bracket.ignore(pos, expected))
            return false;
    }

    if (command_col_decl)
        command->col_decl = command->children.emplace_back(std::move(command_col_decl)).get();
    if (command_column)
        command->column = command->children.emplace_back(std::move(command_column)).get();
    if (command_order_by)
        command->order_by = command->children.emplace_back(std::move(command_order_by)).get();
    if (command_sample_by)
        command->sample_by = command->children.emplace_back(std::move(command_sample_by)).get();
    if (command_index_decl)
        command->index_decl = command->children.emplace_back(std::move(command_index_decl)).get();
    if (command_index)
        command->index = command->children.emplace_back(std::move(command_index)).get();
    if (command_constraint_decl)
        command->constraint_decl = command->children.emplace_back(std::move(command_constraint_decl)).get();
    if (command_constraint)
        command->constraint = command->children.emplace_back(std::move(command_constraint)).get();
    if (command_projection_decl)
        command->projection_decl = command->children.emplace_back(std::move(command_projection_decl)).get();
    if (command_projection)
        command->projection = command->children.emplace_back(std::move(command_projection)).get();
    if (command_statistics_decl)
        command->statistics_decl = command->children.emplace_back(std::move(command_statistics_decl)).get();
    if (command_partition)
        command->partition = command->children.emplace_back(std::move(command_partition)).get();
    if (command_predicate)
        command->predicate = command->children.emplace_back(std::move(command_predicate)).get();
    if (command_update_assignments)
        command->update_assignments = command->children.emplace_back(std::move(command_update_assignments)).get();
    if (command_comment)
        command->comment = command->children.emplace_back(std::move(command_comment)).get();
    if (command_ttl)
        command->ttl = command->children.emplace_back(std::move(command_ttl)).get();
    if (command_settings_changes)
        command->settings_changes = command->children.emplace_back(std::move(command_settings_changes)).get();
    if (command_settings_resets)
        command->settings_resets = command->children.emplace_back(std::move(command_settings_resets)).get();
    if (command_add_enum_values)
        command->add_enum_values = command->children.emplace_back(std::move(command_add_enum_values));
    if (command_select)
        command->select = command->children.emplace_back(std::move(command_select)).get();
    if (command_sql_security)
        command->sql_security = command->children.emplace_back(std::move(command_sql_security)).get();
    if (command_rename_to)
        command->rename_to = command->children.emplace_back(std::move(command_rename_to)).get();
    if (command_snapshot_desc)
        command->snapshot_desc = command->children.emplace_back(std::move(command_snapshot_desc)).get();
    if (command_refresh)
        command->refresh = command->children.emplace_back(std::move(command_refresh)).get();

    return true;
}


bool ParserAlterCommandList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto command_list = make_intrusive<ASTExpressionList>();
    node = command_list;

    ParserToken s_comma(TokenType::Comma);

    const auto with_round_bracket = pos->type == TokenType::OpeningRoundBracket;

    ParserAlterCommand p_command(with_round_bracket, alter_object);

    do
    {
        ASTPtr command;
        if (!p_command.parse(pos, command, expected))
            return false;

        command_list->children.push_back(command);
    }
    while (s_comma.ignore(pos, expected));

    return true;
}

bool ParserAlterQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = make_intrusive<ASTAlterQuery>();
    node = query;

    ParserKeyword s_alter_table(Keyword::ALTER_TABLE);
    ParserKeyword s_alter_temporary_table(Keyword::ALTER_TEMPORARY_TABLE);
    ParserKeyword s_alter_database(Keyword::ALTER_DATABASE);

    ASTAlterQuery::AlterObjectType alter_object_type = {};

    if (s_alter_table.ignore(pos, expected) || s_alter_temporary_table.ignore(pos, expected))
    {
        alter_object_type = ASTAlterQuery::AlterObjectType::TABLE;
    }
    else if (s_alter_database.ignore(pos, expected))
    {
        alter_object_type = ASTAlterQuery::AlterObjectType::DATABASE;
    }
    else
        return false;

    if (alter_object_type == ASTAlterQuery::AlterObjectType::DATABASE)
    {
        if (!parseDatabaseAsAST(pos, expected, query->database))
            return false;

        String cluster_str;
        if (ParserKeyword(Keyword::ON).ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
        }
        query->cluster = cluster_str;
    }
    else
    {
        if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
            return false;

        String cluster_str;
        if (ParserKeyword(Keyword::ON).ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
        }
        query->cluster = cluster_str;
    }

    ParserAlterCommandList p_command_list(alter_object_type);
    ASTPtr command_list;
    if (!p_command_list.parse(pos, command_list, expected))
        return false;

    query->set(query->command_list, command_list);
    query->alter_object = alter_object_type;

    if (query->database)
        query->children.push_back(query->database);

    if (query->table)
        query->children.push_back(query->table);

    return true;
}

}

namespace DB
{

void registerStatementAlter(StatementFactory & factory)
{
    factory.registerStatement("ALTER",
    {
        .description = R"(
Changes the structure, the settings or the data of a table, of a database, of a view, or of an access entity.

Most `ALTER TABLE` queries which change the data are implemented as mutations: they are asynchronous background
processes which rewrite the affected data parts. Most `ALTER TABLE` queries are supported only for tables of the
`*MergeTree`, `Merge` and `Distributed` families.
)",
        .syntax = R"(
ALTER TABLE [db.]name [ON CLUSTER cluster] action [, action ...]
ALTER DATABASE [db.]name [ON CLUSTER cluster] action
ALTER NAMED COLLECTION ...
ALTER USER | ROLE | ROW POLICY | MASKING POLICY | QUOTA | SETTINGS PROFILE ...
)",
        .examples = {{"Add a column", "ALTER TABLE test ADD COLUMN x UInt64;", ""}},
        .related = {
            "ALTER TABLE ... COLUMN", "ALTER TABLE ... PARTITION", "ALTER TABLE ... DELETE", "ALTER TABLE ... UPDATE",
            "CREATE", "SYSTEM"},
    });

    factory.registerStatement("ALTER TABLE ... COLUMN",
    {
        .description = R"(
Changes the structure of a table: adds, drops, renames, clears, comments, modifies or materializes columns. A single
query can contain a list of comma-separated actions.

`ADD`, `DROP`, `COMMENT`, `MODIFY` and `ALTER` of a column are lightweight operations which only change metadata or
remove files, whereas `CLEAR`, `MATERIALIZE` and a `MODIFY` which changes the type of a column are implemented as
mutations.
)",
        .syntax = R"(
ALTER [TEMPORARY] TABLE [db].name [ON CLUSTER cluster] ADD|DROP|RENAME|CLEAR|COMMENT|{MODIFY|ALTER}|MATERIALIZE COLUMN ...

ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [COMMENT 'comment for column'] [codec] [STATISTICS] [TTL] [settings] [AFTER name_after | FIRST]
DROP COLUMN [IF EXISTS] name
RENAME COLUMN [IF EXISTS] name TO new_name
CLEAR COLUMN [IF EXISTS] name [IN PARTITION partition_id]
COMMENT COLUMN [IF EXISTS] name 'Text comment'
MODIFY COLUMN [IF EXISTS] name [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
MODIFY COLUMN [IF EXISTS] name REMOVE property
MATERIALIZE COLUMN name [IN PARTITION partition_id]
)",
        .examples = {
            {"Add a column after another column", "ALTER TABLE alter_test ADD COLUMN Added2 UInt32 AFTER NestedColumn;", ""},
            {"Change the type of a column", "ALTER TABLE alter_test MODIFY COLUMN Added2 UInt64;", ""},
        },
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE", "CODEC", "ALTER TABLE ... MODIFY TTL"},
    });

    factory.registerStatement("ALTER TABLE ... PARTITION",
    {
        .description = R"(
Manipulates partitions and parts of a table: detaches, drops, attaches, replaces, moves, freezes, unfreezes and
fetches them, and updates the metadata of a partition.
)",
        .syntax = R"(
ALTER TABLE table_name [ON CLUSTER cluster] DETACH PARTITION|PART partition_expr
ALTER TABLE table_name [ON CLUSTER cluster] DROP PARTITION|PART partition_expr
ALTER TABLE table_name [ON CLUSTER cluster] DROP DETACHED PARTITION|PART ALL|partition_expr
ALTER TABLE table_name [ON CLUSTER cluster] FORGET PARTITION partition_expr
ALTER TABLE table_name [ON CLUSTER cluster] ATTACH PARTITION|PART partition_expr
ALTER TABLE table2 [ON CLUSTER cluster] ATTACH PARTITION partition_expr FROM table1
ALTER TABLE table2 [ON CLUSTER cluster] REPLACE PARTITION partition_expr FROM table1
ALTER TABLE table_source [ON CLUSTER cluster] MOVE PARTITION partition_expr TO TABLE table_dest
ALTER TABLE table_name [ON CLUSTER cluster] MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR COLUMN column_name IN PARTITION partition_expr
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR INDEX index_name IN PARTITION partition_expr
ALTER TABLE table_name [ON CLUSTER cluster] FREEZE [PARTITION partition_expr] [WITH NAME 'backup_name']
ALTER TABLE table_name [ON CLUSTER cluster] UNFREEZE [PARTITION partition_expr] WITH NAME 'backup_name'
ALTER TABLE table_name [ON CLUSTER cluster] FETCH PARTITION|PART partition_expr FROM 'path-in-zookeeper'
ALTER TABLE table_name [ON CLUSTER cluster] MODIFY PARTITION|PART partition_expr ...
)",
        .examples = {
            {"Detach a partition", "ALTER TABLE mt DETACH PARTITION '2020-11-21';", ""},
            {"Drop a part", "ALTER TABLE mt DROP PART 'all_4_4_0';", ""},
        },
        .parent = "ALTER",
        .related = {"ALTER", "SYSTEM", "OPTIMIZE", "TRUNCATE"},
    });

    factory.registerStatement("ALTER TABLE ... DELETE",
    {
        .description = R"(
Deletes the rows matching the filter expression. Implemented as a mutation: every data part containing matching rows
is rewritten, therefore this is a heavyweight operation. For deleting a small amount of rows, prefer the lightweight
`DELETE` statement.
)",
        .syntax = R"(
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
)",
        .examples = {{"Delete rows by a condition", "ALTER TABLE test DELETE WHERE x = 1;", ""}},
        .parent = "ALTER",
        .related = {"ALTER", "DELETE", "TRUNCATE", "ALTER TABLE ... UPDATE"},
    });

    factory.registerStatement("ALTER TABLE ... UPDATE",
    {
        .description = R"(
Updates the columns of the rows matching the filter expression. Implemented as a mutation: every data part containing
matching rows is rewritten, therefore this is a heavyweight operation. For updating a small amount of rows, prefer the
lightweight `UPDATE` statement.
)",
        .syntax = R"(
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_id] WHERE filter_expr
)",
        .examples = {{"Update a column by a condition", "ALTER TABLE test UPDATE x = 2 WHERE 1;", ""}},
        .parent = "ALTER",
        .related = {"ALTER", "UPDATE", "ALTER TABLE ... DELETE", "ALTER TABLE ... APPLY PATCHES"},
    });

    factory.registerStatement("ALTER TABLE ... MODIFY ORDER BY",
    {
        .description = R"(
Changes the sorting key of the table. The primary key remains the same. The command is lightweight in the sense that
it only changes metadata, therefore the new sorting key may only extend the existing one with new columns which are
not in the primary key.
)",
        .syntax = R"(
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY ORDER BY new_expression
)",
        .examples = {{"Extend the sorting key", "ALTER TABLE test MODIFY ORDER BY (x, y);", ""}},
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE", "ALTER TABLE ... MODIFY SAMPLE BY"},
    });

    factory.registerStatement("ALTER TABLE ... MODIFY SAMPLE BY",
    {
        .description = R"(
Changes or removes the sampling key of the table. The command is lightweight in the sense that it only changes
metadata; it is the responsibility of the user that the data actually satisfies the new sampling expression.
)",
        .syntax = R"(
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SAMPLE BY new_expression
ALTER TABLE [db].name [ON CLUSTER cluster] REMOVE SAMPLE BY
)",
        .examples = {{"Remove the sampling key", "ALTER TABLE test REMOVE SAMPLE BY;", ""}},
        .parent = "ALTER",
        .related = {"ALTER", "SAMPLE", "ALTER TABLE ... MODIFY ORDER BY"},
    });

    factory.registerStatement("ALTER TABLE ... MODIFY TTL",
    {
        .description = R"(
Changes or removes the `TTL` of the table. Removing the `TTL` does not delete the rows which the expired `TTL` rule
would have removed, it only stops applying the rule.
)",
        .syntax = R"(
ALTER TABLE [db.]table_name [ON CLUSTER cluster] MODIFY TTL ttl_expression
ALTER TABLE [db.]table_name [ON CLUSTER cluster] REMOVE TTL
)",
        .examples = {{"Remove the TTL of a table", "ALTER TABLE table_with_ttl REMOVE TTL;", ""}},
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE", "ALTER TABLE ... COLUMN", "OPTIMIZE"},
    });

    factory.registerStatement("ALTER TABLE ... MODIFY SETTING",
    {
        .description = R"(
Changes the settings of a table or resets them to their default values. A single query can change several settings at
once. Modifying a setting which does not exist raises an exception.
)",
        .syntax = R"(
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SETTING setting_name = value [, ...]
ALTER TABLE [db].name [ON CLUSTER cluster] RESET SETTING setting_name [, ...]
)",
        .examples = {{"Change a table setting", "ALTER TABLE test MODIFY SETTING max_part_loading_threads = 8;", ""}},
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE", "SET"},
    });

    factory.registerStatement("ALTER TABLE ... CONSTRAINT",
    {
        .description = R"(
Adds, modifies or drops a constraint of a table. Constraints are only checked for newly inserted rows, the existing
data is not validated.
)",
        .syntax = R"(
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name {CHECK|ASSUME} expression
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY CONSTRAINT [IF EXISTS] constraint_name {CHECK|ASSUME} expression
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name
)",
        .examples = {{"Add a constraint", "ALTER TABLE test ADD CONSTRAINT c CHECK x > 0;", ""}},
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE"},
    });

    factory.registerStatement("ALTER TABLE ... INDEX",
    {
        .description = R"(
Adds, drops, materializes or clears a data skipping index of a table. `ADD`, `DROP` and `CLEAR` are lightweight
operations which only change metadata or remove files, whereas `MATERIALIZE` is implemented as a mutation which
rebuilds the index for the existing data.
)",
        .syntax = R"(
ALTER TABLE [db.]table_name [ON CLUSTER cluster] ADD INDEX [IF NOT EXISTS] name expression TYPE type [GRANULARITY value] [FIRST|AFTER name]
ALTER TABLE [db.]table_name [ON CLUSTER cluster] DROP INDEX [IF EXISTS] name
ALTER TABLE [db.]table_name [ON CLUSTER cluster] MATERIALIZE INDEX [IF EXISTS] name [IN PARTITION partition_name]
ALTER TABLE [db.]table_name [ON CLUSTER cluster] CLEAR INDEX [IF EXISTS] name [IN PARTITION partition_name]
)",
        .examples = {{"Add and materialize a skipping index", R"(
ALTER TABLE test ADD INDEX idx x TYPE minmax GRANULARITY 1;
ALTER TABLE test MATERIALIZE INDEX idx;
)", ""}},
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE", "HYPOTHETICAL INDEX", "ALTER TABLE ... PROJECTION"},
    });

    factory.registerStatement("ALTER TABLE ... PROJECTION",
    {
        .description = R"(
Adds, drops, materializes or clears a projection of a table. A projection stores the data of the table in another
order or pre-aggregated, so that queries which do not match the primary key of the table can still be answered
efficiently.
)",
        .syntax = R"(
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [ORDER BY] | [GROUP BY] ) [WITH SETTINGS ( setting_name = setting_value, ... )]
ALTER TABLE [db.]name [ON CLUSTER cluster] DROP PROJECTION [IF EXISTS] name
ALTER TABLE [db.]name [ON CLUSTER cluster] MATERIALIZE PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
ALTER TABLE [db.]name [ON CLUSTER cluster] CLEAR PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
)",
        .examples = {{"Add and materialize a projection", R"(
ALTER TABLE visits_order ADD PROJECTION user_name_projection (SELECT * ORDER BY user_name);
ALTER TABLE visits_order MATERIALIZE PROJECTION user_name_projection;
)", ""}},
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE", "ALTER TABLE ... INDEX"},
    });

    factory.registerStatement("ALTER TABLE ... STATISTICS",
    {
        .description = R"(
Adds, modifies, drops, materializes or clears the statistics of the columns of a table. Column statistics help the
query optimizer to estimate the selectivity of predicates.
)",
        .syntax = R"(
ALTER TABLE [db].table ADD STATISTICS [IF NOT EXISTS] (column list) TYPE (type list)
ALTER TABLE [db].table MODIFY STATISTICS (column list) TYPE (type list)
ALTER TABLE [db].table DROP STATISTICS [IF EXISTS] (column list)
ALTER TABLE [db].table CLEAR STATISTICS [IF EXISTS] (column list)
ALTER TABLE [db].table MATERIALIZE STATISTICS [IF EXISTS] (column list)
)",
        .examples = {{"Change the statistics of columns", "ALTER TABLE t1 MODIFY STATISTICS c, d TYPE TDigest, Uniq;", ""}},
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE", "EXPLAIN"},
    });

    factory.registerStatement("ALTER TABLE ... MODIFY COMMENT",
    {
        .description = R"(
Adds, modifies or removes the comment of a table, regardless of whether it was set before or not. The comment is
shown in `system.tables` and in the result of `SHOW CREATE TABLE`.
)",
        .syntax = R"(
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
)",
        .examples = {{"Change the comment of a table", "ALTER TABLE table_with_comment MODIFY COMMENT 'new comment on a table';", ""}},
        .parent = "ALTER",
        .related = {"ALTER", "ALTER DATABASE ... MODIFY COMMENT", "CREATE TABLE", "SHOW"},
    });

    factory.registerStatement("ALTER DATABASE ... MODIFY COMMENT",
    {
        .description = R"(
Adds, modifies or removes the comment of a database, regardless of whether it was set before or not. The comment is
shown in `system.databases` and in the result of `SHOW CREATE DATABASE`.
)",
        .syntax = R"(
ALTER DATABASE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
)",
        .examples = {{"Change the comment of a database", "ALTER DATABASE database_with_comment MODIFY COMMENT 'new comment on a database';", ""}},
        .parent = "ALTER",
        .related = {"ALTER", "ALTER TABLE ... MODIFY COMMENT", "CREATE DATABASE", "SHOW"},
    });

    factory.registerStatement("ALTER TABLE ... MODIFY QUERY",
    {
        .description = R"(
Changes the `SELECT` query of a materialized view without interrupting the ingestion process. The query was specified
when the materialized view was created. This statement is intended for a materialized view created with the
`TO [db.]name` clause; it does not change the structure of the target table of the view.
)",
        .syntax = R"(
ALTER TABLE [db.]name [ON CLUSTER cluster] MODIFY QUERY SELECT ...
)",
        .examples = {{"Change the query of a materialized view", R"(
ALTER TABLE mv MODIFY QUERY
    SELECT toStartOfDay(ts) ts, event_type, count() events_cnt
    FROM events
    GROUP BY ts, event_type;
)", ""}},
        .parent = "ALTER",
        .related = {"ALTER", "CREATE VIEW"},
    });

    factory.registerStatement("ALTER TABLE ... APPLY DELETED MASK",
    {
        .description = R"(
Applies the mask created by lightweight deletes and forcefully removes the rows marked as deleted from disk. The
command is a heavyweight mutation; it is semantically equal to `ALTER TABLE [db.]name DELETE WHERE _row_exists = 0`.
)",
        .syntax = R"(
ALTER TABLE [db].name [ON CLUSTER cluster] APPLY DELETED MASK [IN PARTITION partition_id]
)",
        .examples = {{"Materialize lightweight deletes", "ALTER TABLE my_table APPLY DELETED MASK;", ""}},
        .parent = "ALTER",
        .related = {"ALTER", "DELETE", "ALTER TABLE ... DELETE"},
    });

    factory.registerStatement("ALTER TABLE ... APPLY PATCHES",
    {
        .description = R"(
Manually triggers the materialization of the patch parts created by lightweight `UPDATE` statements. It forcefully
applies the pending patches to the data parts by rewriting only the affected columns.
)",
        .syntax = R"(
ALTER TABLE [db.]table [ON CLUSTER cluster] APPLY PATCHES [IN PARTITION partition_id]
)",
        .examples = {{"Materialize lightweight updates", "ALTER TABLE my_table APPLY PATCHES;", ""}},
        .parent = "ALTER",
        .related = {"ALTER", "UPDATE", "ALTER TABLE ... UPDATE"},
    });
}

}
