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
    ParserKeyword s_modify_projection(Keyword::MODIFY_PROJECTION);
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
    ASTPtr command_partitions;
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
            else if (s_modify_projection.ignore(pos, expected))
            {
                if (s_if_exists.ignore(pos, expected))
                    command->if_exists = true;

                if (!parser_projection_decl.parse(pos, command_projection_decl, expected))
                    return false;

                command->type = ASTAlterCommand::MODIFY_PROJECTION;
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
                    ParserList partition_list_parser(
                        std::make_unique<ParserPartition>(), std::make_unique<ParserToken>(TokenType::Comma), false);
                    ASTPtr partition_list_ast;
                    if (!partition_list_parser.parse(pos, partition_list_ast, expected))
                        return false;

                    auto & partition_list = partition_list_ast->as<ASTExpressionList &>();
                    if (partition_list.children.size() == 1)
                        command_partition = std::move(partition_list.children[0]);
                    else
                        command_partitions = std::move(partition_list_ast);
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
                    ParserList partition_list_parser(
                        std::make_unique<ParserPartition>(), std::make_unique<ParserToken>(TokenType::Comma), false);
                    ASTPtr partition_list_ast;
                    if (!partition_list_parser.parse(pos, partition_list_ast, expected))
                        return false;

                    auto & partition_list = partition_list_ast->as<ASTExpressionList &>();
                    if (partition_list.children.size() == 1)
                        command_partition = std::move(partition_list.children[0]);
                    else
                        command_partitions = std::move(partition_list_ast);
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
    if (command_partitions)
        command->partitions = command->children.emplace_back(std::move(command_partitions)).get();
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
        .description = R"DOCS_MD(
Most `ALTER TABLE` queries modify table settings or data:

| Modifier                                                                            |
|-------------------------------------------------------------------------------------|
| [COLUMN](/reference/statements/alter/column)                         |
| [PARTITION](/reference/statements/alter/partition)                   |
| [DELETE](/reference/statements/alter/delete)                         |
| [UPDATE](/reference/statements/alter/update)                         |
| [ORDER BY](/reference/statements/alter/order-by)                     |
| [SAMPLE BY](/reference/statements/alter/sample-by)                   |
| [INDEX](/reference/statements/alter/skipping-index)                  |
| [PROJECTION](/reference/statements/alter/projection)                 |
| [CONSTRAINT](/reference/statements/alter/constraint)                 |
| [TTL](/reference/statements/alter/ttl)                               |
| [STATISTICS](/reference/statements/alter/statistics)                 |
| [SETTING](/reference/statements/alter/setting)                       |
| [APPLY DELETED MASK](/reference/statements/alter/apply-deleted-mask) |
| [APPLY PATCHES](/reference/statements/alter/apply-patches)           |

<Note>
Most `ALTER TABLE` queries are supported only for [\*MergeTree](/reference/engines/table-engines/mergetree-family/index), [Merge](/reference/engines/table-engines/special/merge) and [Distributed](/reference/engines/table-engines/special/distributed) tables.
</Note>

These `ALTER` statements manipulate views:

| Statement                                                                           | Description                                                                          |
|-------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| [ALTER TABLE ... MODIFY QUERY](/reference/statements/alter/view)     | Modifies a [Materialized view](/reference/statements/create/view) structure.                                       |

These `ALTER` statements modify entities related to role-based access control:

| Statement                                                                       |
|---------------------------------------------------------------------------------|
| [USER](/reference/statements/alter/user)                         |
| [ROLE](/reference/statements/alter/role)                         |
| [QUOTA](/reference/statements/alter/quota)                       |
| [ROW POLICY](/reference/statements/alter/row-policy)             |
| [MASKING POLICY](/reference/statements/alter/masking-policy)     |
| [SETTINGS PROFILE](/reference/statements/alter/settings-profile) |

| Statement                                                                             | Description                                                                               |
|---------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| [ALTER TABLE ... MODIFY COMMENT](/reference/statements/alter/comment)  | Adds, modifies, or removes comments to the table, regardless if it was set before or not. |
| [ALTER DATABASE ... MODIFY COMMENT](/reference/statements/alter/database-comment) | Adds, modifies, or removes comments to the database, regardless if it was set before or not. |
| [ALTER NAMED COLLECTION](/reference/statements/alter/named-collection) | Modifies [Named Collections](/concepts/features/configuration/server-config/named-collections).                   |

## Mutations {#mutations}

`ALTER` queries that are intended to manipulate table data are implemented with a mechanism called "mutations", most notably [ALTER TABLE ... DELETE](/reference/statements/alter/delete) and [ALTER TABLE ... UPDATE](/reference/statements/alter/update). They are asynchronous background processes similar to merges in [MergeTree](/reference/engines/table-engines/mergetree-family/index) tables that to produce new "mutated" versions of parts.

For `*MergeTree` tables mutations execute by **rewriting whole data parts**.
There is no atomicity — parts are substituted for mutated parts as soon as they are ready and a `SELECT` query that started executing during a mutation will see data from parts that have already been mutated along with data from parts that have not been mutated yet.

Mutations are totally ordered by their creation order and are applied to each part in that order. Mutations are also partially ordered with `INSERT INTO` queries: data that was inserted into the table before the mutation was submitted will be mutated and data that was inserted after that will not be mutated. Note that mutations do not block inserts in any way.

A mutation query returns immediately after the mutation entry is added (in case of replicated tables to ZooKeeper, for non-replicated tables - to the filesystem). The mutation itself executes asynchronously using the system profile settings. To track the progress of mutations you can use the [`system.mutations`](/reference/system-tables/mutations) table. A mutation that was successfully submitted will continue to execute even if ClickHouse servers are restarted. There is no way to roll back the mutation once it is submitted, but if the mutation is stuck for some reason it can be cancelled with the [`KILL MUTATION`](/reference/statements/kill#kill-mutation) query.

Entries for finished mutations are not deleted right away (the number of preserved entries is determined by the `finished_mutations_to_keep` storage engine parameter). Older mutation entries are deleted.

## Synchronicity of ALTER Queries {#synchronicity-of-alter-queries}

For non-replicated tables, all `ALTER` queries are performed synchronously. For replicated tables, the query just adds instructions for the appropriate actions to `ZooKeeper`, and the actions themselves are performed as soon as possible. However, the query can wait for these actions to be completed on all the replicas.

For `ALTER` queries that creates mutations (e.g.: including, but not limited to `UPDATE`, `DELETE`, `MATERIALIZE INDEX`, `MATERIALIZE PROJECTION`, `MATERIALIZE COLUMN`, `APPLY DELETED MASK`, `APPLY PATCHES`, `CLEAR STATISTIC`, `MATERIALIZE STATISTIC`) the synchronicity is defined by the [mutations_sync](/reference/settings/session-settings/mutations#mutations_sync) setting.

For other `ALTER` queries which only modify the metadata, you can use the [alter_sync](/reference/settings/session-settings/alter#alter_sync) setting to set up waiting.

You can specify how long (in seconds) to wait for inactive replicas to execute all `ALTER` queries with the [replication_wait_for_inactive_replica_timeout](/reference/settings/session-settings/other#replication_wait_for_inactive_replica_timeout) setting.

<Note>
For all `ALTER` queries, if `alter_sync = 2` and some replicas are not active for more than the time, specified in the `replication_wait_for_inactive_replica_timeout` setting, then an exception `UNFINISHED` is thrown.
</Note>

### Concurrent `ALTER` assignment on one table {#concurrent-alter-assignment-on-one-table}

On replicated tables, submitting several separate `ALTER` statements against the same table in quick succession can fail with `CANNOT_ASSIGN_ALTER` (code 517). The replicated path raises this when the replica has not yet applied some previous `ALTER`s (metadata version still behind the common metadata — the server may say the replica "still not applied some of previous alters" or "Probably too many alters executing concurrently"). That condition can remain true even after an earlier `ALTER` has already been assigned. This is a **general concurrent metadata-`ALTER` / mutation** condition — it is not limited to mutation-only statements. Ordinary concurrent metadata alters (`ADD` / `DROP` / `MODIFY`, and similar) can raise the same retryable code (see for example the retry path covered by `tests/queries/0_stateless/03518_alter_logical_race.sh`).

Approaches that avoid the race:

- Combine independent metadata operations into a **single** multi-clause `ALTER` when the grammar allows it (for example multiple `ADD INDEX` clauses).
- Serialize `ALTER` statements and retry on code 517 until previous `ALTER`s have been applied on the replica.
- For mutation-producing `ALTER`s, wait for the previous mutation to finish using a documented observable such as [`mutations_sync`](/reference/settings/session-settings/mutations#mutations_sync) or `is_done` in [`system.mutations`](/reference/system-tables/mutations) before submitting the next one.

### Combining `MATERIALIZE INDEX` clauses {#combining-materialize-index-clauses}

Multiple `MATERIALIZE INDEX` clauses can appear in one `ALTER`. The covered case in-tree is packing several `ADD INDEX` clauses together with `MATERIALIZE INDEX` for those same new indexes in a single statement (see `tests/queries/0_stateless/02911_add_index_and_materialize_index.sql`). That packed `ADD INDEX` + `MATERIALIZE INDEX` form mixes an `AlterCommand` segment with a `MutationCommand` segment, so **`DatabaseReplicated` rejects it** with `QUERY_IS_PROHIBITED` (`InterpreterAlterQuery::validateReplicatedDatabaseSegments`). Treat the `02911` example as valid for ordinary (non-`DatabaseReplicated`) databases; on `DatabaseReplicated`, keep metadata changes and materialize mutations in separate statements.

In the current implementation, each `MATERIALIZE INDEX` clause is resolved against the table metadata snapshot when the mutation is prepared, so materialize-only multi-clause forms on already-existing indexes follow the same preparation path (mutation-only, so they stay within one segment). That exact shape is not yet covered by a focused stateless test; treat it as current implementation behavior rather than a separately guaranteed contract until such coverage exists.

If you need ordered mutation apply, you can still issue one `MATERIALIZE INDEX` per statement and wait with [`mutations_sync`](/reference/settings/session-settings/mutations#mutations_sync).

## Related content {#related-content}

- Blog: [Handling Updates and Deletes in ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
)DOCS_MD",
        .syntax = R"(
ALTER TABLE [db.]name [ON CLUSTER cluster] action [, action ...]
ALTER DATABASE [db.]name [ON CLUSTER cluster] action
ALTER NAMED COLLECTION ...
ALTER USER | ROLE | ROW POLICY | MASKING POLICY | QUOTA | SETTINGS PROFILE ...
)",
        .related = {
            "ALTER TABLE ... COLUMN", "ALTER TABLE ... PARTITION", "ALTER TABLE ... DELETE", "ALTER TABLE ... UPDATE",
            "CREATE", "SYSTEM"},
    });

    factory.registerStatement("ALTER TABLE ... COLUMN",
    {
        .description = R"DOCS_MD(
A set of queries that allow changing the table structure.

Syntax:

```sql
ALTER [TEMPORARY] TABLE [db].name [ON CLUSTER cluster] ADD|DROP|RENAME|CLEAR|COMMENT|{MODIFY|ALTER}|MATERIALIZE COLUMN ...
```

In the query, specify a list of one or more comma-separated actions.
Each action is an operation on a column.

The following actions are supported:

- [ADD COLUMN](#add-column) — Adds a new column to the table.
- [DROP COLUMN](#drop-column) — Deletes the column.
- [RENAME COLUMN](#rename-column) — Renames an existing column.
- [CLEAR COLUMN](#clear-column) — Resets column values.
- [COMMENT COLUMN](#comment-column) — Adds a text comment to the column.
- [MODIFY COLUMN](#modify-column) — Changes column's type, default expression, TTL, and column settings.
- [MODIFY COLUMN REMOVE](#modify-column-remove) — Removes one of the column properties.
- [MODIFY COLUMN MODIFY SETTING](#modify-column-modify-setting) - Changes column settings.
- [MODIFY COLUMN RESET SETTING](#modify-column-reset-setting) - Reset column settings.
- [MODIFY COLUMN ADD ENUM VALUES](#modify-column-add-enum-values) - Adds new values to Enum.
- [MATERIALIZE COLUMN](#materialize-column) — Materializes the column in the parts where the column is missing.
These actions are described in detail below.

## ADD COLUMN {#add-column}

```sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [COMMENT 'comment for column'] [codec] [STATISTICS] [TTL] [settings] [AFTER name_after | FIRST]
```

Adds a new column to the table with the specified `name`, `type`, [`codec`](/reference/statements/create/table/codec) and `default_expr` (see the section [Default expressions](/reference/statements/create/table#default_values)). The modifiers that follow the type can be written in any order, and each of them at most once - see [column description](/reference/statements/create/table#with-explicit-schema).

If the `IF NOT EXISTS` clause is included, the query won't return an error if the column already exists. If you specify `AFTER name_after` (the name of another column), the column is added after the specified one in the list of table columns. If you want to add a column to the beginning of the table use the `FIRST` clause. Otherwise, the column is added to the end of the table. For a chain of actions, `name_after` can be the name of a column that is added in one of the previous actions.

Adding a column just changes the table structure, without performing any actions with data. The data does not appear on the disk after `ALTER`. If the data is missing for a column when reading from the table, it is filled in with default values (by performing the default expression if there is one, or using zeros or empty strings). The column appears on the disk after merging data parts (see [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree)).

This approach allows us to complete the `ALTER` query instantly, without increasing the volume of old data.

Example:

```sql
ALTER TABLE alter_test ADD COLUMN Added1 UInt32 FIRST;
ALTER TABLE alter_test ADD COLUMN Added2 UInt32 AFTER NestedColumn;
ALTER TABLE alter_test ADD COLUMN Added3 UInt32 AFTER ToDrop;
DESC alter_test FORMAT TSV;
```

```text
Added1  UInt32
CounterID       UInt32
StartDate       Date
UserID  UInt32
VisitID UInt32
NestedColumn.A  Array(UInt8)
NestedColumn.S  Array(String)
Added2  UInt32
ToDrop  UInt32
Added3  UInt32
```

## DROP COLUMN {#drop-column}

```sql
DROP COLUMN [IF EXISTS] name
```

Deletes the column with the name `name`. If the `IF EXISTS` clause is specified, the query won't return an error if the column does not exist.

Deletes data from the file system. Since this deletes entire files, the query is completed almost instantly.

<Tip>
You can't delete a column if it is referenced by [materialized view](/reference/statements/create/view). Otherwise, it returns an error.
</Tip>

Example:

```sql
ALTER TABLE visits DROP COLUMN browser
```

## RENAME COLUMN {#rename-column}

```sql
RENAME COLUMN [IF EXISTS] name to new_name
```

Renames the column `name` to `new_name`. If the `IF EXISTS` clause is specified, the query won't return an error if the column does not exist. Since renaming does not involve the underlying data, the query is completed almost instantly.

**NOTE**: Columns specified in the key expression of the table (either with `ORDER BY` or `PRIMARY KEY`) cannot be renamed. Trying to change these columns will produce `SQL Error [524]`.

Example:

```sql
ALTER TABLE visits RENAME COLUMN webBrowser TO browser
```

## CLEAR COLUMN {#clear-column}

```sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

Resets all data in a column for a specified partition. Read more about setting the partition name in the section [How to set the partition expression](/reference/statements/alter/partition#how-to-set-partition-expression).

If the `IF EXISTS` clause is specified, the query won't return an error if the column does not exist.

Example:

```sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

## COMMENT COLUMN {#comment-column}

```sql
COMMENT COLUMN [IF EXISTS] name 'Text comment'
```

Adds a comment to the column. If the `IF EXISTS` clause is specified, the query won't return an error if the column does not exist.

Each column can have one comment. If a comment already exists for the column, a new comment overwrites the previous comment.

Comments are stored in the `comment_expression` column returned by the [DESCRIBE TABLE](/reference/statements/describe-table) query.

Example:

```sql
ALTER TABLE visits COMMENT COLUMN browser 'This column shows the browser used for accessing the site.'
```

## MODIFY COLUMN {#modify-column}

```sql
MODIFY COLUMN [IF EXISTS] name
    [type] [default_expr] [COMMENT 'comment for column'] [codec] [STATISTICS] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
ALTER COLUMN [IF EXISTS] name
    TYPE [type] [default_expr] [COMMENT 'comment for column'] [codec] [STATISTICS] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
```

The modifiers that follow the type can be written in any order, and each of them at most once - see [column description](/reference/statements/create/table#with-explicit-schema).

This query changes the `name` column properties:

- Type

- Default expression

- Compression Codec

- TTL

- Column-level Settings

- Enum Values for Enum/Enum8/Enum16 types

For examples of columns compression CODECS modifying, see [Column Compression Codecs](/reference/statements/create/table/codec).

For examples of columns TTL modifying, see [Column TTL](/reference/engines/table-engines/mergetree-family/mergetree#mergetree-column-ttl).

For examples of column-level settings modifying, see [Column-level Settings](/reference/engines/table-engines/mergetree-family/mergetree#column-level-settings).

If the `IF EXISTS` clause is specified, the query won't return an error if the column does not exist.

When changing the type, values are converted as if the [toType](/reference/functions/regular-functions/type-conversion-functions) functions were applied to them. If only the default expression is changed, the query does not do anything complex, and is completed almost instantly.

Example:

```sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

Changing the column type is the only complex action – it changes the contents of files with data. For large tables, this may take a long time.

The query also can change the order of the columns using `FIRST | AFTER` clause, see [ADD COLUMN](#add-column) description, but column type is mandatory in this case.

Example:

```sql
CREATE TABLE users (
    c1 Int16,
    c2 String
) ENGINE = MergeTree
ORDER BY c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴

ALTER TABLE users MODIFY COLUMN c2 String FIRST;

DESCRIBE users;
┌─name─┬─type───┬
│ c2   │ String │
│ c1   │ Int16  │
└──────┴────────┴

ALTER TABLE users ALTER COLUMN c2 TYPE String AFTER c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴
```

The `ALTER` query is atomic. For MergeTree tables it is also lock-free.

The `ALTER` query for changing columns is replicated. The instructions are saved in ZooKeeper, then each replica applies them. All `ALTER` queries are run in the same order. The query waits for the appropriate actions to be completed on the other replicas. However, a query to change columns in a replicated table can be interrupted, and all actions will be performed asynchronously.

<Note>
Please be careful when changing a Nullable column to Non-Nullable. Make sure it doesn't have any NULL values, otherwise it will cause problems when reading from it. In that case, the workaround would be to Kill the mutation and revert the column back to Nullable type.
</Note>

## MODIFY COLUMN REMOVE {#modify-column-remove}

Removes one of the column properties: `DEFAULT`, `ALIAS`, `MATERIALIZED`, `CODEC`, `COMMENT`, `TTL`, `SETTINGS`.

Syntax:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name REMOVE property;
```

**Example**

Remove TTL:

```sql
ALTER TABLE table_with_ttl MODIFY COLUMN column_ttl REMOVE TTL;
```

**See Also**

- [REMOVE TTL](/reference/statements/alter/ttl).

## MODIFY COLUMN MODIFY SETTING {#modify-column-modify-setting}

Modify a column setting.

Syntax:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING name=value,...;
```

**Example**

Modify column's `max_compress_block_size` to `1MB`:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING max_compress_block_size = 1048576;
```

## MODIFY COLUMN RESET SETTING {#modify-column-reset-setting}

Reset a column setting, also removes the setting declaration in the column expression of the table's CREATE query.

Syntax:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING name,...;
```

**Example**

Reset column setting `max_compress_block_size` to it's default value:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING max_compress_block_size;
```

## MODIFY COLUMN ADD ENUM VALUES {#modify-column-add-enum-values}

Adds new values to a column of type `Enum`, `Enum8`, `Enum16`, `Nullable(Enum)`, `Nullable(Enum8)` or `Nullable(Enum16)`

Syntax:

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('EnumName' [= number], ...);
```

**Example**

Add two values to column `enum_column_name`:

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('Hundred' = 100, 'HundredOne');
```

## MATERIALIZE COLUMN {#materialize-column}

Materializes a column with a `DEFAULT` or `MATERIALIZED` value expression. When adding a materialized column using `ALTER TABLE table_name ADD COLUMN column_name MATERIALIZED`, existing rows without materialized values are not automatically filled. `MATERIALIZE COLUMN` statement can be used to rewrite existing column data after a `DEFAULT` or `MATERIALIZED` expression has been added or updated (which only updates the metadata but does not change existing data). Note that materializing a column in the sort key is an invalid operation because it could break the sort order.
Implemented as a [mutation](/reference/statements/alter/index#mutations).

For columns with a new or updated `MATERIALIZED` value expression, all existing rows are rewritten.

For columns with a new or updated `DEFAULT` value expression, the behavior depends on the ClickHouse version:
- In ClickHouse < v24.2, all existing rows are rewritten.
- ClickHouse >= v24.2 distinguishes if a row value in a column with `DEFAULT` value expression was explicitly specified when it was inserted, or not, i.e. calculated from the `DEFAULT` value expression. If the value was explicitly specified, ClickHouse keeps it as is. If the value was calculated, ClickHouse changes it to the new or updated `MATERIALIZED` value expression.

Syntax:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE COLUMN col [IN PARTITION partition | IN PARTITION ID 'partition_id'];
```
- If you specify a PARTITION, a column will be materialized with only the specified partition.

**Example**

```sql
DROP TABLE IF EXISTS tmp;
SET mutations_sync = 2;
CREATE TABLE tmp (x Int64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY tuple();
INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5;
ALTER TABLE tmp ADD COLUMN s String MATERIALIZED toString(x);

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM (select x,s from tmp order by x);

┌─groupArray(x)─┬─groupArray(s)─────────┐
│ [0,1,2,3,4]   │ ['0','1','2','3','4'] │
└───────────────┴───────────────────────┘

ALTER TABLE tmp MODIFY COLUMN s String MATERIALIZED toString(round(100/x));

INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5,5;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)──────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['0','1','2','3','4','20','17','14','12','11'] │
└───────────────────────┴────────────────────────────────────────────────┘

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)─────────────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['inf','100','50','33','25','20','17','14','12','11'] │
└───────────────────────┴───────────────────────────────────────────────────────┘
```

<Note>
`MATERIALIZE COLUMN` always rewrites **column values**. Whether it also rebuilds [data skipping indexes](/reference/statements/alter/skipping-index) (including text indexes) depends on part layout and how the index is stored:

- On parts that are simultaneously **wide + full-storage**, **standalone** skipping-index files (and text indexes) are **not** automatically rebuilt when only the column is rewritten. After changing underlying column data that an index depends on, run [`ALTER TABLE ... MATERIALIZE INDEX`](/reference/statements/alter/skipping-index#materialize-index) if you need those index files refreshed.
- **Exception:** ordinary skip indexes packed into `skp_idx.packed` (small skip-index substreams under the default [`packed_skip_index_max_bytes`](/reference/settings/merge-tree-settings/other#packed_skip_index_max_bytes); full-text indexes are not packed this way) are force-recomputed even on wide + full-storage parts.
- On **any part that is not wide + full-storage** (including **compact + full**, **compact + packed**, and **wide + packed**), the mutation takes a full-part rewrite path and force-recalculates pre-existing secondary indexes on that part. Small parts are commonly compact while still using full part storage by default, so this branch is the common case for recent inserts.

Separately, `ADD INDEX` only updates metadata: it does **not** immediately materialize the new index (including text indexes) on historical parts. Historical parts become materialized either by an explicit [`MATERIALIZE INDEX`](/reference/statements/alter/skipping-index#materialize-index), or by a later merge when [`materialize_skip_indexes_on_merge`](/reference/settings/merge-tree-settings/materialize#materialize_skip_indexes_on_merge) is enabled and the index is not listed in [`exclude_materialize_skip_indexes_on_merge`](/reference/settings/merge-tree-settings/exclude#exclude_materialize_skip_indexes_on_merge). Explicit `MATERIALIZE INDEX` is the deterministic path when you need the index built without waiting on merge settings.
</Note>

**See Also**

- [MATERIALIZED](/reference/statements/create/view#materialized-view).
- [MATERIALIZE INDEX](/reference/statements/alter/skipping-index#materialize-index).

## Limitations {#limitations}

The `ALTER` query lets you create and delete separate elements (columns) in nested data structures, but not whole nested data structures. To add a nested data structure, you can add columns with a name like `name.nested_name` and the type `Array(T)`. A nested data structure is equivalent to multiple array columns with a name that has the same prefix before the dot.

Renaming columns with dots in their names is partially supported. Dots are reserved for [Nested](/reference/data-types/nested-data-structures/index) sub-column access, so the prefix (parent name) must remain the same. Only the suffix (sub-column name) can be changed. For example, `a.b` can be renamed to `a.c`, but renaming `a.b` to `b.d` is not allowed because it changes the Nested parent prefix.

There is no support for deleting columns in the primary key or the sampling key (columns that are used in the `ENGINE` expression). Changing the type for columns that are included in the primary key is only possible if this change does not cause the data to be modified (for example, you are allowed to add values to an Enum or to change a type from `DateTime` to `UInt32`).

If the `ALTER` query is not sufficient to make the table changes you need, you can create a new table, copy the data to it using the [INSERT SELECT](/reference/statements/insert-into#inserting-the-results-of-select) query, then switch the tables using the [RENAME](/reference/statements/rename#rename-table) query and delete the old table.

The `ALTER` query blocks all reads and writes for the table. In other words, if a long `SELECT` is running at the time of the `ALTER` query, the `ALTER` query will wait for it to complete. At the same time, all new queries to the same table will wait while this `ALTER` is running.

For tables that do not store data themselves (such as [Merge](/reference/statements/alter/index) and [Distributed](/reference/statements/alter/index)), `ALTER` just changes the table structure, and does not change the structure of subordinate tables. For example, when running ALTER for a `Distributed` table, you will also need to run `ALTER` for the tables on all remote servers.
)DOCS_MD",
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
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE", "CODEC", "ALTER TABLE ... MODIFY TTL"},
    });

    factory.registerStatement("ALTER TABLE ... PARTITION",
    {
        .description = R"DOCS_MD(
The following operations with [partitions](/reference/engines/table-engines/mergetree-family/custom-partitioning-key) are available:

- [DETACH PARTITION\|PART](#detach-partitionpart) — Moves a partition or part to the `detached` directory and forget it.
- [DROP PARTITION\|PART](#drop-partitionpart) — Deletes a partition or part.
- [DROP DETACHED PARTITION\|PART](#drop-detached-partitionpart) - Delete a part or all parts of a partition from `detached`.
- [FORGET PARTITION](#forget-partition) — Deletes a partition metadata from zookeeper if it's empty.
- [ATTACH PARTITION\|PART](#attach-partitionpart) — Adds a partition or part from the `detached` directory to the table.
- [ATTACH PARTITION FROM](#attach-partition-from) — Copies the data partition from one table to another and adds.
- [REPLACE PARTITION](#replace-partition) — Copies the data partition from one table to another and replaces.
- [MOVE PARTITION TO TABLE](#move-partition-to-table) — Moves the data partition from one table to another.
- [CLEAR COLUMN IN PARTITION](#clear-column-in-partition) — Resets the value of a specified column in a partition.
- [CLEAR INDEX IN PARTITION](#clear-index-in-partition) — Resets the specified secondary index in a partition.
- [FREEZE PARTITION](#freeze-partition) — Creates a backup of a partition.
- [UNFREEZE PARTITION](#unfreeze-partition) — Removes a backup of a partition.
- [FETCH PARTITION\|PART](#fetch-partitionpart) — Downloads a part or partition from another server.
- [MOVE PARTITION\|PART](#move-partitionpart) — Move partition/data part to another disk or volume.
- [UPDATE IN PARTITION](#update-in-partition) — Update data inside the partition by condition.
- [DELETE IN PARTITION](#delete-in-partition) — Delete data inside the partition by condition.
- [REWRITE PARTS](#rewrite-parts) — Rewrite parts in the table (or specific partition) completely.

{/* */}

## DETACH PARTITION\|PART {#detach-partitionpart}

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DETACH PARTITION|PART partition_expr
```

Moves all data for the specified partition to the `detached` directory. The server forgets about the detached data partition as if it does not exist. The server will not know about this data until you make the [ATTACH](#attach-partitionpart) query.

Example:

```sql
ALTER TABLE mt DETACH PARTITION '2020-11-21';
ALTER TABLE mt DETACH PART 'all_2_2_0';
```

Read about setting the partition expression in a section [How to set the partition expression](#how-to-set-partition-expression).

After the query is executed, you can do whatever you want with the data in the `detached` directory — delete it from the file system, or just leave it.

This query is replicated – it moves the data to the `detached` directory on all replicas. Note that you can execute this query only on a leader replica. To find out if a replica is a leader, perform the `SELECT` query to the [system.replicas](/reference/system-tables/replicas) table. Alternatively, it is easier to make a `DETACH` query on all replicas - all the replicas throw an exception, except the leader replicas (as multiple leaders are allowed).

## DROP PARTITION\|PART {#drop-partitionpart}

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP PARTITION|PART partition_expr
```

Deletes the specified partition from the table. This query tags the partition as inactive and deletes data completely, approximately in 10 minutes.

Read about setting the partition expression in a section [How to set the partition expression](#how-to-set-partition-expression).

The query is replicated – it deletes data on all replicas.

Example:

```sql
ALTER TABLE mt DROP PARTITION '2020-11-21';
ALTER TABLE mt DROP PART 'all_4_4_0';
```

## DROP DETACHED PARTITION\|PART {#drop-detached-partitionpart}

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP DETACHED PARTITION|PART ALL|partition_expr
```

Removes the specified part or all parts of the specified partition from `detached`.
Read more about setting the partition expression in a section [How to set the partition expression](#how-to-set-partition-expression).

## FORGET PARTITION {#forget-partition}

```sql
ALTER TABLE table_name FORGET PARTITION partition_expr
```

Removes all metadata about an empty partition from ZooKeeper. Query fails if partition is not empty or unknown. Make sure to execute only for partitions that will never be used again.

Read about setting the partition expression in a section [How to set the partition expression](#how-to-set-partition-expression).

Example:

```sql
ALTER TABLE mt FORGET PARTITION '20201121';
```

## ATTACH PARTITION\|PART {#attach-partitionpart}

```sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

Adds data to the table from the `detached` directory. It is possible to add data for an entire partition or for a separate part. Examples:

```sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

Read more about setting the partition expression in a section [How to set the partition expression](#how-to-set-partition-expression).

This query is replicated. The replica-initiator checks whether there is data in the `detached` directory.
If data exists, the query checks its integrity. If everything is correct, the query adds the data to the table.

If the non-initiator replica, receiving the attach command, finds the part with the correct checksums in its own `detached` folder, it attaches the data without fetching it from other replicas.
If there is no part with the correct checksums, the data is downloaded from any replica having the part.

You can put data to the `detached` directory on one replica and use the `ALTER ... ATTACH` query to add it to the table on all replicas.

## ATTACH PARTITION FROM {#attach-partition-from}

```sql
ALTER TABLE table2 [ON CLUSTER cluster] ATTACH PARTITION partition_expr FROM table1
```

This query copies the data partition from `table1` to `table2`.

Note that:

- Data will be deleted neither from `table1` nor from `table2`.
- `table1` may be a temporary table.

For the query to run successfully, the following conditions must be met:

- Both tables must have the same structure.
- Both tables must have the same partition key, the same order by key and the same primary key.
- Both tables must have the same storage policy.
- If the source part has non-adaptive index granularity, both tables must have the same `index_granularity`: such a part stores no per-mark row counts, so the destination table's value is used to interpret its marks.
- The destination table must include all indices and projections from the source table. If the `enforce_index_structure_match_on_partition_manipulation` setting is enabled in destination table, the indices and projections must be identical. Otherwise, the destination table can have a superset of the source table's indices and projections.

## REPLACE PARTITION {#replace-partition}

```sql
ALTER TABLE table2 [ON CLUSTER cluster] REPLACE PARTITION partition_expr FROM table1
```

This query copies the data partition from `table1` to `table2` and replaces the existing partition in `table2`. The operation is atomic.

Note that:

- Data won't be deleted from `table1`.
- `table1` may be a temporary table.

For the query to run successfully, the following conditions must be met:

- Both tables must have the same structure.
- Both tables must have the same partition key, the same order by key and the same primary key.
- Both tables must have the same storage policy.
- If the source part has non-adaptive index granularity, both tables must have the same `index_granularity`: such a part stores no per-mark row counts, so the destination table's value is used to interpret its marks.
- The destination table must include all indices and projections from the source table. If the `enforce_index_structure_match_on_partition_manipulation` setting is enabled in destination table, the indices and projections must be identical. Otherwise, the destination table can have a superset of the source table's indices and projections.

## MOVE PARTITION TO TABLE {#move-partition-to-table}

```sql
ALTER TABLE table_source [ON CLUSTER cluster] MOVE PARTITION partition_expr TO TABLE table_dest
```

This query moves the data partition from the `table_source` to `table_dest` with deleting the data from `table_source`.

For the query to run successfully, the following conditions must be met:

- Both tables must have the same structure.
- Both tables must have the same partition key, the same order by key and the same primary key.
- Both tables must have the same storage policy.
- Both tables must be the same engine family (replicated or non-replicated).
- If the source part has non-adaptive index granularity, both tables must have the same `index_granularity`: such a part stores no per-mark row counts, so the destination table's value is used to interpret its marks.
- The destination table must include all indices and projections from the source table. If the `enforce_index_structure_match_on_partition_manipulation` setting is enabled in destination table, the indices and projections must be identical. Otherwise, the destination table can have a superset of the source table's indices and projections.

## CLEAR COLUMN IN PARTITION {#clear-column-in-partition}

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR COLUMN column_name IN PARTITION partition_expr
```

Resets all values in the specified column in a partition. If the `DEFAULT` clause was determined when creating a table, this query sets the column value to a specified default value.

Example:

```sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

## FREEZE PARTITION {#freeze-partition}

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FREEZE [PARTITION partition_expr] [WITH NAME 'backup_name']
```

This query creates a local backup of a specified partition. If the `PARTITION` clause is omitted, the query creates the backup of all partitions at once.

<Note>
The entire backup process is performed without stopping the server.
</Note>

Note that for old-styled tables you can specify the prefix of the partition name (for example, `2019`) - then the query creates the backup for all the corresponding partitions. Read about setting the partition expression in a section [How to set the partition expression](#how-to-set-partition-expression).

At the time of execution, for a data snapshot, the query creates hardlinks to a table data. Hardlinks are placed in the directory `/var/lib/clickhouse/shadow/N/...`, where:

- `/var/lib/clickhouse/` is the working ClickHouse directory specified in the config.
- `N` is the incremental number of the backup.
- if the `WITH NAME` parameter is specified, then the value of the `'backup_name'` parameter is used instead of the incremental number.

<Note>
If you use [a set of disks for data storage in a table](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes), the `shadow/N` directory appears on every disk, storing data parts that matched by the `PARTITION` expression.
</Note>

The same structure of directories is created inside the backup as inside `/var/lib/clickhouse/`. The query performs `chmod` for all files, forbidding writing into them.

After creating the backup, you can copy the data from `/var/lib/clickhouse/shadow/` to the remote server and then delete it from the local server. Note that the `ALTER t FREEZE PARTITION` query is not replicated. It creates a local backup only on the local server.

The query creates backup almost instantly (but first it waits for the current queries to the corresponding table to finish running).

`ALTER TABLE t FREEZE PARTITION` copies only the data, not table metadata. To make a backup of table metadata, copy the file `/var/lib/clickhouse/metadata/database/table.sql`

To restore data from a backup, do the following:

1.  Create the table if it does not exist. To view the query, use the .sql file (replace `ATTACH` in it with `CREATE`).
2.  Copy the data from the `data/database/table/` directory inside the backup to the `/var/lib/clickhouse/data/database/table/detached/` directory.
3.  Run `ALTER TABLE t ATTACH PARTITION` queries to add the data to a table.

Restoring from a backup does not require stopping the server.

The query processes parts in parallel, the number of threads is regulated by the `max_threads` setting.

For more information about backups and restoring data, see section ["Backup and Restore in ClickHouse"](/concepts/features/backup-restore/overview) section.

## UNFREEZE PARTITION {#unfreeze-partition}

```sql
ALTER TABLE table_name [ON CLUSTER cluster] UNFREEZE [PARTITION 'part_expr'] WITH NAME 'backup_name'
```

Removes `frozen` partitions with the specified name from the disk. If the `PARTITION` clause is omitted, the query removes the backup of all partitions at once.

## CLEAR INDEX IN PARTITION {#clear-index-in-partition}

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR INDEX index_name IN PARTITION partition_expr
```

The query works similar to `CLEAR COLUMN`, but it resets an index instead of a column data.

## FETCH PARTITION|PART {#fetch-partitionpart}

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FETCH PARTITION|PART partition_expr FROM 'path-in-zookeeper'
```

Downloads a partition from another server. This query only works for the replicated tables.

The query does the following:

1.  Downloads the partition|part from the specified shard. In 'path-in-zookeeper' you must specify a path to the shard in ZooKeeper.
2.  Then the query puts the downloaded data to the `detached` directory of the `table_name` table. Use the [ATTACH PARTITION\|PART](#attach-partitionpart) query to add the data to the table.

For example:

1. FETCH PARTITION
```sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```
2. FETCH PART
```sql
ALTER TABLE users FETCH PART 201901_2_2_0 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PART 201901_2_2_0;
```

Note that:

- The `ALTER ... FETCH PARTITION|PART` query isn't replicated. It places the part or partition to the `detached` directory only on the local server.
- The `ALTER TABLE ... ATTACH` query is replicated. It adds the data to all replicas. The data is added to one of the replicas from the `detached` directory, and to the others - from neighboring replicas.

Before downloading, the system checks if the partition exists and the table structure matches. The most appropriate replica is selected automatically from the healthy replicas.

Although the query is called `ALTER TABLE`, it does not change the table structure and does not immediately change the data available in the table.

## MOVE PARTITION\|PART {#move-partitionpart}

Moves partitions or data parts to another volume or disk for `MergeTree`-engine tables. See [Using Multiple Block Devices for Data Storage](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes).

```sql
ALTER TABLE table_name [ON CLUSTER cluster] MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

The `ALTER TABLE t MOVE` query:

- Not replicated, because different replicas can have different storage policies.
- Returns an error if the specified disk or volume is not configured. Query also returns an error if conditions of data moving, that specified in the storage policy, can't be applied.
- Can return an error in the case, when data to be moved is already moved by a background process, concurrent `ALTER TABLE t MOVE` query or as a result of background data merging. A user shouldn't perform any additional actions in this case.

Example:

```sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

## UPDATE IN PARTITION {#update-in-partition}

Manipulates data in the specifies partition matching the specified filtering expression. Implemented as a [mutation](/reference/statements/alter/index#mutations).

Syntax:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr
```

### Example {#example}

```sql
-- using partition name
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION ID '2' WHERE p = 2;
```

### See Also {#see-also}

- [UPDATE](/reference/statements/alter/partition#update-in-partition)

## DELETE IN PARTITION {#delete-in-partition}

Deletes data in the specifies partition matching the specified filtering expression. Implemented as a [mutation](/reference/statements/alter/index#mutations).

Syntax:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE [IN PARTITION partition_expr] WHERE filter_expr
```

### Example {#example-1}

```sql
-- using partition name
ALTER TABLE mt DELETE IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt DELETE IN PARTITION ID '2' WHERE p = 2;
```

## REWRITE PARTS {#rewrite-parts}

This will rewrite the parts from scratch, using all new settings. This makes sense because table-level settings like `use_const_adaptive_granularity` are applied only for newly written parts by default.

### Example {#example-rewrite-parts}

```sql
ALTER TABLE mt REWRITE PARTS;
ALTER TABLE mt REWRITE PARTS IN PARTITION 2;
```

### See Also {#see-also-1}

- [DELETE](/reference/statements/alter/delete)

## How to Set Partition Expression {#how-to-set-partition-expression}

You can specify the partition expression in `ALTER ... PARTITION` queries in different ways:

- As a value from the `partition` column of the `system.parts` table. For example, `ALTER TABLE visits DETACH PARTITION 201901`.
- Using the keyword `ALL`. It can be used only with DROP/DETACH/ATTACH/ATTACH FROM. For example, `ALTER TABLE visits ATTACH PARTITION ALL`.
- As a tuple of expressions or constants that matches (in types) the table partitioning keys tuple. In the case of a single element partitioning key, the expression should be wrapped in the `tuple (...)` function. For example, `ALTER TABLE visits DETACH PARTITION tuple(toYYYYMM(toDate('2019-01-25')))`.
- Using the partition ID. Partition ID is a string identifier of the partition (human-readable, if possible) that is used as the names of partitions in the file system and in ZooKeeper. The partition ID must be specified in the `PARTITION ID` clause, in a single quotes. For example, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
- In the [ALTER ATTACH PART](#attach-partitionpart) and [DROP DETACHED PART](#drop-detached-partitionpart) query, to specify the name of a part, use string literal with a value from the `name` column of the [system.detached_parts](/reference/system-tables/detached_parts) table. For example, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

Usage of quotes when specifying the partition depends on the type of partition expression. For example, for the `String` type, you have to specify its name in quotes (`'`). For the `Date` and `Int*` types no quotes are needed.

All the rules above are also true for the [OPTIMIZE](/reference/statements/optimize) query. If you need to specify the only partition when optimizing a non-partitioned table, set the expression `PARTITION tuple()`. For example:

```sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

`IN PARTITION` specifies the partition to which the [UPDATE](/reference/statements/alter/update) or [DELETE](/reference/statements/alter/delete) expressions are applied as a result of the `ALTER TABLE` query. New parts are created only from the specified partition. In this way, `IN PARTITION` helps to reduce the load when the table is divided into many partitions, and you only need to update the data point-by-point.

The examples of `ALTER ... PARTITION` queries are demonstrated in the tests [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) and [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).
)DOCS_MD",
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
        .parent = "ALTER",
        .related = {"ALTER", "SYSTEM", "OPTIMIZE", "TRUNCATE"},
    });

    factory.registerStatement("ALTER TABLE ... DELETE",
    {
        .description = R"DOCS_MD(
```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE [IN PARTITION partition_expr1 [, partition_expr2 ...]] WHERE filter_expr
```

Deletes data matching the specified filtering expression. Implemented as a [mutation](/reference/statements/alter/index#mutations).

<Warning>
The `ALTER TABLE` prefix signals that this is a heavyweight operation, unlike similar queries in OLTP databases. `ALTER TABLE ... DELETE` rewrites every data part containing rows that match the filtering expression, generating substantial write I/O. Use it sparingly, as it can affect `SELECT` performance.

For `MergeTree` tables, consider using the [`DELETE FROM` query](/reference/statements/delete), which performs a lightweight delete and can be considerably faster. For workloads that require frequent deletions or corrections, consider using [`ReplacingMergeTree`](/concepts/features/operations/update/replacing-merge-tree) or [`CollapsingMergeTree`](/reference/engines/table-engines/mergetree-family/collapsingmergetree).
</Warning>

The `filter_expr` must be of type `UInt8`. The query deletes rows in the table for which this expression takes a non-zero value.

One query can contain several commands separated by commas.

The `IN PARTITION` clause limits the mutation to the listed partitions. Without it, on tables of the `ReplicatedMergeTree` family, when the [optimize_mutations_with_partition_pruning](/reference/settings/session-settings/optimize) setting is enabled (the default), ClickHouse automatically detects partition key conditions in `filter_expr` and only mutates the affected partitions. On non-replicated `MergeTree` tables, use an explicit `IN PARTITION` clause to limit the mutation to specific partitions.

The synchronicity of the query processing is defined by the [mutations_sync](/reference/settings/session-settings/mutations#mutations_sync) setting. By default, it is asynchronous.

**See also**

- [Mutations](/reference/statements/alter/index#mutations)
- [Synchronicity of ALTER Queries](/reference/statements/alter/index#synchronicity-of-alter-queries)
- [mutations_sync](/reference/settings/session-settings/mutations#mutations_sync) setting

## Related content {#related-content}

- Blog: [Handling Updates and Deletes in ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
)DOCS_MD",
        .syntax = R"(
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE [IN PARTITION partition_expr1 [, partition_expr2 ...]] WHERE filter_expr
)",
        .parent = "ALTER",
        .related = {"ALTER", "DELETE", "TRUNCATE", "ALTER TABLE ... UPDATE"},
    });

    factory.registerStatement("ALTER TABLE ... UPDATE",
    {
        .description = R"DOCS_MD(
```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_expr1 [, partition_expr2 ...]] WHERE filter_expr
```

Manipulates data matching the specified filtering expression. Implemented as a [mutation](/reference/statements/alter/index#mutations).

<Note>
The `ALTER TABLE` prefix makes this syntax different from most other systems supporting SQL. It is intended to signify that unlike similar queries in OLTP databases this is a heavy operation not designed for frequent use.
</Note>

The `filter_expr` must be of type `UInt8`. This query updates values of specified columns to the values of corresponding expressions in rows for which the `filter_expr` takes a non-zero value. Values are cast to the column type using the `CAST` operator. Updating columns that are used in the calculation of the primary or the partition key is not supported.

One query can contain several commands separated by commas.

The `IN PARTITION` clause limits the mutation to the listed partitions. Without it, on tables of the `ReplicatedMergeTree` family, when the [optimize_mutations_with_partition_pruning](/reference/settings/session-settings/optimize) setting is enabled (the default), ClickHouse automatically detects partition key conditions in `filter_expr` and only mutates the affected partitions. On non-replicated `MergeTree` tables, use an explicit `IN PARTITION` clause to limit the mutation to specific partitions.

The synchronicity of the query processing is defined by the [mutations_sync](/reference/settings/session-settings/mutations#mutations_sync) setting. By default, it is asynchronous.

**See also**

- [Mutations](/reference/statements/alter/index#mutations)
- [Synchronicity of ALTER Queries](/reference/statements/alter/index#synchronicity-of-alter-queries)
- [mutations_sync](/reference/settings/session-settings/mutations#mutations_sync) setting
- [Lightweight `UPDATE`](/reference/statements/update) - Alternative lightweight update using patch parts
- [`APPLY PATCHES`](/reference/statements/alter/apply-patches) - Manually apply patches from lightweight updates

## Materialized columns {#materialized-columns}

A [`MATERIALIZED`](/reference/statements/create/table#materialized) column whose expression reads an
updated column is recalculated by the mutation, so its stored value stays consistent with the new data.

### Columns calculated from EPHEMERAL columns {#columns-calculated-from-ephemeral-columns}

An [`EPHEMERAL`](/reference/statements/create/table#ephemeral) column exists only for the duration of an
`INSERT` and is never stored, so a `MATERIALIZED` column calculated from one cannot be recalculated by a
mutation. Such a column keeps the value computed at `INSERT` time, which then no longer matches its
expression:

```sql
CREATE TABLE test
(
    x Int32,
    e Int32 EPHEMERAL 0,
    m Int32 MATERIALIZED x + e
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO test (x, e) VALUES (1, 7);

ALTER TABLE test UPDATE x = 2 WHERE 1;

SELECT x, m FROM test;
```

```text
┌─x─┬─m─┐
│ 2 │ 8 │
└───┴───┘
```

`m` is `8`, the value calculated during `INSERT`, and not `2 + 7`: the value of `e` is not available
outside the `INSERT` that supplied it. The mutation writes a warning to the server log when it skips a
column for this reason. To bring such a column up to date, re-`INSERT` the affected rows.

## Related content {#related-content}

- Blog: [Handling Updates and Deletes in ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
)DOCS_MD",
        .syntax = R"(
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_expr1 [, partition_expr2 ...]] WHERE filter_expr
)",
        .parent = "ALTER",
        .related = {"ALTER", "UPDATE", "ALTER TABLE ... DELETE", "ALTER TABLE ... APPLY PATCHES"},
    });

    factory.registerStatement("ALTER TABLE ... MODIFY ORDER BY",
    {
        .description = R"DOCS_MD(
```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY ORDER BY new_expression
```

The command changes the [sorting key](/reference/engines/table-engines/mergetree-family/mergetree) of the table to `new_expression` (an expression or a tuple of expressions). Primary key remains the same.

The command is lightweight in a sense that it only changes metadata. To keep the property that data part rows are ordered by the sorting key expression you cannot add expressions containing existing columns to the sorting key (only columns added by the `ADD COLUMN` command in the same `ALTER` query, without default column value).

<Note>
It only works for tables in the [`MergeTree`](/reference/engines/table-engines/mergetree-family/mergetree) family (including [replicated](/reference/engines/table-engines/mergetree-family/replication) tables).
</Note>
)DOCS_MD",
        .syntax = R"(
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY ORDER BY new_expression
)",
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE", "ALTER TABLE ... MODIFY SAMPLE BY"},
    });

    factory.registerStatement("ALTER TABLE ... MODIFY SAMPLE BY",
    {
        .description = R"DOCS_MD(
The following operations are available:

## MODIFY {#modify}

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SAMPLE BY new_expression
```

The command changes the [sampling key](/reference/engines/table-engines/mergetree-family/mergetree) of the table to `new_expression` (an expression or a tuple of expressions). The primary key must contain the new sample key.

## REMOVE {#remove}

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] REMOVE SAMPLE BY
```

The command removes the [sampling key](/reference/engines/table-engines/mergetree-family/mergetree) of the table.

The commands `MODIFY` and `REMOVE` are lightweight in the sense that they only change metadata or remove files.

<Note>
It only works for tables in the [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) family (including [replicated](/reference/engines/table-engines/mergetree-family/replication) tables).
</Note>
)DOCS_MD",
        .syntax = R"(
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SAMPLE BY new_expression
ALTER TABLE [db].name [ON CLUSTER cluster] REMOVE SAMPLE BY
)",
        .parent = "ALTER",
        .related = {"ALTER", "SAMPLE", "ALTER TABLE ... MODIFY ORDER BY"},
    });

    factory.registerStatement("ALTER TABLE ... MODIFY TTL",
    {
        .description = R"DOCS_MD(
<Note>
If you are looking for details on using TTL for managing old data, check out the [Manage Data with TTL](/concepts/features/operations/delete/ttl) user guide. The docs below demonstrate how to alter or remove an existing TTL rule.
</Note>

## MODIFY TTL {#modify-ttl}

You can change [table TTL](/reference/engines/table-engines/mergetree-family/mergetree#mergetree-table-ttl) with a request of the following form:

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] MODIFY TTL ttl_expression;
```

## REMOVE TTL {#remove-ttl}

TTL-property can be removed from table with the following query:

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] REMOVE TTL
```

**Example**

Consider the table with table `TTL`:

```sql
CREATE TABLE table_with_ttl
(
    event_time DateTime,
    UserID UInt64,
    Comment String
)
ENGINE MergeTree()
ORDER BY tuple()
TTL event_time + INTERVAL 3 MONTH
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO table_with_ttl VALUES (now(), 1, 'username1');

INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
```

Run `OPTIMIZE` to force `TTL` cleanup:

```sql
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```
Second row was deleted from table.

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
└───────────────────────┴─────────┴──────────────┘
```

Now remove table `TTL` with the following query:

```sql
ALTER TABLE table_with_ttl REMOVE TTL;
```

Re-insert the deleted row and force the `TTL` cleanup again with `OPTIMIZE`:

```sql
INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

The `TTL` is no longer there, so the second row is not deleted:

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
│   2020-08-11 12:44:57 │       2 │    username2 │
└───────────────────────┴─────────┴──────────────┘
```

**See Also**

- More about the [TTL-expression](/reference/statements/create/table#ttl-expression).
- Modify column [with TTL](/reference/statements/alter/ttl).
)DOCS_MD",
        .syntax = R"(
ALTER TABLE [db.]table_name [ON CLUSTER cluster] MODIFY TTL ttl_expression
ALTER TABLE [db.]table_name [ON CLUSTER cluster] REMOVE TTL
)",
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE", "ALTER TABLE ... COLUMN", "OPTIMIZE"},
    });

    factory.registerStatement("ALTER TABLE ... MODIFY SETTING",
    {
        .description = R"DOCS_MD(
There is a set of queries to change table settings. You can modify settings or reset them to default values. A single query can change several settings at once.
If a setting with the specified name does not exist, then the query raises an exception.

**Syntax**

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY|RESET SETTING ...
```

<Note>
These queries can be applied to [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) tables only.
</Note>

## MODIFY SETTING {#modify-setting}

Changes table settings.

**Syntax**

```sql
MODIFY SETTING setting_name=value [, ...]
```

**Example**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id;

ALTER TABLE example_table MODIFY SETTING max_part_loading_threads=8, max_parts_in_total=50000;
```

## RESET SETTING {#reset-setting}

Resets table settings to their default values. If a setting is in a default state, then no action is taken.

**Syntax**

```sql
RESET SETTING setting_name [, ...]
```

**Example**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id
    SETTINGS max_part_loading_threads=8;

ALTER TABLE example_table RESET SETTING max_part_loading_threads;
```

**See Also**

- [MergeTree settings](/reference/settings/merge-tree-settings)
)DOCS_MD",
        .syntax = R"(
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SETTING setting_name = value [, ...]
ALTER TABLE [db].name [ON CLUSTER cluster] RESET SETTING setting_name [, ...]
)",
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE", "SET"},
    });

    factory.registerStatement("ALTER TABLE ... CONSTRAINT",
    {
        .description = R"DOCS_MD(
Constraints could be added, modified or deleted using following syntax:

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY CONSTRAINT [IF EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name;
```

As with table creation, a constraint can be declared either as `CHECK` (enforced on `INSERT`) or as `ASSUME` (trusted by the optimizer without being checked). See [constraints](/reference/statements/create/table#constraints) for the difference between the two.

`MODIFY CONSTRAINT` replaces the declaration of an existing constraint, keeping its position in the table definition. It can also change the constraint kind (for example, from `CHECK` to `ASSUME`). It is equivalent to dropping the constraint and adding it again with the new declaration. If the constraint does not exist, the query throws an error, unless `IF EXISTS` is specified.

See more on [constraints](/reference/statements/create/table#constraints).

Queries will add, change or remove metadata about constraints from table, so they are processed immediately.

<Tip>
Constraint check **will not be executed** on existing data if it was added or modified.
</Tip>

All changes on replicated tables are broadcast to ZooKeeper and will be applied on other replicas as well.
)DOCS_MD",
        .syntax = R"(
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name {CHECK|ASSUME} expression
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY CONSTRAINT [IF EXISTS] constraint_name {CHECK|ASSUME} expression
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name
)",
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE"},
    });

    factory.registerStatement("ALTER TABLE ... INDEX",
    {
        .description = R"DOCS_MD(
The following operations are available:

## ADD INDEX {#add-index}

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] ADD INDEX [IF NOT EXISTS] name expression TYPE type [GRANULARITY value] [FIRST|AFTER name]` - Adds index description to tables metadata.

## DROP INDEX {#drop-index}

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] DROP INDEX [IF EXISTS] name` - Removes index description from tables metadata and deletes index files from disk. Implemented as a [mutation](/reference/statements/alter/index#mutations).

## MATERIALIZE INDEX {#materialize-index}

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] MATERIALIZE INDEX [IF EXISTS] name [IN PARTITION partition_name]` - Rebuilds the secondary index `name` for the specified `partition_name`. Implemented as a [mutation](/reference/statements/alter/index#mutations). If `IN PARTITION` part is omitted then it rebuilds the index for the whole table data.

[`MATERIALIZE COLUMN`](/reference/statements/alter/column#materialize-column) is not a full substitute for `MATERIALIZE INDEX`. On parts that are simultaneously **wide + full-storage**, it may rewrite column values without refreshing **standalone** skipping-index (or text-index) files. Ordinary skip indexes stored in `skp_idx.packed` are an exception: they can still be force-recomputed on wide + full-storage parts (small skip-index substreams under the default [`packed_skip_index_max_bytes`](/reference/settings/merge-tree-settings/other#packed_skip_index_max_bytes); full-text indexes are not packed this way). On **any part that is not wide + full-storage** (including **compact + full**, **compact + packed**, and **wide + packed**), a full-part rewrite can recalculate pre-existing indexes — small parts are commonly compact while still using full part storage by default. Use `MATERIALIZE INDEX` for the **deterministic / immediate** path when an index was added to a table that already has data (metadata-only `ADD INDEX`), and after column rewrites on wide+full-storage parts when you need standalone index files rebuilt right away. Newly added indexes (including text indexes) on historical parts can also be materialized by a later merge when [`materialize_skip_indexes_on_merge`](/reference/settings/merge-tree-settings/materialize#materialize_skip_indexes_on_merge) is enabled and the index is not excluded via [`exclude_materialize_skip_indexes_on_merge`](/reference/settings/merge-tree-settings/exclude#exclude_materialize_skip_indexes_on_merge); otherwise they stay unmaterialized until an explicit `MATERIALIZE INDEX`.

## CLEAR INDEX {#clear-index}

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] CLEAR INDEX [IF EXISTS] name [IN PARTITION partition_name]` - Deletes the secondary index files from disk without removing description. Implemented as a [mutation](/reference/statements/alter/index#mutations).

The commands `ADD`, `DROP`, and `CLEAR` are lightweight in the sense that they only change metadata or remove files.
Also, they are replicated, syncing indices metadata via ClickHouse Keeper or ZooKeeper.

<Note>
Index manipulation is supported only for tables with [`*MergeTree`](/reference/engines/table-engines/mergetree-family/mergetree) engine (including [replicated](/reference/engines/table-engines/mergetree-family/replication) variants).
</Note>


## Concurrent `ALTER` and multi-clause `MATERIALIZE INDEX` {#concurrent-alter-and-multi-clause-materialize-index}

On replicated tables, rapid separate `ALTER`s against one table can raise `CANNOT_ASSIGN_ALTER` (code 517) when previous `ALTER`s have not yet been applied on the replica (metadata still behind — can remain true after an earlier alter was already assigned). This is a general concurrent metadata-`ALTER` / mutation condition (not mutation-only); serialize/retry, wait for prior mutation-producing alters via [`mutations_sync`](/reference/settings/session-settings/mutations#mutations_sync) / `is_done` in [`system.mutations`](/reference/system-tables/mutations), or combine independent metadata operations into one multi-clause `ALTER` when the grammar allows it. See [Synchronicity of ALTER Queries](/reference/statements/alter/index#synchronicity-of-alter-queries) and [Concurrent ALTER assignment](/reference/statements/alter/index#concurrent-alter-assignment-on-one-table).

Multiple `MATERIALIZE INDEX` clauses can appear in one `ALTER`. The covered case in-tree is packing several `ADD INDEX` clauses together with `MATERIALIZE INDEX` for those same new indexes in a single statement (`tests/queries/0_stateless/02911_add_index_and_materialize_index.sql`). That packed form is for ordinary (non-`DatabaseReplicated`) databases — `DatabaseReplicated` rejects mixed `ADD INDEX` + `MATERIALIZE INDEX` segments with `QUERY_IS_PROHIBITED`. Materialize-only multi-clause forms on already-existing indexes follow the same metadata-snapshot prepare path in the current implementation, but that exact shape is not yet covered by a focused stateless test—treat it as current implementation behavior rather than a separately guaranteed contract until such coverage exists. For ordered apply, issue one `MATERIALIZE INDEX` per statement and wait with [`mutations_sync`](/reference/settings/session-settings/mutations#mutations_sync).
)DOCS_MD",
        .syntax = R"(
ALTER TABLE [db.]table_name [ON CLUSTER cluster] ADD INDEX [IF NOT EXISTS] name expression TYPE type [GRANULARITY value] [FIRST|AFTER name]
ALTER TABLE [db.]table_name [ON CLUSTER cluster] DROP INDEX [IF EXISTS] name
ALTER TABLE [db.]table_name [ON CLUSTER cluster] MATERIALIZE INDEX [IF EXISTS] name [IN PARTITION partition_name]
ALTER TABLE [db.]table_name [ON CLUSTER cluster] CLEAR INDEX [IF EXISTS] name [IN PARTITION partition_name]
)",
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE", "HYPOTHETICAL INDEX", "ALTER TABLE ... PROJECTION"},
    });

    factory.registerStatement("ALTER TABLE ... PROJECTION",
    {
        .description = R"DOCS_MD(
This page discusses what projections are, how you can use them and various options for manipulating projections.

## Overview of projections {#overview}

Projections store data in a format that optimizes query execution, this feature is useful for:
- Running queries on a column that is not a part of the primary key
- Pre-aggregating columns, it will reduce both computation and IO

You can define one or more projections for a table, and during the query analysis the projection with the least data to scan will be selected by ClickHouse without modifying the query provided by the user.

<Info>
**Disk usage**

Projections will create internally a new hidden table, this means that more IO and space on disk will be required.
For example, if the projection has defined a different primary key, all the data from the original table will be duplicated.
</Info>

You can see more technical details about how projections work internally on this [page](/guides/clickhouse/data-modelling/sparse-primary-indexes#option-3-projections).

## Using projections {#examples}

### Example filtering without using primary keys {#example-filtering-without-using-primary-keys}

Creating the table:

```sql
CREATE TABLE visits_order
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String
)
ENGINE = MergeTree()
PRIMARY KEY user_agent
```

Using `ALTER TABLE`, we could add the Projection to an existing table:

```sql
ALTER TABLE visits_order ADD PROJECTION user_name_projection (
    SELECT *
    ORDER BY user_name
)

ALTER TABLE visits_order MATERIALIZE PROJECTION user_name_projection
```

Inserting the data:

```sql
INSERT INTO visits_order SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

The Projection will allow us to filter by `user_name` fast even if in the original Table `user_name` was not defined as a `PRIMARY_KEY`.
At query time, ClickHouse determines that less data will be processed if the projection is used, as the data is ordered by `user_name`.

```sql
SELECT
    *
FROM visits_order
WHERE user_name='test'
LIMIT 2
```

To verify that a query is using the projection, we could review the `system.query_log` table. On the `projections` field we have the name of the projection used or empty if none has been used:

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

### Example pre-aggregation query {#example-pre-aggregation-query}

Create the table with projection `projection_visits_by_user`:

```sql
CREATE TABLE visits
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String,
   PROJECTION projection_visits_by_user
   (
       SELECT
           user_agent,
           sum(pages_visited)
       GROUP BY user_id, user_agent
   )
)
ENGINE = MergeTree()
ORDER BY user_agent
```

Insert the data:

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1. * (number / 2),
   'IOS'
FROM numbers(100, 500);
```

Execute a first query with `GROUP BY` using the field `user_agent`.
This query will not use the projection defined as the pre-aggregation does not match.

```sql
SELECT
    user_agent,
    count(DISTINCT user_id)
FROM visits
GROUP BY user_agent
```

To make use of the projection you can execute queries that select part of, or all of the pre-aggregation and `GROUP BY` fields:

```sql
SELECT
    user_agent
FROM visits
WHERE user_id > 50 AND user_id < 150
GROUP BY user_agent
```

```sql
SELECT
    user_agent,
    sum(pages_visited)
FROM visits
GROUP BY user_agent
```

As previously mentioned, you can review the `system.query_log` table to understand if a projection was used.
The `projections` field shows the name of the projection used.
It will be empty if no projection has been used:

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

### Creating and using projection indexes {#projection-indexes}

Creating a [projection index](/reference/engines/table-engines/mergetree-family/mergetree#projection-index):

```sql
CREATE TABLE events
(
    `event_time` DateTime,
    `event_id` UInt64,
    `user_id` UInt64,
    `huge_string` String,
    PROJECTION order_by_user_id INDEX user_id TYPE basic
)
ENGINE = MergeTree()
ORDER BY (event_id);
```

<details markdown="1">

<summary>Creating a projection with explicit `_part_offset` field</summary>

Projection indexes can alternatively be created using the following syntax (not recommended):

```sql
CREATE TABLE events
(
    `event_time` DateTime,
    `event_id` UInt64,
    `user_id` UInt64,
    `huge_string` String,
    PROJECTION order_by_user_id
    (
        SELECT
            _part_offset
        ORDER BY user_id
    )
)
ENGINE = MergeTree()
ORDER BY (event_id);
```

</details>

Inserting some sample data:

```sql
INSERT INTO events SELECT * FROM generateRandom() LIMIT 100000;
```

The `_part_offset` field preserves its value through merges and mutations, making it valuable for secondary indexing. We can leverage this in queries:

```sql
SELECT
    count()
FROM events
WHERE _part_starting_offset + _part_offset IN (
    SELECT _part_starting_offset + _part_offset
    FROM events
    WHERE user_id = 42
)
SETTINGS enable_shared_storage_snapshot_in_query = 1
```

### Example projection with WHERE clause {#example-projection-with-where}

Projections can include a `WHERE` clause to store only a subset of rows. This is useful when queries frequently filter on a known predicate — the projection materializes only the matching rows, reducing storage and improving query performance.

Creating a table and adding a filtered projection:

```sql
CREATE TABLE events
(
    `event_type` String,
    `time` DateTime,
    `message` String
)
ENGINE = MergeTree()
ORDER BY time;

ALTER TABLE events ADD PROJECTION proj_pageview (
    SELECT event_type, time, message
    WHERE event_type = 'pageview'
    ORDER BY time
);

ALTER TABLE events MATERIALIZE PROJECTION proj_pageview;
```

Inserting data:

```sql
INSERT INTO events VALUES
    ('pageview', '2024-01-01', 'homepage'),
    ('click', '2024-01-02', 'button'),
    ('pageview', '2024-01-03', 'about');
```

When a query's `WHERE` clause **implies** the projection's `WHERE` clause (i.e., every condition in the projection's filter is also present in the query's filter), the optimizer can automatically use the projection when it determines this is beneficial:

```sql
-- This query implies the projection's WHERE, so the projection may be used:
SELECT time, message FROM events WHERE event_type = 'pageview';

-- A stricter query also implies the projection's WHERE:
SELECT time, message FROM events WHERE event_type = 'pageview' AND time > '2024-01-01';

-- This query does NOT imply the projection, so the base table is scanned:
SELECT time, message FROM events WHERE event_type = 'click';
```

The implication check is conservative — it uses exact conjunct matching on the canonical expression form. It may miss some valid optimization opportunities (e.g., range implications), but it will never produce incorrect results.

## Manipulating projections {#manipulating-projections}

The following operations with [projections](/reference/engines/table-engines/mergetree-family/mergetree#projections) are available:

### ADD PROJECTION {#add-projection}

Use the statement below to add a projection description to a tables metadata:

```sql
-- Normal projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [ORDER BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]

-- Aggregate projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [GROUP BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]
```

<Note>
When a projection defines a `WHERE` clause, only rows matching the predicate are materialized. The optimizer can use such a projection when the query's `WHERE` logically implies the projection's `WHERE` and the projection is beneficial for the query plan. This applies to both normal and aggregate projections.
</Note>

#### `WITH SETTINGS` Clause {#with-settings}

`WITH SETTINGS` defines **projection-level settings**, which customize how the projection stores data (for example, `index_granularity` or `index_granularity_bytes`).
These correspond directly to **MergeTree table settings**, but apply **only to this projection**.

Example:

```sql
ALTER TABLE t
ADD PROJECTION p (
    SELECT x ORDER BY x
) WITH SETTINGS (
    index_granularity = 4096,
    index_granularity_bytes = 1048576
);
```

Projection settings override the effective table settings for the projection, subject to validation rules (e.g., invalid or incompatible overrides will be rejected).

### MODIFY PROJECTION {#modify-projection}

Use the statement below to change the [`WITH SETTINGS`](#with-settings) clause of an existing projection without rebuilding its data:

```sql
ALTER TABLE [db.]name [ON CLUSTER cluster] MODIFY PROJECTION [IF EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [GROUP BY] [ORDER BY] ) WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)
```

For a [projection index](/reference/engines/table-engines/mergetree-family/mergetree#projection-index), restate the `INDEX` declaration instead of the `SELECT` query:

```sql
ALTER TABLE [db.]name [ON CLUSTER cluster] MODIFY PROJECTION [IF EXISTS] name INDEX <index_expr> TYPE <index_type> WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)
```

The statement restates the full projection definition, but only the `WITH SETTINGS` clause may differ from the existing definition.
The projection query itself (or, for a projection index, the index expression and type) must stay the same, because existing projection parts store data built from it; to change it, use [`DROP PROJECTION`](#drop-projection) followed by [`ADD PROJECTION`](#add-projection).

The command only changes the table metadata and does not rewrite any data: existing projection parts keep the settings they were written with, while projection parts written by future inserts and merges use the new settings.
To rebuild existing parts with the new settings, run [`MATERIALIZE PROJECTION`](#materialize-projection).

Example:

```sql
ALTER TABLE t
MODIFY PROJECTION p (
    SELECT x ORDER BY x
) WITH SETTINGS (
    index_granularity = 128
);
```

### DROP PROJECTION {#drop-projection}

Use the statement below to remove a projection description from a tables metadata and delete projection files from disk.
This is implemented as a [mutation](/reference/statements/alter/index#mutations).

```sql
ALTER TABLE [db.]name [ON CLUSTER cluster] DROP PROJECTION [IF EXISTS] name
```

### MATERIALIZE PROJECTION {#materialize-projection}

Use the statement below to rebuild the projection `name` in partition `partition_name`.
This is implemented as a [mutation](/reference/statements/alter/index#mutations).

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

### CLEAR PROJECTION {#clear-projection}

Use the statement below to delete projection files from disk without removing description.
This is implemented as a [mutation](/reference/statements/alter/index#mutations).

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] CLEAR PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

The commands `ADD`, `MODIFY`, `DROP` and `CLEAR` are lightweight in the sense that they only change metadata or remove files.
Additionally, they are replicated, and sync projection metadata via ClickHouse Keeper or ZooKeeper.

<Note>
Projection manipulation is supported only for tables with [`*MergeTree`](/reference/engines/table-engines/mergetree-family/mergetree) engine (including [replicated](/reference/engines/table-engines/mergetree-family/replication) variants).
</Note>

### Controlling projection merge behavior {#control-projections-merges}

When you execute a query, ClickHouse chooses between reading from the original table or one of its projections.
The decision to read from the original table or one of its projections is made individually per every table part.
ClickHouse generally aims to read as little data as possible and employs a couple of tricks to identify the best part to read from, for example, sampling the primary key of a part.
In some cases, source table parts have no corresponding projection parts.
This can happen, for example, because creating a projection for a table in SQL is “lazy” by default - it only affects newly inserted data but keeps existing parts unaltered.

As one of the projections already contains the pre-computed aggregate values, ClickHouse tries to read from the corresponding projection parts to avoid aggregating at query runtime again. If a specific part lacks the corresponding projection part, query execution falls back to the original part.

But what happens if the rows in the original table change in a non-trivial way by non-trivial data part background merges?
For example, assume the table is stored using the `ReplacingMergeTree` table engine.
If the same row is detected in multiple input parts during merge, only the most recent row version (from the most recently inserted part) will be kept, while all older versions will be discarded.

Similarly, if the table is stored using the `AggregatingMergeTree` table engine, the merge operation may fold the same rows in the input parts (based on the primary key values) into a single row to update partial aggregation states.

Before ClickHouse v24.8, projection parts either silently got out of sync with the main data, or certain operations like updates and deletes could not be run at all as the database automatically threw an exception if the table had projections.

Since v24.8, a new table-level setting [`deduplicate_merge_projection_mode`](/reference/settings/merge-tree-settings/other#deduplicate_merge_projection_mode) controls the behavior if the aforementioned non-trivial background merge operations occur in parts of the original table.

Delete mutations are another example of part merge operations that drop rows in the parts of the original table. Since v24.7, we also have a setting to control the behavior w.r.t. delete mutations triggered by lightweight deletes: [`lightweight_mutation_projection_mode`](/reference/settings/merge-tree-settings/other#lightweight_mutation_projection_mode).

Below are the possible values for both `deduplicate_merge_projection_mode` and `lightweight_mutation_projection_mode`:

- `throw` (default): An exception is thrown, preventing projection parts from going out of sync.
- `drop`: Affected projection table parts are dropped. Queries will fall back to the original table part for affected projection parts.
- `rebuild`: The affected projection part is rebuilt to stay consistent with data in the original table part.

## Limitations {#limitations}

It is not possible to use an `ALIAS` column in a projection's `ORDER BY` clause. For example:

```sql highlight={6}
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 ALIAS a + 1,
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;
-- Fails with UNKNOWN_IDENTIFIER
```

`ALIAS` columns are not physically stored and are computed on-the-fly at query time, so they are unavailable during the projection part write path when the sorting expression is evaluated.

Instead, use `MATERIALIZED` columns or inline the expression directly:

```sql
-- using MATERIALIZED column
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 MATERIALIZED a + 1,
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;

-- using an inline expression
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    PROJECTION p (SELECT a ORDER BY a + 1)
)
ENGINE = MergeTree ORDER BY id;
```

## See also {#see-also}
- ["Control Of Projections During Merges" (blog post)](https://clickhouse.com/blog/clickhouse-release-24-08#control-of-projections-during-merges)
- ["Projections" (guide)](/concepts/features/projections/projections#using-projections-to-speed-up-UK-price-paid)
- ["Materialized Views versus Projections"](/concepts/features/projections/materialized-views-versus-projections)
)DOCS_MD",
        .syntax = R"(
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name (SELECT <COLUMN LIST EXPR> [WHERE <expr>] [ORDER BY] | [GROUP BY]) [WITH SETTINGS (setting_name = setting_value, ...)]
ALTER TABLE [db.]name [ON CLUSTER cluster] DROP PROJECTION [IF EXISTS] name
ALTER TABLE [db.]name [ON CLUSTER cluster] MATERIALIZE PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
ALTER TABLE [db.]name [ON CLUSTER cluster] CLEAR PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
)",
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE", "ALTER TABLE ... INDEX"},
    });

    factory.registerStatement("ALTER TABLE ... STATISTICS",
    {
        .description = R"DOCS_MD(
import { CloudNotSupportedBadge } from "/snippets/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx";

<CloudNotSupportedBadge/>

The following operations are available:

-   `ALTER TABLE [db].table ADD STATISTICS [IF NOT EXISTS] (column list) TYPE (type list)` - Adds statistic description to tables metadata.

-   `ALTER TABLE [db].table MODIFY STATISTICS (column list) TYPE (type list)` - Modifies statistic description to tables metadata.

-   `ALTER TABLE [db].table DROP STATISTICS [IF EXISTS] (column list)` - Removes statistics from the metadata of the specified columns and deletes all statistics objects in all parts for the specified columns.

-   `ALTER TABLE [db].table CLEAR STATISTICS [IF EXISTS] (column list)` - Deletes all statistics objects in all parts for the specified columns. Statistics objects can be rebuild using `ALTER TABLE MATERIALIZE STATISTICS`.

-   `ALTER TABLE [db.]table MATERIALIZE STATISTICS (ALL | [IF EXISTS] (column list))` - Rebuilds the statistic for columns. Implemented as a [mutation](/reference/statements/alter/index#mutations).

The first two commands are lightweight in a sense that they only change metadata or remove files.

Also, they are replicated, syncing statistics metadata via ZooKeeper.

## Example: {#example}

Adding two statistics types to two columns:

```sql
ALTER TABLE t1 MODIFY STATISTICS c, d TYPE tdigest, uniq_v2;
```

<Note>
Statistic are supported only for [`*MergeTree`](/reference/engines/table-engines/mergetree-family/mergetree) engine tables (including [replicated](/reference/engines/table-engines/mergetree-family/replication) variants).
</Note>
)DOCS_MD",
        .syntax = R"(
ALTER TABLE [db].table ADD STATISTICS [IF NOT EXISTS] (column list) TYPE (type list)
ALTER TABLE [db].table MODIFY STATISTICS (column list) TYPE (type list)
ALTER TABLE [db].table DROP STATISTICS [IF EXISTS] (column list)
ALTER TABLE [db].table CLEAR STATISTICS [IF EXISTS] (column list)
ALTER TABLE [db].table MATERIALIZE STATISTICS [IF EXISTS] (column list)
)",
        .parent = "ALTER",
        .related = {"ALTER", "CREATE TABLE", "EXPLAIN"},
    });

    factory.registerStatement("ALTER TABLE ... MODIFY COMMENT",
    {
        .description = R"DOCS_MD(
Adds, modifies, or removes a table comment, regardless of whether it was set
before or not. The comment change is reflected in both [`system.tables`](/reference/system-tables/tables)
and in the `SHOW CREATE TABLE` query.

## Syntax {#syntax}

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

## Examples {#examples}

To create a table with a comment:

```sql title="Query"
CREATE TABLE table_with_comment
(
    `k` UInt64,
    `s` String
)
ENGINE = Memory()
COMMENT 'The temporary table';
```

To modify the table comment:

```sql title="Query"
ALTER TABLE table_with_comment
MODIFY COMMENT 'new comment on a table';
```

To view the modified comment:

```sql title="Query"
SELECT comment
FROM system.tables
WHERE database = currentDatabase() AND name = 'table_with_comment';
```

```text title="Response"
┌─comment────────────────┐
│ new comment on a table │
└────────────────────────┘
```

To remove the table comment:

```sql title="Query"
ALTER TABLE table_with_comment MODIFY COMMENT '';
```

To verify that the comment was removed:

```sql title="Query"
SELECT comment
FROM system.tables
WHERE database = currentDatabase() AND name = 'table_with_comment';
```

```text title="Response"
┌─comment─┐
│         │
└─────────┘
```

## Caveats {#caveats}

For Replicated tables, the comment can be different on different replicas.
Modifying the comment applies to a single replica.

The feature is available since version 23.9. It does not work in previous
ClickHouse versions.

## Related content {#related-content}

- [`COMMENT`](/reference/statements/create/table#comment-clause) clause
- [`ALTER DATABASE ... MODIFY COMMENT`](/reference/statements/alter/database-comment)
)DOCS_MD",
        .syntax = R"(
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
)",
        .parent = "ALTER",
        .related = {"ALTER", "ALTER DATABASE ... MODIFY COMMENT", "CREATE TABLE", "SHOW"},
    });

    factory.registerStatement("ALTER DATABASE ... MODIFY COMMENT",
    {
        .description = R"DOCS_MD(
Adds, modifies, or removes a database comment, regardless of whether it was set
before or not. The comment change is reflected in both [`system.databases`](/reference/system-tables/databases)
and the `SHOW CREATE DATABASE` query.

## Syntax {#syntax}

``` sql
ALTER DATABASE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

## Examples {#examples}

To create a `DATABASE` with a comment:

``` sql title="Query"
CREATE DATABASE database_with_comment ENGINE = Memory COMMENT 'The temporary database';
```

To modify the comment:

``` sql title="Query"
ALTER DATABASE database_with_comment
MODIFY COMMENT 'new comment on a database';
```

To view the modified comment:

```sql title="Query"
SELECT comment
FROM system.databases
WHERE name = 'database_with_comment';
```

```text title="Response"
┌─comment─────────────────┐
│ new comment on database │
└─────────────────────────┘
```

To remove the database comment:

``` sql title="Query"
ALTER DATABASE database_with_comment
MODIFY COMMENT '';
```

To verify that the comment was removed:

```sql title="Query"
SELECT comment
FROM system.databases
WHERE  name = 'database_with_comment';
```

```text title="Response"
┌─comment─┐
│         │
└─────────┘
```

## Related content {#related-content}

- [`COMMENT`](/reference/statements/create/table#comment-clause) clause
- [`ALTER TABLE ... MODIFY COMMENT`](/reference/statements/alter/comment)
)DOCS_MD",
        .syntax = R"(
ALTER DATABASE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
)",
        .parent = "ALTER",
        .related = {"ALTER", "ALTER TABLE ... MODIFY COMMENT", "CREATE DATABASE", "SHOW"},
    });

    factory.registerStatement("ALTER TABLE ... MODIFY QUERY",
    {
        .description = R"DOCS_MD(
You can modify `SELECT` query that was specified when a [materialized view](/reference/statements/create/view#materialized-view) was created with the `ALTER TABLE ... MODIFY QUERY` statement without interrupting ingestion process.

This command is created to change materialized view created with `TO [db.]name` clause. It does not change the structure of the underlying storage table and it does not change the columns' definition of the materialized view, because of this the application of this command is very limited for materialized views are created without `TO [db.]name` clause.

**Example with TO table**

```sql
CREATE TABLE events (ts DateTime, event_type String)
ENGINE = MergeTree ORDER BY (event_type, ts);

CREATE TABLE events_by_day (ts DateTime, event_type String, events_cnt UInt64)
ENGINE = SummingMergeTree ORDER BY (event_type, ts);

CREATE MATERIALIZED VIEW mv TO events_by_day AS
SELECT toStartOfDay(ts) ts, event_type, count() events_cnt
FROM events
GROUP BY ts, event_type;

INSERT INTO events
SELECT DATE '2020-01-01' + interval number * 900 second,
       ['imp', 'click'][number%2+1]
FROM numbers(100);

SELECT ts, event_type, sum(events_cnt)
FROM events_by_day
GROUP BY ts, event_type
ORDER BY ts, event_type;

┌──────────────────ts─┬─event_type─┬─sum(events_cnt)─┐
│ 2020-01-01 00:00:00 │ click      │              48 │
│ 2020-01-01 00:00:00 │ imp        │              48 │
│ 2020-01-02 00:00:00 │ click      │               2 │
│ 2020-01-02 00:00:00 │ imp        │               2 │
└─────────────────────┴────────────┴─────────────────┘

-- Let's add the new measurement `cost`
-- and the new dimension `browser`.

ALTER TABLE events
  ADD COLUMN browser String,
  ADD COLUMN cost Float64;

-- Column do not have to match in a materialized view and TO
-- (destination table), so the next alter does not break insertion.

ALTER TABLE events_by_day
    ADD COLUMN cost Float64,
    ADD COLUMN browser String after event_type,
    MODIFY ORDER BY (event_type, ts, browser);

INSERT INTO events
SELECT Date '2020-01-02' + interval number * 900 second,
       ['imp', 'click'][number%2+1],
       ['firefox', 'safary', 'chrome'][number%3+1],
       10/(number+1)%33
FROM numbers(100);

-- New columns `browser` and `cost` are empty because we did not change Materialized View yet.

SELECT ts, event_type, browser, sum(events_cnt) events_cnt, round(sum(cost),2) cost
FROM events_by_day
GROUP BY ts, event_type, browser
ORDER BY ts, event_type;

┌──────────────────ts─┬─event_type─┬─browser─┬─events_cnt─┬─cost─┐
│ 2020-01-01 00:00:00 │ click      │         │         48 │    0 │
│ 2020-01-01 00:00:00 │ imp        │         │         48 │    0 │
│ 2020-01-02 00:00:00 │ click      │         │         50 │    0 │
│ 2020-01-02 00:00:00 │ imp        │         │         50 │    0 │
│ 2020-01-03 00:00:00 │ click      │         │          2 │    0 │
│ 2020-01-03 00:00:00 │ imp        │         │          2 │    0 │
└─────────────────────┴────────────┴─────────┴────────────┴──────┘

ALTER TABLE mv MODIFY QUERY
  SELECT toStartOfDay(ts) ts, event_type, browser,
  count() events_cnt,
  sum(cost) cost
  FROM events
  GROUP BY ts, event_type, browser;

INSERT INTO events
SELECT Date '2020-01-03' + interval number * 900 second,
       ['imp', 'click'][number%2+1],
       ['firefox', 'safary', 'chrome'][number%3+1],
       10/(number+1)%33
FROM numbers(100);

SELECT ts, event_type, browser, sum(events_cnt) events_cnt, round(sum(cost),2) cost
FROM events_by_day
GROUP BY ts, event_type, browser
ORDER BY ts, event_type;

┌──────────────────ts─┬─event_type─┬─browser─┬─events_cnt─┬──cost─┐
│ 2020-01-01 00:00:00 │ click      │         │         48 │     0 │
│ 2020-01-01 00:00:00 │ imp        │         │         48 │     0 │
│ 2020-01-02 00:00:00 │ click      │         │         50 │     0 │
│ 2020-01-02 00:00:00 │ imp        │         │         50 │     0 │
│ 2020-01-03 00:00:00 │ click      │ firefox │         16 │  6.84 │
│ 2020-01-03 00:00:00 │ click      │         │          2 │     0 │
│ 2020-01-03 00:00:00 │ click      │ safary  │         16 │  9.82 │
│ 2020-01-03 00:00:00 │ click      │ chrome  │         16 │  5.63 │
│ 2020-01-03 00:00:00 │ imp        │         │          2 │     0 │
│ 2020-01-03 00:00:00 │ imp        │ firefox │         16 │ 15.14 │
│ 2020-01-03 00:00:00 │ imp        │ safary  │         16 │  6.14 │
│ 2020-01-03 00:00:00 │ imp        │ chrome  │         16 │  7.89 │
│ 2020-01-04 00:00:00 │ click      │ safary  │          1 │   0.1 │
│ 2020-01-04 00:00:00 │ click      │ firefox │          1 │   0.1 │
│ 2020-01-04 00:00:00 │ imp        │ firefox │          1 │   0.1 │
│ 2020-01-04 00:00:00 │ imp        │ chrome  │          1 │   0.1 │
└─────────────────────┴────────────┴─────────┴────────────┴───────┘

-- !!! During `MODIFY ORDER BY` PRIMARY KEY was implicitly introduced.

SHOW CREATE TABLE events_by_day FORMAT TSVRaw

CREATE TABLE test.events_by_day
(
    `ts` DateTime,
    `event_type` String,
    `browser` String,
    `events_cnt` UInt64,
    `cost` Float64
)
ENGINE = SummingMergeTree
PRIMARY KEY (event_type, ts)
ORDER BY (event_type, ts, browser)

-- !!! The columns' definition is unchanged but it does not matter, we are not querying
-- MATERIALIZED VIEW, we are querying TO (storage) table.
-- SELECT section is updated.

SHOW CREATE TABLE mv FORMAT TSVRaw;

CREATE MATERIALIZED VIEW test.mv TO test.events_by_day
(
    `ts` DateTime,
    `event_type` String,
    `events_cnt` UInt64
) AS
SELECT
    toStartOfDay(ts) AS ts,
    event_type,
    browser,
    count() AS events_cnt,
    sum(cost) AS cost
FROM test.events
GROUP BY
    ts,
    event_type,
    browser
```

**Example without TO table**

The application is very limited because you can only change the `SELECT` section without adding new columns.

```sql
CREATE TABLE src_table (`a` UInt32) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv (`a` UInt32) ENGINE = MergeTree ORDER BY a AS SELECT a FROM src_table;
INSERT INTO src_table (a) VALUES (1), (2);
SELECT * FROM mv;
```
```text
┌─a─┐
│ 1 │
│ 2 │
└───┘
```
```sql
ALTER TABLE mv MODIFY QUERY SELECT a * 2 as a FROM src_table;
INSERT INTO src_table (a) VALUES (3), (4);
SELECT * FROM mv;
```
```text
┌─a─┐
│ 6 │
│ 8 │
└───┘
┌─a─┐
│ 1 │
│ 2 │
└───┘
```

## ALTER TABLE ... MODIFY REFRESH Statement {#alter-table--modify-refresh-statement}

`ALTER TABLE ... MODIFY REFRESH` changes refresh parameters of a [Refreshable Materialized View](/reference/statements/create/view#refreshable-materialized-view), including the schedule, dependencies, randomization, and [refresh settings](/reference/statements/create/view#refresh-settings).

```sql
ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]
```

The schedule (`EVERY` or `AFTER`) is mandatory: the statement replaces *all* refresh parameters at once. Any clause not specified — `RANDOMIZE FOR`, `DEPENDS ON`, or `SETTINGS` — is removed or reset to defaults. To change only refresh settings, repeat the current schedule.

The command updates the refresh configuration of the existing view in place without recreating the materialized view or its target table, clearing existing target data, or canceling an already-running refresh. Reads continue against the existing target table while the configuration is changed.

Repeating the command with the same complete `REFRESH` specification is safe. Make sure to repeat every clause that should remain configured, because each execution replaces all refresh parameters as described above.

```sql
-- Change the schedule.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE;

-- Change retry settings (schedule must be repeated).
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE
SETTINGS refresh_retries = 5,
         refresh_retry_initial_backoff_ms = 500,
         refresh_retry_max_backoff_ms = 60000;

-- Add or keep a dependency.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR DEPENDS ON other_rmv;

-- Drop the dependency by omitting `DEPENDS ON`.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR;
```

Limitations:

- `ALTER TABLE ... MODIFY SETTING` is not supported on materialized views; refresh settings can only be changed via `MODIFY REFRESH`.
- Adding or removing `APPEND` is not supported.
- The `all_replicas` refresh setting cannot be changed after the view is created.

The full list of refresh settings is documented in [Refresh Settings](/reference/statements/create/view#refresh-settings). Refresh status, including the currently applied settings, is visible in [`system.view_refreshes`](/reference/system-tables/view_refreshes).
)DOCS_MD",
        .syntax = R"(
ALTER TABLE [db.]name [ON CLUSTER cluster] MODIFY QUERY SELECT ...
)",
        .parent = "ALTER",
        .related = {"ALTER", "CREATE VIEW"},
    });

    factory.registerStatement("ALTER TABLE ... APPLY DELETED MASK",
    {
        .description = R"DOCS_MD(
```sql
ALTER TABLE [db].name [ON CLUSTER cluster] APPLY DELETED MASK [IN PARTITION partition_id]
```

The command applies mask created by [lightweight delete](/reference/statements/delete) and forcefully removes rows marked as deleted from disk. This command is a heavyweight mutation, and it semantically equals to query ```ALTER TABLE [db].name DELETE WHERE _row_exists = 0```.

<Note>
It only works for tables in the [`MergeTree`](/reference/engines/table-engines/mergetree-family/mergetree) family (including [replicated](/reference/engines/table-engines/mergetree-family/replication) tables).
</Note>

**See also**

- [Lightweight deletes](/reference/statements/delete)
- [Heavyweight deletes](/reference/statements/alter/delete)
)DOCS_MD",
        .syntax = R"(
ALTER TABLE [db].name [ON CLUSTER cluster] APPLY DELETED MASK [IN PARTITION partition_id]
)",
        .parent = "ALTER",
        .related = {"ALTER", "DELETE", "ALTER TABLE ... DELETE"},
    });

    factory.registerStatement("ALTER TABLE ... APPLY PATCHES",
    {
        .description = R"DOCS_MD(
import { BetaBadge } from "/snippets/components/BetaBadge/BetaBadge.jsx";

<BetaBadge/>

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] APPLY PATCHES [IN PARTITION partition_id]
```

The command manually triggers the physical materialization of patch parts created by [lightweight `UPDATE`](/reference/statements/update) statements. It forcefully applies pending patches to the data parts by rewriting only the affected columns.

<Note>
- It only works for tables in the [`MergeTree`](/reference/engines/table-engines/mergetree-family/mergetree) family (including [replicated](/reference/engines/table-engines/mergetree-family/replication) tables).
- This is a mutation operation and executes asynchronously in the background.
</Note>

## When to use APPLY PATCHES {#when-to-use}

<Tip>
Generally, you should not need to use `APPLY PATCHES`
</Tip>

Patch parts are normally applied automatically during merges when the [`apply_patches_on_merge`](/reference/settings/merge-tree-settings/other#apply_patches_on_merge) setting is enabled (default). However, you may want to manually trigger patch application in these scenarios:

- To reduce the overhead of applying patches during `SELECT` queries
- To consolidate multiple patch parts before they accumulate
- To prepare data for backup or export with patches already materialized
- When `apply_patches_on_merge` is disabled and you want to control when patches are applied

## Examples {#examples}

Apply all pending patches for a table:
```sql
ALTER TABLE my_table APPLY PATCHES;
```

Apply patches only for a specific partition:
```sql
ALTER TABLE my_table APPLY PATCHES IN PARTITION '2024-01';
```

Combine with other operations:
```sql
ALTER TABLE my_table APPLY PATCHES, UPDATE column = value WHERE condition;
```

## Monitoring patch application {#monitor}

You can monitor the progress of patch application using the [`system.mutations`](/reference/system-tables/mutations) table:

```sql
SELECT * FROM system.mutations
WHERE table = 'my_table' AND command LIKE '%APPLY PATCHES%';
```

## See also {#see-also}

- [Lightweight `UPDATE`](/reference/statements/update) - Create patch parts with lightweight updates
- [`apply_patches_on_merge` setting](/reference/settings/merge-tree-settings/other#apply_patches_on_merge) - Control automatic patch application during merges
)DOCS_MD",
        .syntax = R"(
ALTER TABLE [db.]table [ON CLUSTER cluster] APPLY PATCHES [IN PARTITION partition_id]
)",
        .parent = "ALTER",
        .related = {"ALTER", "UPDATE", "ALTER TABLE ... UPDATE"},
    });
}

}
