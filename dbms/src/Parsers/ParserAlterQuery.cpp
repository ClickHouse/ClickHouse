#include <Parsers/ParserAlterQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{

bool ParserAlterQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_alter_table("ALTER TABLE");
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

    ParserToken s_dot(TokenType::Dot);
    ParserToken s_comma(TokenType::Comma);

    ParserIdentifier table_parser;
    ParserCompoundIdentifier parser_name;
    ParserCompoundColumnDeclaration parser_col_decl;
    ParserPartition parser_partition;
    ParserStringLiteral parser_string_literal;
    ParserExpression exp_elem;

    String cluster_str;
    ASTPtr col_type;
    ASTPtr col_after;
    ASTPtr col_drop;

    auto query = std::make_shared<ASTAlterQuery>();

    if (!s_alter_table.ignore(pos, expected))
        return false;

    if (!parseDatabaseAndTableName(pos, expected, query->database, query->table))
        return false;

    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    bool parsing_finished = false;
    do
    {
        ASTAlterQuery::Parameters params;

        if (s_add_column.ignore(pos, expected))
        {
            if (!parser_col_decl.parse(pos, params.col_decl, expected))
                return false;

            if (s_after.ignore(pos, expected))
            {
                if (!parser_name.parse(pos, params.column, expected))
                    return false;
            }

            params.type = ASTAlterQuery::ADD_COLUMN;
        }
        else if (s_drop_partition.ignore(pos, expected))
        {
            if (!parser_partition.parse(pos, params.partition, expected))
                return false;

            params.type = ASTAlterQuery::DROP_PARTITION;
        }
        else if (s_drop_column.ignore(pos, expected))
        {
            if (!parser_name.parse(pos, params.column, expected))
                return false;

            params.type = ASTAlterQuery::DROP_COLUMN;
            params.detach = false;
        }
        else if (s_clear_column.ignore(pos, expected))
        {
            if (!parser_name.parse(pos, params.column, expected))
                return false;

            params.type = ASTAlterQuery::DROP_COLUMN;
            params.clear_column = true;
            params.detach = false;

            if (s_in_partition.ignore(pos, expected))
            {
                if (!parser_partition.parse(pos, params.partition, expected))
                    return false;
            }
        }
        else if (s_detach_partition.ignore(pos, expected))
        {
            if (!parser_partition.parse(pos, params.partition, expected))
                return false;

            params.type = ASTAlterQuery::DROP_PARTITION;
            params.detach = true;
        }
        else if (s_attach_partition.ignore(pos, expected))
        {
            if (!parser_partition.parse(pos, params.partition, expected))
                return false;

            if (s_from.ignore(pos))
            {
                if (!parseDatabaseAndTableName(pos, expected, params.from_database, params.from_table))
                    return false;

                params.replace = false;
                params.type = ASTAlterQuery::REPLACE_PARTITION;
            }
            else
            {
                params.type = ASTAlterQuery::ATTACH_PARTITION;
            }
        }
        else if (ParserKeyword{"REPLACE PARTITION"}.ignore(pos, expected))
        {
            if (!parser_partition.parse(pos, params.partition, expected))
                return false;

            if (!s_from.ignore(pos, expected))
                return false;

            if (!parseDatabaseAndTableName(pos, expected, params.from_database, params.from_table))
                return false;

            params.replace = true;
            params.type = ASTAlterQuery::REPLACE_PARTITION;
        }
        else if (s_attach_part.ignore(pos, expected))
        {
            if (!parser_string_literal.parse(pos, params.partition, expected))
                return false;

            params.part = true;
            params.type = ASTAlterQuery::ATTACH_PARTITION;
        }
        else if (s_fetch_partition.ignore(pos, expected))
        {
            if (!parser_partition.parse(pos, params.partition, expected))
                return false;

            if (!s_from.ignore(pos, expected))
                return false;

            ASTPtr ast_from;
            if (!parser_string_literal.parse(pos, ast_from, expected))
                return false;

            params.from = typeid_cast<const ASTLiteral &>(*ast_from).value.get<const String &>();
            params.type = ASTAlterQuery::FETCH_PARTITION;
        }
        else if (s_freeze_partition.ignore(pos, expected))
        {
            if (!parser_partition.parse(pos, params.partition, expected))
                return false;

            /// WITH NAME 'name' - place local backup to directory with specified name
            if (s_with.ignore(pos, expected))
            {
                if (!s_name.ignore(pos, expected))
                    return false;

                ASTPtr ast_with_name;
                if (!parser_string_literal.parse(pos, ast_with_name, expected))
                    return false;

                params.with_name = typeid_cast<const ASTLiteral &>(*ast_with_name).value.get<const String &>();
            }

            params.type = ASTAlterQuery::FREEZE_PARTITION;
        }
        else if (s_modify_column.ignore(pos, expected))
        {
            if (!parser_col_decl.parse(pos, params.col_decl, expected))
                return false;

            params.type = ASTAlterQuery::MODIFY_COLUMN;
        }
        else if (s_modify_primary_key.ignore(pos, expected))
        {
            if (pos->type != TokenType::OpeningRoundBracket)
                return false;
            ++pos;

            if (!ParserNotEmptyExpressionList(false).parse(pos, params.primary_key, expected))
                return false;

            if (pos->type != TokenType::ClosingRoundBracket)
                return false;
            ++pos;

            params.type = ASTAlterQuery::MODIFY_PRIMARY_KEY;
        }
        else if (s_delete_where.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, params.predicate, expected))
                return false;

            params.type = ASTAlterQuery::DELETE;
        }
        else
            return false;

        if (!s_comma.ignore(pos, expected))
            parsing_finished = true;

        query->addParameters(params);
    }
    while (!parsing_finished);

    query->cluster = cluster_str;
    node = query;

    return true;
}
}
