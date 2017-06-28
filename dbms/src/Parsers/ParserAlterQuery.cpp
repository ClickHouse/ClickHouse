#include <Parsers/ParserAlterQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

bool ParserAlterQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

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
    ParserKeyword s_reshard("RESHARD");
    ParserKeyword s_partition("PARTITION");

    ParserKeyword s_after("AFTER");
    ParserKeyword s_from("FROM");
    ParserKeyword s_in_partition("IN PARTITION");
    ParserKeyword s_copy("COPY");
    ParserKeyword s_to("TO");
    ParserKeyword s_using("USING");
    ParserKeyword s_coordinate("COORDINATE");
    ParserKeyword s_with("WITH");
    ParserKeyword s_name("NAME");

    ParserString s_dot(".");
    ParserString s_comma(",");
    ParserString s_doubledot("..");

    ParserWhitespaceOrComments ws;

    ParserIdentifier table_parser;
    ParserCompoundIdentifier parser_name;
    ParserCompoundColumnDeclaration parser_col_decl;
    ParserLiteral parser_literal;
    ParserUnsignedInteger parser_uint;
    ParserStringLiteral parser_string_literal;

    ASTPtr table;
    ASTPtr database;
    String cluster_str;
    ASTPtr col_type;
    ASTPtr col_after;
    ASTPtr col_drop;

    auto query = std::make_shared<ASTAlterQuery>();

    ws.ignore(pos, end);
    if (!s_alter_table.ignore(pos, end, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (!table_parser.parse(pos, end, database, max_parsed_pos, expected))
        return false;

    /// Parse [db].name
    if (s_dot.ignore(pos, end))
    {
        if (!table_parser.parse(pos, end, table, max_parsed_pos, expected))
            return false;

        query->table = typeid_cast<ASTIdentifier &>(*table).name;
        query->database = typeid_cast<ASTIdentifier &>(*database).name;
    }
    else
    {
        table = database;
        query->table = typeid_cast<ASTIdentifier &>(*table).name;
    }

    ws.ignore(pos, end);

    if (ParserKeyword{"ON"}.ignore(pos, end, max_parsed_pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, end, cluster_str, max_parsed_pos, expected))
            return false;
    }

    bool parsing_finished = false;
    do
    {
        ASTAlterQuery::Parameters params;
        ws.ignore(pos, end);

        if (s_add_column.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            if (!parser_col_decl.parse(pos, end, params.col_decl, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);
            if (s_after.ignore(pos, end, max_parsed_pos, expected))
            {
                ws.ignore(pos, end);

                if(!parser_name.parse(pos, end, params.column, max_parsed_pos, expected))
                    return false;
            }

            params.type = ASTAlterQuery::ADD_COLUMN;
        }
        else if (s_drop_partition.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            if (!parser_literal.parse(pos, end, params.partition, max_parsed_pos, expected))
                return false;

            params.type = ASTAlterQuery::DROP_PARTITION;
        }
        else if (s_drop_column.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            if (!parser_name.parse(pos, end, params.column, max_parsed_pos, expected))
                return false;

            params.type = ASTAlterQuery::DROP_COLUMN;
            params.detach = false;
        }
        else if (s_clear_column.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            if (!parser_name.parse(pos, end, params.column, max_parsed_pos, expected))
                return false;

            params.type = ASTAlterQuery::DROP_COLUMN;
            params.clear_column = true;
            params.detach = false;

            ws.ignore(pos, end);

            if (s_in_partition.ignore(pos, end, max_parsed_pos, expected))
            {
                ws.ignore(pos, end);

                if (!parser_literal.parse(pos, end, params.partition, max_parsed_pos, expected))
                    return false;
            }
        }
        else if (s_detach_partition.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            if (!parser_literal.parse(pos, end, params.partition, max_parsed_pos, expected))
                return false;

            params.type = ASTAlterQuery::DROP_PARTITION;
            params.detach = true;
        }
        else if (s_attach_partition.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            if (!parser_literal.parse(pos, end, params.partition, max_parsed_pos, expected))
                return false;

            params.type = ASTAlterQuery::ATTACH_PARTITION;
        }
        else if (s_attach_part.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            if (!parser_literal.parse(pos, end, params.partition, max_parsed_pos, expected))
                return false;

            params.part = true;
            params.type = ASTAlterQuery::ATTACH_PARTITION;
        }
        else if (s_fetch_partition.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            if (!parser_literal.parse(pos, end, params.partition, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);

            if (!s_from.ignore(pos, end, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);

            ASTPtr ast_from;
            if (!parser_string_literal.parse(pos, end, ast_from, max_parsed_pos, expected))
                return false;

            params.from = typeid_cast<const ASTLiteral &>(*ast_from).value.get<const String &>();
            params.type = ASTAlterQuery::FETCH_PARTITION;
        }
        else if (s_freeze_partition.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            if (!parser_literal.parse(pos, end, params.partition, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);

            /// WITH NAME 'name' - place local backup to directory with specified name
            if (s_with.ignore(pos, end, max_parsed_pos, expected))
            {
                ws.ignore(pos, end);

                if (!s_name.ignore(pos, end, max_parsed_pos, expected))
                    return false;

                ws.ignore(pos, end);

                ASTPtr ast_with_name;
                if (!parser_string_literal.parse(pos, end, ast_with_name, max_parsed_pos, expected))
                    return false;

                params.with_name = typeid_cast<const ASTLiteral &>(*ast_with_name).value.get<const String &>();

                ws.ignore(pos, end);
            }

            params.type = ASTAlterQuery::FREEZE_PARTITION;
        }
        else if (s_modify_column.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            if (!parser_col_decl.parse(pos, end, params.col_decl, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);

            params.type = ASTAlterQuery::MODIFY_COLUMN;
        }
        else if (s_modify_primary_key.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            if (!ParserString("(").ignore(pos, end, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);

            if (!ParserNotEmptyExpressionList(false).parse(pos, end, params.primary_key, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);

            if (!ParserString(")").ignore(pos, end, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);

            params.type = ASTAlterQuery::MODIFY_PRIMARY_KEY;
        }
        else if (s_reshard.ignore(pos, end, max_parsed_pos, expected))
        {
            ParserList weighted_zookeeper_paths_p(std::make_unique<ParserWeightedZooKeeperPath>(), std::make_unique<ParserString>(","), false);
            ParserExpressionWithOptionalAlias parser_sharding_key_expr(false);
            ParserStringLiteral parser_coordinator;

            ws.ignore(pos, end);

            if (s_copy.ignore(pos, end, max_parsed_pos, expected))
                params.do_copy = true;

            ws.ignore(pos, end);

            if (s_partition.ignore(pos, end, max_parsed_pos, expected))
            {
                ws.ignore(pos, end);

                if (!parser_uint.parse(pos, end, params.partition, max_parsed_pos, expected))
                    return false;

                ws.ignore(pos, end);

                if (s_doubledot.ignore(pos, end, max_parsed_pos, expected))
                {
                    ws.ignore(pos, end);

                    if (!parser_uint.parse(pos, end, params.last_partition, max_parsed_pos, expected))
                        return false;
                }
            }

            ws.ignore(pos, end);

            if (!s_to.ignore(pos, end, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);

            if (!weighted_zookeeper_paths_p.parse(pos, end, params.weighted_zookeeper_paths, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);

            if (!s_using.ignore(pos, end, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);

            if (!parser_sharding_key_expr.parse(pos, end, params.sharding_key_expr, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);

            if (s_coordinate.ignore(pos, end, max_parsed_pos, expected))
            {
                ws.ignore(pos, end);

                if (!s_with.ignore(pos, end, max_parsed_pos, expected))
                    return false;

                ws.ignore(pos, end);

                if (!parser_coordinator.parse(pos, end, params.coordinator, max_parsed_pos, expected))
                    return false;

                ws.ignore(pos, end);
            }

            params.type = ASTAlterQuery::RESHARD_PARTITION;
        }
        else
            return false;

        ws.ignore(pos, end);

        if (!s_comma.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);
            parsing_finished = true;
        }

        query->addParameters(params);
    }
    while (!parsing_finished);

    query->range = StringRange(begin, end);
    query->cluster = cluster_str;
    node = query;

    return true;
}
}
