#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserShowTablesQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserTablePropertiesQuery.h>
#include <Parsers/ParserShowProcesslistQuery.h>
#include <Parsers/ParserCheckQuery.h>
#include <Parsers/ParserKillQueryQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Common/typeid_cast.h>


namespace DB
{

bool ParserQueryWithOutput::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserShowTablesQuery show_tables_p;
    ParserSelectQuery select_p;
    ParserTablePropertiesQuery table_p;
    ParserShowProcesslistQuery show_processlist_p;
    ParserCheckQuery check_p;
    ParserKillQueryQuery kill_query_p;

    ASTPtr query;

    bool parsed = select_p.parse(pos, end, query, max_parsed_pos, expected)
        || show_tables_p.parse(pos, end, query, max_parsed_pos, expected)
        || table_p.parse(pos, end, query, max_parsed_pos, expected)
        || show_processlist_p.parse(pos, end, query, max_parsed_pos, expected)
        || check_p.parse(pos, end, query, max_parsed_pos, expected)
        || kill_query_p.parse(pos, end, query, max_parsed_pos, expected);

    if (!parsed)
        return false;

    auto & query_with_output = dynamic_cast<ASTQueryWithOutput &>(*query);

    ParserString s_into("INTO", /* word_boundary_ = */ true, /* case_insensitive_ = */ true);
    if (s_into.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        ParserString s_outfile("OUTFILE", true, true);
        if (!s_outfile.ignore(pos, end, max_parsed_pos, expected))
        {
            expected = "OUTFILE";
            return false;
        }

        ws.ignore(pos, end);

        ParserStringLiteral out_file_p;
        if (!out_file_p.parse(pos, end, query_with_output.out_file, max_parsed_pos, expected))
            return false;

        query_with_output.children.push_back(query_with_output.out_file);

        ws.ignore(pos, end);
    }

    ParserString s_format("FORMAT", true, true);

    if (s_format.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        ParserIdentifier format_p;

        if (!format_p.parse(pos, end, query_with_output.format, max_parsed_pos, expected))
            return false;
        typeid_cast<ASTIdentifier &>(*(query_with_output.format)).kind = ASTIdentifier::Format;

        query_with_output.children.push_back(query_with_output.format);

        ws.ignore(pos, end);
    }

    node = query;
    return true;
}

}
