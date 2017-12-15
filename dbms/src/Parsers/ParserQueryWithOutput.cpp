#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserShowTablesQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserTablePropertiesQuery.h>
#include <Parsers/ParserDescribeTableQuery.h>
#include <Parsers/ParserShowProcesslistQuery.h>
#include <Parsers/ParserCheckQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserKillQueryQuery.h>


namespace DB
{

bool ParserQueryWithOutput::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserShowTablesQuery show_tables_p;
    ParserSelectQuery select_p;
    ParserTablePropertiesQuery table_p;
    ParserDescribeTableQuery describe_table_p;
    ParserShowProcesslistQuery show_processlist_p;
    ParserCreateQuery create_p;
    ParserAlterQuery alter_p;
    ParserRenameQuery rename_p;
    ParserDropQuery drop_p;
    ParserCheckQuery check_p;
    ParserKillQueryQuery kill_query_p;

    ASTPtr query;

    bool parsed = select_p.parse(pos, query, expected)
        || show_tables_p.parse(pos, query, expected)
        || table_p.parse(pos, query, expected)
        || describe_table_p.parse(pos, query, expected)
        || show_processlist_p.parse(pos, query, expected)
        || create_p.parse(pos, query, expected)
        || alter_p.parse(pos, query, expected)
        || rename_p.parse(pos, query, expected)
        || drop_p.parse(pos, query, expected)
        || check_p.parse(pos, query, expected)
        || kill_query_p.parse(pos, query, expected);

    if (!parsed)
        return false;

    auto & query_with_output = dynamic_cast<ASTQueryWithOutput &>(*query);

    ParserKeyword s_into_outfile("INTO OUTFILE");
    if (s_into_outfile.ignore(pos, expected))
    {
        ParserStringLiteral out_file_p;
        if (!out_file_p.parse(pos, query_with_output.out_file, expected))
            return false;

        query_with_output.children.push_back(query_with_output.out_file);
    }

    ParserKeyword s_format("FORMAT");

    if (s_format.ignore(pos, expected))
    {
        ParserIdentifier format_p;

        if (!format_p.parse(pos, query_with_output.format, expected))
            return false;
        typeid_cast<ASTIdentifier &>(*(query_with_output.format)).kind = ASTIdentifier::Format;

        query_with_output.children.push_back(query_with_output.format);
    }

    node = query;
    return true;
}

}
