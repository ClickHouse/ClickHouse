#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserShowTablesQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserTablePropertiesQuery.h>
#include <Parsers/ParserDescribeTableQuery.h>
#include <Parsers/ParserShowProcesslistQuery.h>
#include <Parsers/ParserCheckQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserKillQueryQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserWatchQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ParserShowAccessEntitiesQuery.h>
#include <Parsers/ParserShowCreateAccessEntityQuery.h>
#include <Parsers/ParserShowGrantsQuery.h>
#include <Parsers/ParserShowPrivilegesQuery.h>


namespace DB
{

bool ParserQueryWithOutput::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    ParserShowTablesQuery show_tables_p;
    ParserSelectWithUnionQuery select_p;
    ParserTablePropertiesQuery table_p;
    ParserDescribeTableQuery describe_table_p;
    ParserShowProcesslistQuery show_processlist_p;
    ParserCreateQuery create_p;
    ParserAlterQuery alter_p;
    ParserRenameQuery rename_p;
    ParserDropQuery drop_p;
    ParserCheckQuery check_p;
    ParserOptimizeQuery optimize_p;
    ParserKillQueryQuery kill_query_p;
    ParserWatchQuery watch_p;
    ParserShowAccessEntitiesQuery show_access_entities_p;
    ParserShowCreateAccessEntityQuery show_create_access_entity_p;
    ParserShowGrantsQuery show_grants_p;
    ParserShowPrivilegesQuery show_privileges_p;

    ASTPtr query;

    ParserKeyword s_ast("AST");
    ParserKeyword s_analyze("ANALYZE");
    bool explain_ast = false;
    bool analyze_syntax = false;

    if (enable_explain && s_ast.ignore(pos, expected, ranges))
        explain_ast = true;

    if (enable_explain && s_analyze.ignore(pos, expected, ranges))
        analyze_syntax = true;

    bool parsed = select_p.parse(pos, query, expected, ranges)
        || show_create_access_entity_p.parse(pos, query, expected, ranges) /// should be before `show_tables_p`
        || show_tables_p.parse(pos, query, expected, ranges)
        || table_p.parse(pos, query, expected, ranges)
        || describe_table_p.parse(pos, query, expected, ranges)
        || show_processlist_p.parse(pos, query, expected, ranges)
        || create_p.parse(pos, query, expected, ranges)
        || alter_p.parse(pos, query, expected, ranges)
        || rename_p.parse(pos, query, expected, ranges)
        || drop_p.parse(pos, query, expected, ranges)
        || check_p.parse(pos, query, expected, ranges)
        || kill_query_p.parse(pos, query, expected, ranges)
        || optimize_p.parse(pos, query, expected, ranges)
        || watch_p.parse(pos, query, expected, ranges)
        || show_access_entities_p.parse(pos, query, expected, ranges)
        || show_grants_p.parse(pos, query, expected, ranges)
        || show_privileges_p.parse(pos, query, expected, ranges);

    if (!parsed)
        return false;

    /// FIXME: try to prettify this cast using `as<>()`
    auto & query_with_output = dynamic_cast<ASTQueryWithOutput &>(*query);

    ParserKeyword s_into_outfile("INTO OUTFILE");
    if (s_into_outfile.ignore(pos, expected, ranges))
    {
        ParserStringLiteral out_file_p;
        if (!out_file_p.parse(pos, query_with_output.out_file, expected, ranges))
            return false;

        query_with_output.children.push_back(query_with_output.out_file);
    }

    ParserKeyword s_format("FORMAT");

    if (s_format.ignore(pos, expected, ranges))
    {
        ParserIdentifier format_p;

        if (!format_p.parse(pos, query_with_output.format, expected, ranges))
            return false;
        setIdentifierSpecial(query_with_output.format);

        query_with_output.children.push_back(query_with_output.format);
    }

    // SETTINGS key1 = value1, key2 = value2, ...
    ParserKeyword s_settings("SETTINGS");
    if (s_settings.ignore(pos, expected, ranges))
    {
        ParserSetQuery parser_settings(true);
        if (!parser_settings.parse(pos, query_with_output.settings_ast, expected, ranges))
            return false;
        query_with_output.children.push_back(query_with_output.settings_ast);
    }

    if (explain_ast)
    {
        node = std::make_shared<ASTExplainQuery>(ASTExplainQuery::ParsedAST);
        node->children.push_back(query);
    }
    else if (analyze_syntax)
    {
        node = std::make_shared<ASTExplainQuery>(ASTExplainQuery::AnalyzedSyntax);
        node->children.push_back(query);
    }
    else
        node = query;

    return true;
}

}
