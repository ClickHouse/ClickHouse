#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserBackupQuery.h>
#include <Parsers/ParserCheckQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDescribeTableQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserUndropQuery.h>
#include <Parsers/ParserExplainQuery.h>
#include <Parsers/ParserKillQueryQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserShowProcesslistQuery.h>
#include <Parsers/ParserShowTablesQuery.h>
#include <Parsers/ParserShowColumnsQuery.h>
#include <Parsers/ParserShowEngineQuery.h>
#include <Parsers/ParserShowFunctionsQuery.h>
#include <Parsers/ParserShowIndexesQuery.h>
#include <Parsers/ParserShowSettingQuery.h>
#include <Parsers/ParserTablePropertiesQuery.h>
#include <Parsers/ParserWatchQuery.h>
#include <Parsers/ParserDescribeCacheQuery.h>
#include <Parsers/Access/ParserShowAccessEntitiesQuery.h>
#include <Parsers/Access/ParserShowAccessQuery.h>
#include <Parsers/Access/ParserShowCreateAccessEntityQuery.h>
#include <Parsers/Access/ParserShowGrantsQuery.h>
#include <Parsers/Access/ParserShowPrivilegesQuery.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>


namespace DB
{

bool ParserQueryWithOutput::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserShowTablesQuery show_tables_p;
    ParserShowColumnsQuery show_columns_p;
    ParserShowEnginesQuery show_engine_p;
    ParserShowFunctionsQuery show_functions_p;
    ParserShowIndexesQuery show_indexes_p;
    ParserShowSettingQuery show_setting_p;
    ParserSelectWithUnionQuery select_p;
    ParserTablePropertiesQuery table_p;
    ParserDescribeTableQuery describe_table_p;
    ParserDescribeCacheQuery describe_cache_p;
    ParserShowProcesslistQuery show_processlist_p;
    ParserCreateQuery create_p;
    ParserAlterQuery alter_p;
    ParserRenameQuery rename_p;
    ParserDropQuery drop_p;
    ParserUndropQuery undrop_p;
    ParserCheckQuery check_p;
    ParserOptimizeQuery optimize_p;
    ParserKillQueryQuery kill_query_p;
    ParserWatchQuery watch_p;
    ParserShowAccessQuery show_access_p;
    ParserShowAccessEntitiesQuery show_access_entities_p;
    ParserShowCreateAccessEntityQuery show_create_access_entity_p;
    ParserShowGrantsQuery show_grants_p;
    ParserShowPrivilegesQuery show_privileges_p;
    ParserExplainQuery explain_p(end, allow_settings_after_format_in_insert);
    ParserBackupQuery backup_p;

    ASTPtr query;

    bool parsed =
           explain_p.parse(pos, query, expected)
        || select_p.parse(pos, query, expected)
        || show_create_access_entity_p.parse(pos, query, expected) /// should be before `show_tables_p`
        || show_tables_p.parse(pos, query, expected)
        || show_columns_p.parse(pos, query, expected)
        || show_engine_p.parse(pos, query, expected)
        || show_functions_p.parse(pos, query, expected)
        || show_indexes_p.parse(pos, query, expected)
        || show_setting_p.parse(pos, query, expected)
        || table_p.parse(pos, query, expected)
        || describe_cache_p.parse(pos, query, expected)
        || describe_table_p.parse(pos, query, expected)
        || show_processlist_p.parse(pos, query, expected)
        || create_p.parse(pos, query, expected)
        || alter_p.parse(pos, query, expected)
        || rename_p.parse(pos, query, expected)
        || drop_p.parse(pos, query, expected)
        || undrop_p.parse(pos, query, expected)
        || check_p.parse(pos, query, expected)
        || kill_query_p.parse(pos, query, expected)
        || optimize_p.parse(pos, query, expected)
        || watch_p.parse(pos, query, expected)
        || show_access_p.parse(pos, query, expected)
        || show_access_entities_p.parse(pos, query, expected)
        || show_grants_p.parse(pos, query, expected)
        || show_privileges_p.parse(pos, query, expected)
        || backup_p.parse(pos, query, expected);

    if (!parsed)
        return false;

    /// FIXME: try to prettify this cast using `as<>()`
    auto & query_with_output = dynamic_cast<ASTQueryWithOutput &>(*query);

    ParserKeyword s_into_outfile(Keyword::INTO_OUTFILE);
    if (s_into_outfile.ignore(pos, expected))
    {
        ParserStringLiteral out_file_p;
        if (!out_file_p.parse(pos, query_with_output.out_file, expected))
            return false;

        ParserKeyword s_append(Keyword::APPEND);
        if (s_append.ignore(pos, expected))
        {
            query_with_output.is_outfile_append = true;
        }

        ParserKeyword s_truncate(Keyword::TRUNCATE);
        if (s_truncate.ignore(pos, expected))
        {
            query_with_output.is_outfile_truncate = true;
        }

        ParserKeyword s_stdout(Keyword::AND_STDOUT);
        if (s_stdout.ignore(pos, expected))
        {
            query_with_output.is_into_outfile_with_stdout = true;
        }

        ParserKeyword s_compression_method(Keyword::COMPRESSION);
        if (s_compression_method.ignore(pos, expected))
        {
            ParserStringLiteral compression;
            if (!compression.parse(pos, query_with_output.compression, expected))
                return false;
            query_with_output.children.push_back(query_with_output.compression);

            ParserKeyword s_compression_level(Keyword::LEVEL);
            if (s_compression_level.ignore(pos, expected))
            {
                ParserNumber compression_level;
                if (!compression_level.parse(pos, query_with_output.compression_level, expected))
                    return false;
                query_with_output.children.push_back(query_with_output.compression_level);
            }
        }

        query_with_output.children.push_back(query_with_output.out_file);

    }

    /// These two sections are allowed in an arbitrary order.
    ParserKeyword s_format(Keyword::FORMAT);
    ParserKeyword s_settings(Keyword::SETTINGS);

    /** Why: let's take the following example:
      * SELECT 1 UNION ALL SELECT 2 FORMAT TSV
      * Each subquery can be put in parentheses and have its own settings:
      *   (SELECT 1 SETTINGS a=b) UNION ALL (SELECT 2 SETTINGS c=d) FORMAT TSV
      * And the whole query can have settings:
      *   (SELECT 1 SETTINGS a=b) UNION ALL (SELECT 2 SETTINGS c=d) FORMAT TSV SETTINGS e=f
      * A single query with output is parsed in the same way as the UNION ALL chain:
      *   SELECT 1 SETTINGS a=b FORMAT TSV SETTINGS e=f
      * So while these forms have a slightly different meaning, they both exist:
      *   SELECT 1 SETTINGS a=b FORMAT TSV
      *   SELECT 1 FORMAT TSV SETTINGS e=f
      * And due to this effect, the users expect that the FORMAT and SETTINGS may go in an arbitrary order.
      * But while this work:
      *   (SELECT 1) UNION ALL (SELECT 2) FORMAT TSV SETTINGS d=f
      * This does not work automatically, unless we explicitly allow different orders:
      *   (SELECT 1) UNION ALL (SELECT 2) SETTINGS d=f FORMAT TSV
      * Inevitably, we also allow this:
      *   SELECT 1 SETTINGS a=b SETTINGS d=f FORMAT TSV
      *   ^^^^^^^^^^^^^^^^^^^^^
      * Because this part is consumed into ASTSelectWithUnionQuery
      * and the rest into ASTQueryWithOutput.
      */

    for (size_t i = 0; i < 2; ++i)
    {
        if (!query_with_output.format && s_format.ignore(pos, expected))
        {
            ParserIdentifier format_p;

            if (!format_p.parse(pos, query_with_output.format, expected))
                return false;
            setIdentifierSpecial(query_with_output.format);

            query_with_output.children.push_back(query_with_output.format);
        }
        else if (!query_with_output.settings_ast && s_settings.ignore(pos, expected))
        {
            // SETTINGS key1 = value1, key2 = value2, ...
            ParserSetQuery parser_settings(true);
            if (!parser_settings.parse(pos, query_with_output.settings_ast, expected))
                return false;
            query_with_output.children.push_back(query_with_output.settings_ast);
        }
        else
            break;
    }

    node = std::move(query);
    return true;
}

}
