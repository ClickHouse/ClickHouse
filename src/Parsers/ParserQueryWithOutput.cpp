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
#include <Parsers/ParserSnapshotQuery.h>
#include <Parsers/ParserTablePropertiesQuery.h>
#include <Parsers/ParserDescribeCacheQuery.h>
#include <Parsers/Access/ParserShowAccessEntitiesQuery.h>
#include <Parsers/Access/ParserShowAccessQuery.h>
#include <Parsers/Access/ParserShowCreateAccessEntityQuery.h>
#include <Parsers/Access/ParserShowGrantsQuery.h>
#include <Parsers/Access/ParserShowPrivilegesQuery.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <algorithm>


namespace DB
{

/** `SHOW GRANTS`, `SHOW CREATE USER` and the rest of the read-only half of access management.
  * Left out of a `CLICKHOUSE_PARSER_NO_DCL` build - see `ParserQuery.cpp`.
  */
#if defined(CLICKHOUSE_PARSER_NO_DCL)

static bool parseShowCreateAccessEntityQuery(IParser::Pos &, ASTPtr &, Expected &) { return false; }
static bool parseShowAccessQuery(IParser::Pos &, ASTPtr &, Expected &) { return false; }

#else

static bool parseShowCreateAccessEntityQuery(IParser::Pos & pos, ASTPtr & query, Expected & expected)
{
    ParserShowCreateAccessEntityQuery show_create_access_entity_p;
    return show_create_access_entity_p.parse(pos, query, expected);
}

static bool parseShowAccessQuery(IParser::Pos & pos, ASTPtr & query, Expected & expected)
{
    ParserShowAccessQuery show_access_p;
    ParserShowAccessEntitiesQuery show_access_entities_p;
    ParserShowGrantsQuery show_grants_p;
    ParserShowPrivilegesQuery show_privileges_p;

    return show_access_p.parse(pos, query, expected)
        || show_access_entities_p.parse(pos, query, expected)
        || show_grants_p.parse(pos, query, expected)
        || show_privileges_p.parse(pos, query, expected);
}

#endif

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
    ParserExplainQuery explain_p(end, allow_settings_after_format_in_insert);
    ParserBackupQuery backup_p;
    ParserSnapshotQuery snapshot_p;

    ASTPtr query;

    bool parsed =
           explain_p.parse(pos, query, expected)
        || select_p.parse(pos, query, expected)
        || parseShowCreateAccessEntityQuery(pos, query, expected) /// should be before `show_tables_p`
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
        || parseShowAccessQuery(pos, query, expected)
        || backup_p.parse(pos, query, expected)
        || snapshot_p.parse(pos, query, expected);

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
            query_with_output.setIsOutfileAppend(true);
        }

        ParserKeyword s_truncate(Keyword::TRUNCATE);
        if (s_truncate.ignore(pos, expected))
        {
            query_with_output.setIsOutfileTruncate(true);
        }

        ParserKeyword s_stdout(Keyword::AND_STDOUT);
        if (s_stdout.ignore(pos, expected))
        {
            query_with_output.setIsIntoOutfileWithStdout(true);
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
        if (!query_with_output.format_ast && s_format.ignore(pos, expected))
        {
            ParserIdentifier format_p;

            if (!format_p.parse(pos, query_with_output.format_ast, expected))
                return false;
            setIdentifierSpecial(query_with_output.format_ast);

            query_with_output.children.push_back(query_with_output.format_ast);
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

    /// The formatter always outputs the output options in a fixed order:
    /// INTO OUTFILE (with COMPRESSION/LEVEL), then FORMAT, then SETTINGS.
    /// The parser, however, may append these children in a different order:
    /// FORMAT and SETTINGS are allowed in either order above, and for
    /// `EXPLAIN INSERT ... SELECT ... FORMAT ...` the FORMAT child is attached
    /// to the query (by `ParserExplainQuery`) before INTO OUTFILE is parsed here.
    /// Reorder the output-option children into the canonical (formatting) order
    /// so that the tree hash is stable across a formatting roundtrip, regardless
    /// of the original clause order. The order is shared with `cloneOutputOptions`
    /// and `formatImpl` via `ASTQueryWithOutput::output_option_members`.
    {
        auto & ch = query_with_output.children;
        auto is_output_option = [&](const ASTPtr & child)
        {
            return std::any_of(
                ASTQueryWithOutput::output_option_members.begin(),
                ASTQueryWithOutput::output_option_members.end(),
                [&](auto member) { return (query_with_output.*member) && (query_with_output.*member).get() == child.get(); });
        };

        ch.erase(std::remove_if(ch.begin(), ch.end(), is_output_option), ch.end());
        for (auto member : ASTQueryWithOutput::output_option_members)
            if (query_with_output.*member)
                ch.push_back(query_with_output.*member);
    }

    node = std::move(query);
    return true;
}

}

namespace DB
{

void registerStatementQueryWithOutput(StatementFactory & factory)
{
    factory.registerStatement("FORMAT",
    {
        .description = R"DOCS_MD(
ClickHouse supports a wide range of [serialization formats](/reference/formats/index) that can be used on query results among other things. There are multiple ways to choose a format for `SELECT` output, one of them is to specify `FORMAT format` at the end of query to get resulting data in any specific format.

Specific format might be used either for convenience, integration with other systems or performance gain.

## Default Format {#default-format}

If the `FORMAT` clause is omitted, the default format is used, which depends on both the settings and the interface used for accessing the ClickHouse server. For the [HTTP interface](/concepts/features/interfaces/http) and the [command-line client](/concepts/features/interfaces/client) in batch mode, the default format is `TabSeparated`. For the command-line client in interactive mode, the default format is `PrettyCompact` (it produces compact human-readable tables).

## Implementation Details {#implementation-details}

When using the command-line client, data is always passed over the network in an internal efficient format (`Native`). The client independently interprets the `FORMAT` clause of the query and formats the data itself (thus relieving the network and the server from the extra load).
)DOCS_MD",
        .syntax = R"(
SELECT ... FORMAT format
)",
        .parent = "SELECT",
        .related = {"SELECT", "INTO OUTFILE", "INSERT INTO"},
    });

    factory.registerStatement("INTO OUTFILE",
    {
        .description = R"DOCS_MD(
`INTO OUTFILE` clause redirects the result of a `SELECT` query to a file on the **client** side.

Compressed files are supported. Compression type is detected by the extension of the file name (mode `'auto'` is used by default). Or it can be explicitly specified in a `COMPRESSION` clause. The compression level for a certain compression type can be specified in a `LEVEL` clause.

**Syntax**

```sql
SELECT <expr_list> INTO OUTFILE file_name [AND STDOUT] [APPEND | TRUNCATE] [COMPRESSION type [LEVEL level]]
```

`file_name` and `type` are string literals. Supported compression types are: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`, `'snappy'`. For `snappy`, the wire format is selected by the [snappy_mode](/reference/settings/session-settings/other#snappy_mode) setting (`basic` by default).

`level` is a numeric literal. Positive integers in following ranges are supported: `1-12` for `gzip`, `deflate` and `lz4` types, `1-22` for `zstd` type and `1-9` for other compression types. For `gzip` and `deflate`, levels above `9` require the default build with `libdeflate`; a build without `libdeflate` supports levels `1-9`.

## Implementation Details {#implementation-details}

- This functionality is available in the [command-line client](/concepts/features/interfaces/client) and [clickhouse-local](/concepts/features/tools-and-utilities/clickhouse-local). Thus a query sent via [HTTP interface](/concepts/features/interfaces/http) will fail.
- The query will fail if a file with the same file name already exists.
- The default [output format](/reference/formats/index) is `TabSeparated` (like in the command-line client batch mode). Use [FORMAT](/reference/statements/select/format) clause to change it.
- If `AND STDOUT` is mentioned in the query then the output that is written to the file is also displayed on standard output. If used with compression, the plaintext is displayed on standard output.
- If `APPEND` is mentioned in the query then the output is appended to an existing file. If compression is used, append cannot be used.
- When writing to a file that already exists, `APPEND` or `TRUNCATE` must be used.

**Example**

Execute the following query using [command-line client](/concepts/features/interfaces/client):

```bash title="Query"
clickhouse-client --query="SELECT 1,'ABC' INTO OUTFILE 'select.gz' FORMAT CSV;"
zcat select.gz
```

```text title="Response"
1,"ABC"
```
)DOCS_MD",
        .syntax = R"(
SELECT <expr_list> INTO OUTFILE file_name [AND STDOUT] [APPEND | TRUNCATE] [COMPRESSION type [LEVEL level]]
)",
        .parent = "SELECT",
        .related = {"SELECT", "FORMAT", "INSERT INTO"},
    });
}

}
