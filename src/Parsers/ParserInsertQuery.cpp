#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTQueryWithOutput.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserWithElement.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/InsertQuerySettingsPushDownVisitor.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <unordered_map>
#include <string_view>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


namespace
{

/// Whether the SELECT of an INSERT ... SELECT reads inline data through the `input` table function.
/// Only in that case does an INSERT with a SELECT carry inline data following the FORMAT clause.
bool selectReadsInlineDataViaInputFunction(const ASTPtr & ast)
{
    if (!ast)
        return false;
    if (const auto * function = ast->as<ASTFunction>(); function && function->name == "input")
        return true;
    for (const auto & child : ast->children)
        if (selectReadsInlineDataViaInputFunction(child))
            return true;
    return false;
}

}


bool ParserInsertQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// Create parsers
    ParserKeyword s_insert_into(Keyword::INSERT_INTO);
    ParserKeyword s_from_infile(Keyword::FROM_INFILE);
    ParserKeyword s_compression(Keyword::COMPRESSION);
    ParserKeyword s_table(Keyword::TABLE);
    ParserKeyword s_function(Keyword::FUNCTION);
    ParserToken s_dot(TokenType::Dot);
    ParserKeyword s_values(Keyword::VALUES);
    ParserKeyword s_format(Keyword::FORMAT);
    ParserKeyword s_settings(Keyword::SETTINGS);
    ParserKeyword s_returning(Keyword::RETURNING);
    ParserKeyword s_select(Keyword::SELECT);
    ParserKeyword s_from(Keyword::FROM);
    ParserKeyword s_partition_by(Keyword::PARTITION_BY);
    ParserKeyword s_with(Keyword::WITH);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserIdentifier name_p(true);
    ParserList columns_p(std::make_unique<ParserInsertElement>(), std::make_unique<ParserToken>(TokenType::Comma), false);
    ParserFunction table_function_p{false, true};
    ParserStringLiteral infile_name_p;
    ParserExpressionWithOptionalAlias exp_elem_p(false);

    /// create ASTPtr variables (result of parsing will be put in them).
    /// They will be used to initialize ASTInsertQuery's fields.
    ASTPtr database;
    ASTPtr table;
    ASTPtr infile;
    ASTPtr columns;
    ASTPtr format;
    ASTPtr select;
    ASTPtr table_function;
    ASTPtr settings_ast;
    ASTPtr source_select_settings_ast;
    ASTPtr source_select_pre_returning_settings_ast;
    ASTPtr source_select_settings_runtime_ast;
    ASTPtr source_select_settings_global_ast;
    ASTPtr returning_select;
    ASTPtr partition_by_expr;
    ASTPtr compression;
    ASTPtr with_expression_list;

    /// Insertion data
    const char * data = nullptr;

    if (s_with.ignore(pos, expected))
    {
        if (!ParserList(std::make_unique<ParserWithElement>(), std::make_unique<ParserToken>(TokenType::Comma))
            .parse(pos, with_expression_list, expected))
            return false;
        if (with_expression_list->children.empty())
            return false;
    }

    /// Check for key words `INSERT INTO`. If it isn't found, the query can't be parsed as insert query.
    if (!s_insert_into.ignore(pos, expected))
        return false;

    /// try to find 'TABLE'
    s_table.ignore(pos, expected);

    /// Search for 'FUNCTION'. If this key word is in query, read fields for insertion into 'TABLE FUNCTION'.
    /// Word table is optional for table functions. (for example, s3 table function)
    /// Otherwise fill 'TABLE' fields.
    if (s_function.ignore(pos, expected))
    {
        /// Read function name
        if (!table_function_p.parse(pos, table_function, expected))
            return false;

        /// Support insertion values with partition by.
        if (s_partition_by.ignore(pos, expected))
        {
            if (!exp_elem_p.parse(pos, partition_by_expr, expected))
                return false;
        }
    }
    else
    {
        /// Read one word. It can be table or database name.
        if (!name_p.parse(pos, table, expected))
            return false;

        /// If there is a dot, previous name was database name,
        /// so read table name after dot.
        if (s_dot.ignore(pos, expected))
        {
            database = table;
            if (!name_p.parse(pos, table, expected))
                return false;
        }
    }

    Pos before_lparen = pos;

    /// Is there a list of columns
    if (s_lparen.ignore(pos, expected))
    {
        if (!columns_p.parse(pos, columns, expected))
        {
            /// Column list parsing failed entirely (e.g. "((SELECT ..." where the second '(' is not a valid column name).
            /// Rewind to before the '(' so it can be parsed as part of a SELECT query later.
            columns.reset();
            pos = before_lparen;
        }
        else
        {
            /// Optional trailing comma
            ParserToken(TokenType::Comma).ignore(pos);

            /// If this fails, we want to rewind to before the lparen so we can later check for (SELECT ...)
            if (!s_rparen.ignore(pos, expected))
            {
                columns.reset();
                pos = before_lparen;
            }
        }
    }

    /// Check if file is a source of data.
    if (s_from_infile.ignore(pos, expected))
    {
        /// Read file name to process it later
        if (!infile_name_p.parse(pos, infile, expected))
            return false;

        /// Check for 'COMPRESSION' parameter (optional)
        if (s_compression.ignore(pos, expected))
        {
            /// Read compression name. Create parser for this purpose.
            ParserStringLiteral compression_p;
            if (!compression_p.parse(pos, compression, expected))
                return false;
        }
    }

    /// Read SETTINGS if they are defined
    if (s_settings.ignore(pos, expected))
    {
        /// Settings are written like SET query, so parse them with ParserSetQuery
        ParserSetQuery parser_settings(true);
        if (!parser_settings.parse(pos, settings_ast, expected))
            return false;
    }

    auto try_parse_returning_subquery = [&]() -> bool
    {
        if (!s_returning.ignore(pos, expected))
            return false;

        if (!s_lparen.ignore(pos, expected))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Expected opening round bracket after RETURNING");

        ParserSelectWithUnionQuery select_p;
        if (!select_p.parse(pos, returning_select, expected))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Expected SELECT query in RETURNING clause");

        if (!s_rparen.ignore(pos, expected))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Expected closing round bracket after RETURNING subquery");

        return true;
    };

    String format_str;
    Pos before_values = pos;

    /// For INSERT VALUES and INSERT FORMAT, RETURNING must appear before the data clause.
    if (!infile)
        try_parse_returning_subquery();

    /// VALUES or FORMAT or SELECT or WITH.
    /// After FROM INFILE we expect FORMAT, SELECT, WITH or nothing.
    if (!infile && s_values.ignore(pos, expected))
    {
        /// If VALUES is defined in query, everything except setting will be parsed as data,
        /// and if values followed by semicolon, the data should be null.
        if (pos->type != TokenType::Semicolon)
            data = pos->begin;

        format_str = "Values";
    }
    else if (s_format.ignore(pos, expected))
    {
        /// If FORMAT is defined, read format name
        if (!name_p.parse(pos, format, expected))
            return false;

        tryGetIdentifierNameInto(format, format_str);
    }
    else if (s_select.ignore(pos, expected) || s_with.ignore(pos, expected) || s_from.ignore(pos, expected) || s_lparen.ignore(pos, expected))
    {
        /// If SELECT is defined (possibly in parentheses), return to position before select and parse
        /// rest of query as SELECT query. Parentheses are handled by ParserSelectWithUnionQuery.
        /// The query can also start with the FROM clause: INSERT INTO t2 FROM t1 |> WHERE x.
        /// Note that FROM INFILE was already parsed before, so FROM at this position starts a SELECT query.
        pos = before_values;
        returning_select.reset();
        ParserSelectWithUnionQuery select_p;
        select_p.parse(pos, select, expected);

        /// FORMAT section is expected if we have input() in SELECT part
        if (s_format.ignore(pos, expected) && !name_p.parse(pos, format, expected))
            return false;

        tryGetIdentifierNameInto(format, format_str);

        /// Some SELECT shapes (notably certain set-op trees) may leave a source-level
        /// trailing SETTINGS right before RETURNING unconsumed by ParserSelectWithUnionQuery.
        if (s_settings.ignore(pos, expected))
        {
            ParserSetQuery parser_settings(true);
            if (!parser_settings.parse(pos, source_select_pre_returning_settings_ast, expected))
                return false;
        }

        /// For INSERT SELECT, RETURNING appears after the source SELECT / source SETTINGS.
        const bool has_returning = try_parse_returning_subquery();

        /// A query-level SETTINGS clause normally trails the source SELECT and is absorbed by
        /// `ParserSelectWithUnionQuery` as the SELECT's own settings. When RETURNING is present the
        /// SELECT parser stops at RETURNING, so the trailing `SETTINGS` (e.g. `parallel_distributed_insert_select`)
        /// would never be consumed. Parse it here and keep it separately from INSERT-level `settings_ast`:
        /// these settings still apply to the INSERT/source SELECT phase, but must not leak into RETURNING
        /// limits/context.
        if (has_returning && s_settings.ignore(pos, expected))
        {
            ParserSetQuery parser_settings(true);
            if (!parser_settings.parse(pos, source_select_settings_ast, expected))
                return false;
        }
    }
    else if (!infile)
    {
        /// If all previous conditions were false and it's not FROM INFILE, query is incorrect
        return false;
    }

    /// Read SETTINGS after FORMAT.
    ///
    /// Note, that part of SETTINGS can be interpreted as values,
    /// hence it is done only under option.
    ///
    /// Refs: https://github.com/ClickHouse/ClickHouse/issues/35100
    if (allow_settings_after_format_in_insert && s_settings.ignore(pos, expected))
    {
        if (settings_ast)
            throw Exception(ErrorCodes::SYNTAX_ERROR,
                            "You have SETTINGS before and after FORMAT, this is not allowed. "
                            "Consider switching to SETTINGS before FORMAT and disable allow_settings_after_format_in_insert.");

        /// Settings are written like SET query, so parse them with ParserSetQuery
        ParserSetQuery parser_settings(true);
        if (!parser_settings.parse(pos, settings_ast, expected))
            return false;
        /// In case of INSERT INTO ... VALUES SETTINGS ... (...), (...), ...
        /// we should move data pointer after all settings.
        if (data != nullptr)
            data = pos->begin;
    }

    auto propagate_with_clause = [&](ASTPtr & target_select, std::string_view target_name)
    {
        if (!with_expression_list || !target_select)
            return;

        auto propagate_impl = [&](auto && self, ASTPtr & current) -> void
        {
            if (!current)
                return;

            if (auto * select_with_union = current->as<ASTSelectWithUnionQuery>())
            {
                if (!select_with_union->list_of_selects)
                    return;
                for (auto & child : select_with_union->list_of_selects->children)
                    self(self, child);
                return;
            }

            if (auto * intersect_except = current->as<ASTSelectIntersectExceptQuery>())
            {
                auto children = intersect_except->getListOfSelects();
                for (auto & child : children)
                    self(self, child);
                return;
            }

            auto * child_select = current->as<ASTSelectQuery>();
            if (!child_select)
                return;

            if (child_select->getExpression(ASTSelectQuery::Expression::WITH, false))
                throw Exception(
                    ErrorCodes::SYNTAX_ERROR,
                    "Only one WITH should be presented, either before INSERT or {}.",
                    target_name);

            child_select->setExpression(ASTSelectQuery::Expression::WITH, with_expression_list->clone());
            /// WITH was appended after SELECT/TABLES; normalize back to canonical order.
            child_select->normalizeChildrenOrder();
        };

        propagate_impl(propagate_impl, target_select);
    };

    propagate_with_clause(select, "SELECT");
    propagate_with_clause(returning_select, "RETURNING subquery");

    if (select)
    {
        auto merge_settings_ast = [](ASTPtr & target_settings_ast, const ASTPtr & source_settings_ast)
        {
            if (!source_settings_ast)
                return;

            if (!target_settings_ast)
            {
                target_settings_ast = source_settings_ast->clone();
                return;
            }

            auto & source_settings = source_settings_ast->as<ASTSetQuery &>();
            auto & target_settings = target_settings_ast->as<ASTSetQuery &>();
            std::unordered_set<String> target_setting_names;
            target_setting_names.reserve(target_settings.changes.size() + target_settings.default_settings.size());

            for (const auto & change : target_settings.changes)
                target_setting_names.insert(change.name);
            for (const auto & default_setting : target_settings.default_settings)
                target_setting_names.insert(default_setting);

            for (const auto & change : source_settings.changes)
            {
                if (!target_setting_names.contains(change.name))
                {
                    target_settings.changes.push_back(change);
                    target_setting_names.insert(change.name);
                }
            }

            for (const auto & default_setting : source_settings.default_settings)
            {
                if (!target_setting_names.contains(default_setting))
                {
                    target_settings.default_settings.push_back(default_setting);
                    target_setting_names.insert(default_setting);
                }
            }
        };

        auto merge_settings_ast_with_override = [](ASTPtr & target_ast, const ASTPtr & source_ast)
        {
            if (!source_ast)
                return;

            if (!target_ast)
            {
                target_ast = source_ast->clone();
                return;
            }

            auto & source_settings = source_ast->as<ASTSetQuery &>();
            auto & target_settings = target_ast->as<ASTSetQuery &>();

            std::unordered_map<String, size_t> target_change_positions;
            target_change_positions.reserve(target_settings.changes.size());
            for (size_t i = 0; i < target_settings.changes.size(); ++i)
                target_change_positions[target_settings.changes[i].name] = i;

            std::unordered_map<String, size_t> target_default_positions;
            target_default_positions.reserve(target_settings.default_settings.size());
            for (size_t i = 0; i < target_settings.default_settings.size(); ++i)
                target_default_positions[target_settings.default_settings[i]] = i;

            auto erase_default = [&](const String & name)
            {
                auto it = target_default_positions.find(name);
                if (it == target_default_positions.end())
                    return;

                target_settings.default_settings.erase(target_settings.default_settings.begin() + it->second);
                target_default_positions.clear();
                for (size_t i = 0; i < target_settings.default_settings.size(); ++i)
                    target_default_positions[target_settings.default_settings[i]] = i;
            };

            auto erase_change = [&](const String & name)
            {
                auto it = target_change_positions.find(name);
                if (it == target_change_positions.end())
                    return;

                target_settings.changes.erase(target_settings.changes.begin() + it->second);
                target_change_positions.clear();
                for (size_t i = 0; i < target_settings.changes.size(); ++i)
                    target_change_positions[target_settings.changes[i].name] = i;
            };

            for (const auto & change : source_settings.changes)
            {
                erase_default(change.name);
                if (auto it = target_change_positions.find(change.name); it != target_change_positions.end())
                    target_settings.changes[it->second] = change;
                else
                {
                    target_settings.changes.push_back(change);
                    target_change_positions[change.name] = target_settings.changes.size() - 1;
                }
            }

            for (const auto & default_setting : source_settings.default_settings)
            {
                erase_change(default_setting);
                if (target_default_positions.contains(default_setting))
                    continue;

                target_settings.default_settings.push_back(default_setting);
                target_default_positions[default_setting] = target_settings.default_settings.size() - 1;
            }
        };

        auto collect_top_level_source_settings = [&](ASTPtr & target_settings_ast)
        {

            auto collect_top_level_impl = [&](auto && self, const ASTPtr & current, bool allow_subquery_unwrap) -> void
            {
                if (!current)
                    return;

                if (const auto * select_with_union = current->as<ASTSelectWithUnionQuery>())
                {
                    if (!select_with_union->list_of_selects)
                        return;

                    /// Match standalone SELECT precedence:
                    /// set-op level SETTINGS are applied first, then the last first-order arm overrides duplicates.
                    merge_settings_ast_with_override(target_settings_ast, select_with_union->settings_ast);
                    const auto & children = select_with_union->list_of_selects->children;
                    if (!children.empty())
                        self(self, children.back(), true);
                    return;
                }

                if (const auto * intersect_except = current->as<ASTSelectIntersectExceptQuery>())
                {
                    merge_settings_ast_with_override(target_settings_ast, intersect_except->settings());
                    auto children = intersect_except->getListOfSelects();
                    if (!children.empty())
                        self(self, children.back(), true);
                    return;
                }

                /// Keep `(SELECT ...)` equivalent to top-level SELECT while still avoiding traversal into
                /// arbitrary nested subqueries below the source root.
                if (allow_subquery_unwrap)
                {
                    if (const auto * subquery = current->as<ASTSubquery>())
                        self(self, subquery->children.empty() ? ASTPtr{} : subquery->children.front(), false);
                }

                if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(current.get()))
                    merge_settings_ast_with_override(target_settings_ast, query_with_output->settings_ast);

                if (const auto * select_query = current->as<ASTSelectQuery>())
                    merge_settings_ast_with_override(target_settings_ast, select_query->settings());
            };

            collect_top_level_impl(collect_top_level_impl, select, true);
        };

        if (returning_select)
        {
            if (source_select_pre_returning_settings_ast)
                source_select_settings_runtime_ast = source_select_pre_returning_settings_ast->clone();
            merge_settings_ast_with_override(source_select_settings_runtime_ast, source_select_settings_ast);

            /// For INSERT ... RETURNING we need recursive collection of source settings to reject unsupported
            /// query-global settings and to restore query settings before RETURNING planning.
            InsertQuerySettingsPushDownVisitor::Data visitor_data{source_select_settings_runtime_ast};
            InsertQuerySettingsPushDownVisitor(visitor_data).visit(select);

            ASTPtr source_top_level_settings_ast = source_select_pre_returning_settings_ast ? source_select_pre_returning_settings_ast->clone() : ASTPtr{};
            collect_top_level_source_settings(source_top_level_settings_ast);

            /// Trailing source SETTINGS after RETURNING must keep precedence over source SELECT settings.
            merge_settings_ast_with_override(source_top_level_settings_ast, source_select_settings_ast);
            source_select_settings_global_ast = source_top_level_settings_ast;
        }
        else
        {
            /// For plain INSERT ... SELECT keep historical top-level-only pushdown semantics:
            /// nested source subquery SETTINGS stay local and must not leak into outer planning/execution.
            ASTPtr source_top_level_settings_ast = source_select_pre_returning_settings_ast ? source_select_pre_returning_settings_ast->clone() : ASTPtr{};
            collect_top_level_source_settings(source_top_level_settings_ast);
            merge_settings_ast(settings_ast, source_top_level_settings_ast);
        }

    }

    /// In case of defined format, data follows it -- but only for inline-data INSERTs.
    /// An INSERT ... SELECT has no inline data (the rows come from the SELECT), unless the SELECT
    /// reads them through the `input` table function. Without `input`, anything after the FORMAT
    /// (including a `;` query terminator) is not insert data, so we must not look for it nor raise
    /// the "excessive ';'" error. This matters e.g. for `EXPLAIN ... INSERT ... SELECT ... FORMAT
    /// <name>;`, where the trailing FORMAT is the EXPLAIN output format, not an insert data format.
    if (format && !infile && (!select || selectReadsInlineDataViaInputFunction(select)))
    {
        Pos last_token = pos;
        --last_token;
        data = last_token->end;

        /// If format name is followed by ';' (end of query symbol) there is no data to insert.
        if (data < end && *data == ';')
            throw Exception(ErrorCodes::SYNTAX_ERROR, "You have excessive ';' symbol before data for INSERT.\n"
                                    "Example:\n\n"
                                    "INSERT INTO t (x, y) FORMAT TabSeparated\n"
                                    ";\tHello\n"
                                    "2\tWorld\n"
                                    "\n"
                                    "Note that there is no ';' just after format name, "
                                    "you need to put at least one whitespace symbol before the data.");

        while (data < end && (*data == ' ' || *data == '\t' || *data == '\f'))
            ++data;

        /// Data starts after the first newline, if there is one, or after all the whitespace characters, otherwise.

        if (data < end && *data == '\r')
            ++data;

        if (data < end && *data == '\n')
            ++data;
    }

    /// Create query and fill its fields.
    auto query = make_intrusive<ASTInsertQuery>();
    node = query;

    if (infile)
    {
        query->infile = infile;
        query->compression = compression;

        query->children.push_back(infile);
        if (compression)
            query->children.push_back(compression);
    }

    if (table_function)
    {
        query->table_function = table_function;
        query->partition_by = partition_by_expr;

        query->children.push_back(table_function);
        if (partition_by_expr)
            query->children.push_back(partition_by_expr);
    }
    else
    {
        query->database = database;
        query->table = table;

        if (database)
            query->children.push_back(database);
        if (table)
            query->children.push_back(table);
    }

    query->columns = columns;
    query->format = std::move(format_str);
    query->select = select;
    query->returning_select = returning_select;
    query->settings_ast = settings_ast;
    query->source_select_pre_returning_settings_ast = source_select_pre_returning_settings_ast;
    query->source_select_settings_ast = source_select_settings_ast;
    query->source_select_settings_runtime_ast = source_select_settings_runtime_ast;
    query->source_select_settings_global_ast = source_select_settings_global_ast;
    query->data = data != end ? data : nullptr;
    query->end = data ? end : nullptr;

    if (columns)
        query->children.push_back(columns);
    if (select)
        query->children.push_back(select);
    if (source_select_pre_returning_settings_ast)
        query->children.push_back(source_select_pre_returning_settings_ast);
    if (returning_select)
        query->children.push_back(returning_select);
    if (settings_ast)
        query->children.push_back(settings_ast);
    if (source_select_settings_ast)
        query->children.push_back(source_select_settings_ast);

    return true;
}

bool ParserInsertElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// ParserQualifiedColumnsMatcher must precede ParserCompoundIdentifier, which would otherwise
    /// consume the `<qualifier>.COLUMNS` prefix as a plain identifier and leave `(...)` unparsed.
    return ParserColumnsMatcher().parse(pos, node, expected)
        || ParserQualifiedAsterisk().parse(pos, node, expected)
        || ParserAsterisk().parse(pos, node, expected)
        || ParserQualifiedColumnsMatcher().parse(pos, node, expected)
        || ParserCompoundIdentifier().parse(pos, node, expected);
}

}

namespace DB
{

void registerStatementInsert(StatementFactory & factory)
{
    factory.registerStatement("INSERT INTO",
    {
        .description = R"DOCS_MD(
Inserts data into a table.

**Syntax**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] [SETTINGS ...] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

You can specify a list of columns to insert using  the `(c1, c2, c3)`. You can also use an expression with column [matcher](/reference/statements/select/index#asterisk) such as `*` and/or [modifiers](/reference/statements/select/index#select-modifiers) such as [APPLY](/reference/statements/select/apply_modifier), [EXCEPT](/reference/statements/select/except_modifier), [REPLACE](/reference/statements/select/replace_modifier).

For example, consider the table:

```sql
SHOW CREATE insert_select_testtable;
```

```text
CREATE TABLE insert_select_testtable
(
    `a` Int8,
    `b` String,
    `c` Int8
)
ENGINE = MergeTree()
ORDER BY a
```

```sql
INSERT INTO insert_select_testtable (*) VALUES (1, 'a', 1) ;
```

If you want to insert data into all of the columns, except column `b`, you can do so using the `EXCEPT` keyword. With reference to the syntax above, you will need to ensure that you insert as many values (`VALUES (v11, v13)`) as you specify columns (`(c1, c3)`) :

```sql
INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2);
```

```sql
SELECT * FROM insert_select_testtable;
```

```text
┌─a─┬─b─┬─c─┐
│ 2 │   │ 2 │
└───┴───┴───┘
┌─a─┬─b─┬─c─┐
│ 1 │ a │ 1 │
└───┴───┴───┘
```

In this example, we see that the second inserted row has `a` and `c` columns filled by the passed values, and `b` filled with value by default. It is also possible to use the `DEFAULT` keyword to insert default values:

```sql
INSERT INTO insert_select_testtable VALUES (1, DEFAULT, 1) ;
```

If a list of columns does not include all existing columns, the rest of the columns are filled with:

- The values calculated from the `DEFAULT` expressions specified in the table definition.
- Zeros and empty strings, if `DEFAULT` expressions are not defined.

Data can be passed to the INSERT in any [format](/reference/formats/index) supported by ClickHouse. The format must be specified explicitly in the query:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

For example, the following query format is identical to the basic version of `INSERT ... VALUES`:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse removes all spaces and one line feed (if there is one) before the data. When forming a query, we recommend putting the data on a new line after the query operators which is important if the data begins with spaces.

Example:

```sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

You can insert data separately from the query by using the [command-line client](/concepts/features/tools-and-utilities/clickhouse-local) or the [HTTP interface](/concepts/features/interfaces/http).

<Note>
If you want to specify `SETTINGS` for `INSERT` query then you have to do it _before_ the `FORMAT` clause since everything after `FORMAT format_name` is treated as data. For example:

```sql
INSERT INTO table SETTINGS ... FORMAT format_name data_set
```
</Note>

## Constraints {#constraints}

If a table has [constraints](/reference/statements/create/table#constraints), their expressions will be checked for each row of inserted data. If any of those constraints is not satisfied — the server will raise an exception containing the constraint name and expression, and the query will be stopped.

## Data Type Validation {#data-type-validation}

ClickHouse validates allowed data types (controlled by settings like `enable_time_time64_type`, `allow_suspicious_low_cardinality_types`, `allow_suspicious_fixed_string_types`, etc.) only during table creation (`CREATE TABLE`) and schema modification (`ALTER TABLE`), not during `INSERT`.

This means that if a table with a disallowed data type already exists, data can be inserted into it even when the corresponding setting is disabled on the server. This is by design — once a table is created, inserts should not be blocked by settings that control type creation.

For example:

```sql
SET enable_time_time64_type = 1;

CREATE TABLE events
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id;

SET enable_time_time64_type = 0;

-- This works even though the setting is now disabled.
-- The table already exists, so inserts are not blocked.
INSERT INTO events VALUES (1, '14:30:25');

-- But creating a new table with the Time type will fail.
CREATE TABLE events_new
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id; -- ERR: TYPE_TIME_TIME64_IS_NOT_ENABLED
```

<Note>
As a consequence, a client with a newer version (where a setting is enabled by default) can insert data with disallowed data types into a server with an older version (where the setting is disabled), as long as the target table already has the corresponding column types. The validation is enforced at the DDL level, not at the DML level.
</Note>

## Inserting the Results of SELECT {#inserting-the-results-of-select}

**Syntax**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] SELECT ...
```

Columns are mapped according to their position in the `SELECT` clause. However, their names in the `SELECT` expression and the table for `INSERT` may differ. If necessary, type casting is performed.

None of the data formats except the Values format allow setting values to expressions such as `now()`, `1 + 2`, and so on. The Values format allows limited use of expressions, but this is not recommended, because in this case inefficient code is used for their execution.

Other queries for modifying data parts are not supported: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
However, you can delete old data using `ALTER TABLE ... DROP PARTITION`.

The `FORMAT` clause must be specified at the end of the query if the `SELECT` clause contains the table function [input()](/reference/functions/table-functions/input).

To insert a default value instead of `NULL` into a column with a non-nullable data type, enable the [insert_null_as_default](/reference/settings/session-settings/insert#insert_null_as_default) setting.

`INSERT` also supports CTE (common table expression). For example, the following two statements are equivalent:

```sql
INSERT INTO x WITH y AS (SELECT * FROM numbers(10)) SELECT * FROM y;
WITH y AS (SELECT * FROM numbers(10)) INSERT INTO x SELECT * FROM y;
```

## Inserting Data from a File {#inserting-data-from-a-file}

**Syntax**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] FROM INFILE file_name [COMPRESSION type] [SETTINGS ...] [FORMAT format_name]
```

Use the syntax above to insert data from a file, or files, stored on the **client** side. `file_name` and `type` are string literals. Input file [format](/reference/formats/index) must be set in the `FORMAT` clause.

Compressed files are supported. The compression type is detected by the extension of the file name. Or it can be explicitly specified in a `COMPRESSION` clause. Supported types are: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`, `'snappy'`. For `snappy`, the wire format is selected by the [snappy_mode](/reference/settings/session-settings/other#snappy_mode) setting (`basic` by default).

This functionality is available in the [command-line client](/concepts/features/interfaces/client) and [clickhouse-local](/concepts/features/tools-and-utilities/clickhouse-local).

**Examples**

### Single file with FROM INFILE {#single-file-with-from-infile}

Execute the following queries using [command-line client](/concepts/features/interfaces/client):

```bash title="Query"
echo 1,A > input.csv ; echo 2,B >> input.csv
clickhouse-client --query="CREATE TABLE table_from_file (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO table_from_file FROM INFILE 'input.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM table_from_file FORMAT PrettyCompact;"
```

```text title="Response"
┌─id─┬─text─┐
│  1 │ A    │
│  2 │ B    │
└────┴──────┘
```

### Multiple files with FROM INFILE using globs {#multiple-files-with-from-infile-using-globs}

This example is very similar to the previous one but inserts are performed from multiple files using `FROM INFILE 'input_*.csv`.

```bash
echo 1,A > input_1.csv ; echo 2,B > input_2.csv
clickhouse-client --query="CREATE TABLE infile_globs (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM infile_globs FORMAT PrettyCompact;"
```

<Tip>
In addition to selecting multiple files with `*`, you can use ranges (`{1,2}` or `{1..9}`) and other [glob substitutions](/reference/functions/table-functions/file#globs-in-path). These three all would work with the example above:

```sql
INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_{1,2}.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_?.csv' FORMAT CSV;
```
</Tip>

## Inserting using a Table Function {#inserting-using-a-table-function}

Data can be inserted into tables referenced by [table functions](/reference/functions/table-functions/index).

**Syntax**

```sql
INSERT INTO [TABLE] FUNCTION table_func ...
```

**Example**

The [remote](/reference/functions/table-functions/remote) table function is used in the following queries:

```sql title="Query"
CREATE TABLE simple_table (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;
INSERT INTO TABLE FUNCTION remote('localhost', default.simple_table)
    VALUES (100, 'inserted via remote()');
SELECT * FROM simple_table;
```

```text title="Response"
┌──id─┬─text──────────────────┐
│ 100 │ inserted via remote() │
└─────┴───────────────────────┘
```

## Inserting into ClickHouse Cloud {#inserting-into-clickhouse-cloud}

By default, services on ClickHouse Cloud provide multiple replicas for high availability. When you connect to a service, a connection is established to one of these replicas.

After an `INSERT` succeeds, data is written to the underlying storage. However, it may take some time for replicas to receive these updates. Therefore, if you use a different connection that executes a `SELECT` query on one of these other replicas, the updated data may not yet be reflected.

It is possible to use the `select_sequential_consistency` to force the replica to receive the latest updates. Here is an example of a `SELECT` query using this setting:

```sql
SELECT .... SETTINGS select_sequential_consistency = 1;
```

Note that using `select_sequential_consistency` will increase the load on ClickHouse Keeper (used by ClickHouse Cloud internally) and may result in slower performance depending on the load on the service. We recommend against enabling this setting unless necessary. The recommended approach is to execute read/writes in the same session or to use a client driver that uses the native protocol (and thus supports sticky connections).

## Inserting into a replicated setup {#inserting-into-a-replicated-setup}

In a replicated setup, data will be visible on other replicas after it has been replicated. Data begins being replicated (downloaded on other replicas) immediately after an `INSERT`. This differs from ClickHouse Cloud, where data is immediately written to shared storage and replicas subscribe to metadata changes.

Note that for replicated setups, `INSERTs` can sometimes take a considerable amount of time (in the order of one second) as it requires committing to ClickHouse Keeper for distributed consensus. Using S3 for storage also adds additional latency.

## Performance Considerations {#performance-considerations}

`INSERT` sorts the input data by primary key and splits them into partitions by a partition key. If you insert data into several partitions at once, it can significantly reduce the performance of the `INSERT` query. To avoid this:

- Add data in fairly large batches, such as 100,000 rows at a time.
- Group data by a partition key before uploading it to ClickHouse.

Performance will not decrease if:

- Data is added in real time.
- You upload data that is usually sorted by time.

### Asynchronous inserts {#asynchronous-inserts}

It is possible to asynchronously insert data in small but frequent inserts. The data from such insertions is combined into batches and then safely inserted into a table. To use asynchronous inserts, enable the [`async_insert`](/reference/settings/session-settings/async-insert#async_insert) setting.

Using `async_insert` or the [`Buffer` table engine](/reference/engines/table-engines/special/buffer) results in additional buffering.

### Large or long-running inserts {#large-or-long-running-inserts}

When you are inserting large amounts of data, ClickHouse will optimize write performance through a process called "squashing". Small blocks of inserted data in memory are merged and squashed into larger blocks before being written to disk. Squashing reduces the overhead associated with each write operation. In this process, inserted data will be available to query after ClickHouse completes writing each [`max_insert_block_size`](/reference/settings/session-settings/max-insert#max_insert_block_size) rows.

**See Also**

- [async_insert](/reference/settings/session-settings/async-insert#async_insert)
- [wait_for_async_insert](/reference/settings/session-settings/wait-for#wait_for_async_insert)
- [wait_for_async_insert_timeout](/reference/settings/session-settings/wait-for#wait_for_async_insert_timeout)
- [async_insert_max_data_size](/reference/settings/session-settings/async-insert#async_insert_max_data_size)
- [async_insert_busy_timeout_ms](/reference/settings/session-settings/async-insert#async_insert_busy_timeout_max_ms)
- `async_insert_stale_timeout_ms`
)DOCS_MD",
        .syntax = R"(
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] [SETTINGS ...] VALUES (v11, v12, v13), (v21, v22, v23), ...
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] [SETTINGS ...] FORMAT format_name data_set
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] [SETTINGS ...] SELECT ...
INSERT INTO [TABLE] FUNCTION table_func(...) [(c1, c2, c3)] [SETTINGS ...] SELECT ...
)",
        .related = {"SELECT", "FORMAT", "CREATE TABLE", "UPDATE", "DELETE"},
    });
}

}
