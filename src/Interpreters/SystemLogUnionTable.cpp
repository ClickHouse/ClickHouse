#include <Interpreters/SystemLogUnionTable.h>

#include <Common/StringUtils.h>
#include <Core/QualifiedTableName.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>

#include <fmt/format.h>

namespace DB
{

namespace
{

/// Escapes a table name for use inside a regular expression
/// (the argument of the `merge` table function).
String escapeStringForRegexp(const String & s)
{
    String result;
    result.reserve(s.size());
    for (char c : s)
    {
        if (!isWordCharASCII(c))
            result += '\\';
        result += c;
    }
    return result;
}

/// The table name that `getRotatedLogTablesRegexp` built `regexp` from, if it has that exact form.
std::optional<String> getLogTableNameFromRotatedLogTablesRegexp(const String & regexp)
{
    static constexpr std::string_view prefix = "^";
    static constexpr std::string_view suffix = "(_[0-9]+)?$";

    if (regexp.size() <= prefix.size() + suffix.size() || !regexp.starts_with(prefix) || !regexp.ends_with(suffix))
        return std::nullopt;

    std::string_view escaped(regexp.data() + prefix.size(), regexp.size() - prefix.size() - suffix.size());
    String name;
    for (size_t i = 0; i < escaped.size(); ++i)
    {
        if (escaped[i] == '\\')
        {
            ++i;
            if (i == escaped.size())
                return std::nullopt;
        }
        name += escaped[i];
    }

    /// Anything that escapes differently, such as an escaped word character, is not a generated pattern.
    if (getRotatedLogTablesRegexp(StorageID(String{}, name)) != regexp)
        return std::nullopt;

    return name;
}

/// The string held by `ast`, if it is a string literal.
std::optional<String> getStringLiteral(const IAST * ast)
{
    const auto * literal = ast ? ast->as<ASTLiteral>() : nullptr;
    if (!literal || literal->value.getType() != Field::Types::String)
        return std::nullopt;
    return literal->value.safeGet<String>();
}

/// The name that `ast` spells, if it is a string literal or a bare identifier: the table functions accept
/// their database, table and cluster names in both forms (`merge(system, ...)` and `merge('system', ...)`).
/// A stored definition always holds the literal form, and so does the query that `CREATE OR REPLACE`
/// checks, because the table function has already rewritten its arguments into literals by then (see the
/// call to `getSystemLogOfGeneratedUnionTable` in `InterpreterCreateQuery::doCreateOrReplaceTable`); the
/// identifier form is accepted so that the rule does not depend on that order of events.
std::optional<String> getNameArgument(const IAST * ast)
{
    if (auto name = getStringLiteral(ast))
        return name;
    return tryGetIdentifierName(ast);
}

/// Whether `ast` still needs `context` to be read as a name: anything but a literal or a bare identifier.
bool isNameExpression(const ASTPtr & ast)
{
    return !ast->as<ASTLiteral>() && !ast->as<ASTIdentifier>();
}

/// The log table that `function` reads from, if it is the `merge` table function over a log table and its
/// rotated versions, exactly as `SystemLog::getCreateUnionTableQuery` builds it. With `context`, the arguments
/// may be any constant expressions, which `TableFunctionMerge::parseArguments` evaluates the same way; they
/// are left as written when the `merge` is nested into `clusterAllReplicas`, which evaluates it remotely.
std::optional<StorageID> getSystemLogOfGeneratedMergeFunction(const ASTFunction & function, ContextPtr context)
{
    if (function.name != "merge")
        return std::nullopt;

    const auto * arguments = function.arguments ? function.arguments->as<ASTExpressionList>() : nullptr;
    if (!arguments || arguments->children.size() != 2)
        return std::nullopt;

    ASTPtr database_ast = arguments->children[0];
    ASTPtr regexp_ast = arguments->children[1];
    if (context && isNameExpression(database_ast))
        database_ast = evaluateConstantExpressionForDatabaseName(database_ast, context);
    if (context && isNameExpression(regexp_ast))
        regexp_ast = evaluateConstantExpressionAsLiteral(regexp_ast, context);

    auto database_name = getNameArgument(database_ast.get());
    auto regexp = getStringLiteral(regexp_ast.get());
    if (!database_name || database_name->empty() || !regexp)
        return std::nullopt;

    auto table_name = getLogTableNameFromRotatedLogTablesRegexp(*regexp);
    if (!table_name)
        return std::nullopt;

    return StorageID(*database_name, *table_name);
}

/// The log table that `function` reads from, if it is one of the table functions that
/// `SystemLog::getCreateUnionTableQuery` generates: the `merge` over the log table and its rotated versions,
/// that same `merge` wrapped into `clusterAllReplicas`, or `clusterAllReplicas` over the log table alone.
std::optional<StorageID> getSystemLogOfGeneratedUnionTableFunction(const ASTFunction & function, ContextPtr context)
{
    if (auto log_table_id = getSystemLogOfGeneratedMergeFunction(function, context))
        return log_table_id;

    if (function.name != "clusterAllReplicas")
        return std::nullopt;

    const auto * arguments = function.arguments ? function.arguments->as<ASTExpressionList>() : nullptr;
    if (!arguments)
        return std::nullopt;

    /// The generated definition also carries a `SETTINGS` clause, which is not a positional argument.
    ASTs positional_arguments;
    for (const auto & argument : arguments->children)
        if (!argument->as<ASTSetQuery>())
            positional_arguments.push_back(argument);

    /// The first argument is the cluster name.
    if (positional_arguments.size() < 2 || !getNameArgument(positional_arguments[0].get()))
        return std::nullopt;

    if (positional_arguments.size() == 2)
    {
        if (const auto * inner_function = positional_arguments[1]->as<ASTFunction>())
            return getSystemLogOfGeneratedMergeFunction(*inner_function, context);

        /// The log table spelled as one qualified name, `system.query_log` or `'system.query_log'`: the
        /// table function accepts it in place of the separate database and table arguments, and stores
        /// the definition of `clusterAllReplicas(cluster, system.query_log)` in exactly that form.
        auto qualified_name = getNameArgument(positional_arguments[1].get());
        if (!qualified_name)
            return std::nullopt;
        auto parsed = QualifiedTableName::tryParseFromString(*qualified_name);
        if (!parsed || parsed->database.empty() || parsed->table.empty())
            return std::nullopt;
        return StorageID(parsed->database, parsed->table);
    }

    if (positional_arguments.size() == 3)
    {
        auto database_name = getNameArgument(positional_arguments[1].get());
        auto table_name = getNameArgument(positional_arguments[2].get());
        if (!database_name || database_name->empty() || !table_name || table_name->empty())
            return std::nullopt;
        return StorageID(*database_name, *table_name);
    }

    return std::nullopt;
}

}

String getRotatedLogTablesRegexp(const StorageID & log_table_id)
{
    return fmt::format("^{}(_[0-9]+)?$", escapeStringForRegexp(log_table_id.table_name));
}

std::optional<StorageID> getSystemLogOfGeneratedUnionTable(const ASTCreateQuery & create_query, ContextPtr context)
{
    const auto * table_function = create_query.as_table_function ? create_query.as_table_function->as<ASTFunction>() : nullptr;
    if (!table_function)
        return std::nullopt;

    /// The table function alone does not tell a generated table from a user's proxy over the same function, and
    /// the comment alone does not either. A user table matches both only if the user copied the generated
    /// definition, comment included, and that promise was then made deliberately.
    auto comment = getStringLiteral(create_query.comment);
    if (!comment || !comment->ends_with(SYSTEM_LOG_UNION_TABLE_COMMENT_MARKER))
        return std::nullopt;

    return getSystemLogOfGeneratedUnionTableFunction(*table_function, context);
}

bool isGeneratedUnionTable(const ASTPtr & create_query_ast, const StorageID & log_table_id)
{
    const auto * create_query = create_query_ast->as<ASTCreateQuery>();
    if (!create_query)
        return false;

    auto generated_for = getSystemLogOfGeneratedUnionTable(*create_query);
    return generated_for && generated_for->database_name == log_table_id.database_name
        && generated_for->table_name == log_table_id.table_name;
}

}
