#include <Interpreters/SystemLogUnionTable.h>

#include <Common/StringUtils.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
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

/// The log table that `function` reads from, if it is the `merge` table function over a log table and its
/// rotated versions, exactly as `SystemLog::getCreateUnionTableQuery` builds it.
std::optional<StorageID> getSystemLogOfGeneratedMergeFunction(const ASTFunction & function)
{
    if (function.name != "merge")
        return std::nullopt;

    const auto * arguments = function.arguments ? function.arguments->as<ASTExpressionList>() : nullptr;
    if (!arguments || arguments->children.size() != 2)
        return std::nullopt;

    auto database_name = getStringLiteral(arguments->children[0].get());
    auto regexp = getStringLiteral(arguments->children[1].get());
    if (!database_name || !regexp)
        return std::nullopt;

    auto table_name = getLogTableNameFromRotatedLogTablesRegexp(*regexp);
    if (!table_name)
        return std::nullopt;

    return StorageID(*database_name, *table_name);
}

/// The log table that `function` reads from, if it is one of the table functions that
/// `SystemLog::getCreateUnionTableQuery` generates: the `merge` over the log table and its rotated versions,
/// that same `merge` wrapped into `clusterAllReplicas`, or `clusterAllReplicas` over the log table alone.
std::optional<StorageID> getSystemLogOfGeneratedUnionTableFunction(const ASTFunction & function)
{
    if (auto log_table_id = getSystemLogOfGeneratedMergeFunction(function))
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
    if (positional_arguments.size() < 2 || !positional_arguments[0]->as<ASTLiteral>())
        return std::nullopt;

    if (positional_arguments.size() == 2)
    {
        const auto * inner_function = positional_arguments[1]->as<ASTFunction>();
        if (!inner_function)
            return std::nullopt;
        return getSystemLogOfGeneratedMergeFunction(*inner_function);
    }

    if (positional_arguments.size() == 3)
    {
        auto database_name = getStringLiteral(positional_arguments[1].get());
        auto table_name = getStringLiteral(positional_arguments[2].get());
        if (!database_name || !table_name || table_name->empty())
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

std::optional<StorageID> getSystemLogOfGeneratedUnionTable(const ASTCreateQuery & create_query)
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

    return getSystemLogOfGeneratedUnionTableFunction(*table_function);
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
