#include <Storages/TableNameOrQuery.h>

#include <Core/Field.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/IAST.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

const String & TableNameOrQuery::getTableName() const
{
    if (type != Type::TABLE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to get table name from TableNameOrQuery: {}", format());
    return target;
}

const String & TableNameOrQuery::getQuery() const
{
    if (type != Type::QUERY)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to get query string from TableNameOrQuery: {}", format());
    return target;
}

String TableNameOrQuery::format() const
{
    String prefix;
    switch (type)
    {
        case Type::TABLE:
            prefix = "Table: ";
            break;
        case Type::QUERY:
            prefix = "Query: ";
            break;
    }
    return prefix + target;
}

std::optional<String> tryGetExternalDatabaseQuery(
    const ASTPtr & argument,
    const ContextPtr & context,
    IdentifierQuotingStyle identifier_quoting_style,
    LiteralEscapingStyle literal_escaping_style)
{
    if (const auto * subquery = argument->as<ASTSubquery>())
    {
        /// The subquery is formatted back to SQL text in the dialect of the external database, so that
        /// identifiers that require quoting (e.g. mixed case or containing spaces) and string literals are
        /// emitted using the external database's quoting/escaping style rather than the ClickHouse one (which
        /// would, for example, produce backtick-quoted identifiers that PostgreSQL rejects). `WhenNecessary`
        /// keeps simple identifiers unquoted, exactly as a hand-written query would be.
        WriteBufferFromOwnString out;
        IAST::FormatSettings settings(
            /*one_line=*/true,
            /*identifier_quoting_rule=*/IdentifierQuotingRule::WhenNecessary,
            /*identifier_quoting_style=*/identifier_quoting_style,
            /*show_secrets_=*/true,
            /*literal_escaping_style=*/literal_escaping_style);
        subquery->children.at(0)->format(out, settings);
        return out.str();
    }

    if (const auto * function = argument->as<ASTFunction>(); function && function->name == "query")
    {
        if (!function->arguments || function->arguments->children.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function 'query' for an external database expects exactly one argument");

        auto evaluated = evaluateConstantExpressionOrIdentifierAsLiteral(function->arguments->children[0], context);
        const auto * literal = evaluated->as<ASTLiteral>();
        if (!literal || literal->value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function 'query' for an external database expects a string literal argument");

        return literal->value.safeGet<String>();
    }

    return {};
}

static String quoteExternalIdentifier(const String & name, IdentifierQuotingStyle identifier_quoting_style)
{
    if (identifier_quoting_style == IdentifierQuotingStyle::BackticksMySQL)
        return backQuoteMySQL(name);
    if (identifier_quoting_style == IdentifierQuotingStyle::DoubleQuotes)
        return doubleQuoteString(name);
    return backQuote(name);
}

String buildQueryForExternalDatabaseSubquery(
    const String & query, const Names & columns, IdentifierQuotingStyle identifier_quoting_style)
{
    WriteBufferFromOwnString out;
    out << "SELECT ";
    if (columns.empty())
    {
        out << "*";
    }
    else
    {
        bool first = true;
        for (const auto & column : columns)
        {
            if (!first)
                out << ", ";
            first = false;
            out << quoteExternalIdentifier(column, identifier_quoting_style);
        }
    }
    out << " FROM (" << query << ") AS __subquery";
    return out.str();
}

}
