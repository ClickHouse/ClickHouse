#include <Interpreters/ExpressionContainsColumnMatcher.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>

#include <Common/UnorderedSetWithMemoryTracking.h>

#include <base/types.h>

namespace DB
{

namespace
{

const IAST * getMatcherQualifier(const IAST & ast)
{
    if (const auto * matcher = ast.as<ASTQualifiedAsterisk>())
        return matcher->qualifier.get();
    if (const auto * matcher = ast.as<ASTQualifiedColumnsRegexpMatcher>())
        return matcher->qualifier.get();
    if (const auto * matcher = ast.as<ASTQualifiedColumnsListMatcher>())
        return matcher->qualifier.get();
    return nullptr;
}

bool isColumnMatcher(const IAST & ast, const IsColumnQualifier & is_column_qualifier)
{
    if (ast.as<ASTAsterisk>() || ast.as<ASTColumnsRegexpMatcher>() || ast.as<ASTColumnsListMatcher>())
        return true;

    const auto * qualifier = getMatcherQualifier(ast);
    if (!qualifier)
        return false;

    const auto * qualifier_identifier = qualifier->as<ASTIdentifier>();
    return !qualifier_identifier || !is_column_qualifier(*qualifier_identifier);
}

const IAST * findColumnMatcherInExpressionImpl(
    const IAST & ast, const IsColumnQualifier & is_column_qualifier, UnorderedSetWithMemoryTracking<String> & visited_udfs)
{
    if (isColumnMatcher(ast, is_column_qualifier))
        return &ast;

    if (const auto * function = ast.as<ASTFunction>())
    {
        /// Each body is walked at most once, so that a cycle among them cannot make this recurse forever.
        auto udf_body = UserDefinedSQLFunctionFactory::instance().tryGet(function->name);
        if (udf_body && visited_udfs.insert(function->name).second)
        {
            if (const auto * matcher = findColumnMatcherInExpressionImpl(*udf_body, is_column_qualifier, visited_udfs))
                return matcher;
        }
    }

    for (const auto & child : ast.children)
    {
        if (child->as<ASTSelectQuery>())
            continue;
        if (const auto * matcher = findColumnMatcherInExpressionImpl(*child, is_column_qualifier, visited_udfs))
            return matcher;
    }

    return nullptr;
}

}

const IAST * findColumnMatcherInExpression(const IAST & ast, const IsColumnQualifier & is_column_qualifier)
{
    UnorderedSetWithMemoryTracking<String> visited_udfs;
    return findColumnMatcherInExpressionImpl(ast, is_column_qualifier, visited_udfs);
}

IsColumnQualifier makeTupleColumnQualifierCheck(
    std::function<std::optional<NameAndTypePair>(const String &)> try_get_column_,
    NameSet table_names_,
    std::optional<std::pair<String, String>> database_and_table_name_,
    bool prefer_table_over_column)
{
    return [try_get_column = std::move(try_get_column_),
            table_names = std::move(table_names_),
            database_and_table_name = std::move(database_and_table_name_),
            prefer_table_over_column](const ASTIdentifier & qualifier)
    {
        /// The parts are compared one by one rather than the flattened name, because a quoted part may contain a dot,
        /// e.g. `tab.with.dots`.tup is a single-part table prefix followed by a column.
        const auto & parts = qualifier.name_parts;
        if (parts.empty())
            return false;

        auto join_parts = [&](size_t start)
        {
            String result;
            for (size_t i = start; i < parts.size(); ++i)
            {
                if (i != start)
                    result += '.';
                result += parts[i];
            }
            return result;
        };

        bool has_single_part_prefix = table_names.contains(parts[0]);
        bool has_database_and_table_prefix = database_and_table_name && parts.size() >= 2
            && parts[0] == database_and_table_name->first && parts[1] == database_and_table_name->second;

        if (prefer_table_over_column
            && ((parts.size() == 1 && has_single_part_prefix) || (parts.size() == 2 && has_database_and_table_prefix)))
            return false;

        auto try_get_column_without_single_part_prefix = [&]() -> std::optional<NameAndTypePair>
        {
            if (parts.size() > 1 && has_single_part_prefix)
                return try_get_column(join_parts(1));
            return {};
        };

        auto try_get_column_without_database_and_table_prefix = [&]() -> std::optional<NameAndTypePair>
        {
            if (parts.size() > 2 && has_database_and_table_prefix)
                return try_get_column(join_parts(2));
            return {};
        };

        /// The qualifier is resolved as an identifier in the scope of the table, so it may be the name of a column,
        /// e.g. `tup` or a nested `tup.inner`, or the name of a column qualified with the table, e.g. `t.tup`.
        /// Like `IdentifierResolver::tryResolveIdentifierFromTableExpression`, with
        /// `analyzer_compatibility_prefer_alias_over_subcolumn` a qualifier whose first part is the table name or
        /// the alias, e.g. `t.a`, is looked up without that part first, and only then as a whole.
        /// Like `IdentifierResolver`, a single-part prefix (the table name or the alias) is always tried before
        /// a `database.table` prefix, e.g. for `db.tbl.tup.*` over `db.tbl AS db` the column `tbl.tup` is tried
        /// before `tup`.
        std::optional<NameAndTypePair> column;
        if (prefer_table_over_column)
            column = try_get_column_without_single_part_prefix();
        if (!column)
            column = try_get_column(join_parts(0));
        if (!column && !prefer_table_over_column)
            column = try_get_column_without_single_part_prefix();
        if (!column)
            column = try_get_column_without_database_and_table_prefix();
        if (!column)
            return false;

        DataTypePtr type = column->type;
        while (true)
        {
            if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
                type = nullable_type->getNestedType();
            else if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
                type = array_type->getNestedType();
            else if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
                type = map_type->getNestedType();
            else
                break;
        }

        return typeid_cast<const DataTypeTuple *>(type.get()) != nullptr;
    };
}

}
