#include <Interpreters/ExpressionContainsColumnMatcher.h>

#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>

#include <Common/UnorderedSetWithMemoryTracking.h>

#include <base/types.h>

namespace DB
{

namespace
{

bool isColumnMatcher(const IAST & ast)
{
    return ast.as<ASTAsterisk>() || ast.as<ASTQualifiedAsterisk>() || ast.as<ASTColumnsRegexpMatcher>()
        || ast.as<ASTColumnsListMatcher>() || ast.as<ASTQualifiedColumnsRegexpMatcher>() || ast.as<ASTQualifiedColumnsListMatcher>();
}

const IAST * findColumnMatcherInExpressionImpl(const IAST & ast, UnorderedSetWithMemoryTracking<String> & visited_udfs)
{
    if (isColumnMatcher(ast))
        return &ast;

    if (const auto * function = ast.as<ASTFunction>())
    {
        /// Each body is walked at most once, so that a cycle among them cannot make this recurse forever.
        auto udf_body = UserDefinedSQLFunctionFactory::instance().tryGet(function->name);
        if (udf_body && visited_udfs.insert(function->name).second)
        {
            if (const auto * matcher = findColumnMatcherInExpressionImpl(*udf_body, visited_udfs))
                return matcher;
        }
    }

    for (const auto & child : ast.children)
    {
        if (child->as<ASTSelectQuery>())
            continue;
        if (const auto * matcher = findColumnMatcherInExpressionImpl(*child, visited_udfs))
            return matcher;
    }

    return nullptr;
}

}

const IAST * findColumnMatcherInExpression(const IAST & ast)
{
    UnorderedSetWithMemoryTracking<String> visited_udfs;
    return findColumnMatcherInExpressionImpl(ast, visited_udfs);
}

}
