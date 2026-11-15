#include <Interpreters/ExpressionContainsArrayJoin.h>

#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>

#include <Common/UnorderedSetWithMemoryTracking.h>

#include <base/types.h>

namespace DB
{

namespace
{

bool expressionContainsArrayJoinImpl(const IAST & ast, UnorderedSetWithMemoryTracking<String> & visited_udfs)
{
    if (const auto * function = ast.as<ASTFunction>())
    {
        if (getFunctionCanonicalNameIfAny(function->name) == "arrayJoin")
            return true;

        /// Each body is walked at most once, so that a cycle among them cannot make this recurse forever.
        auto udf_body = UserDefinedSQLFunctionFactory::instance().tryGet(function->name);
        if (udf_body && visited_udfs.insert(function->name).second
            && expressionContainsArrayJoinImpl(*udf_body, visited_udfs))
            return true;
    }

    for (const auto & child : ast.children)
    {
        if (!child->as<ASTSelectQuery>() && expressionContainsArrayJoinImpl(*child, visited_udfs))
            return true;
    }

    return false;
}

}

bool expressionContainsArrayJoin(const IAST & ast)
{
    UnorderedSetWithMemoryTracking<String> visited_udfs;
    return expressionContainsArrayJoinImpl(ast, visited_udfs);
}

bool expressionContainsArrayJoin(const ASTPtr & ast)
{
    return ast && expressionContainsArrayJoin(*ast);
}

}
