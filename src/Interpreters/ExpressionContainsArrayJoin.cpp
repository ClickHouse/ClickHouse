#include <Interpreters/ExpressionContainsArrayJoin.h>

#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>

#include <Common/UnorderedSetWithMemoryTracking.h>

#include <base/types.h>

namespace DB
{

namespace
{

bool expressionContainsArrayJoinImpl(const IAST & ast, UnorderedSetWithMemoryTracking<String> & visited_udfs);

bool nameIsOrHidesArrayJoin(const String & name, UnorderedSetWithMemoryTracking<String> & visited_udfs)
{
    if (getFunctionCanonicalNameIfAny(name) == "arrayJoin")
        return true;

    /// Each body is walked at most once, so that a cycle among them cannot make this recurse forever.
    auto udf_body = UserDefinedSQLFunctionFactory::instance().tryGet(name);
    return udf_body && visited_udfs.insert(name).second && expressionContainsArrayJoinImpl(*udf_body, visited_udfs);
}

bool expressionContainsArrayJoinImpl(const IAST & ast, UnorderedSetWithMemoryTracking<String> & visited_udfs)
{
    if (const auto * function = ast.as<ASTFunction>())
    {
        if (nameIsOrHidesArrayJoin(function->name, visited_udfs))
            return true;
    }

    /// `ASTColumnsApplyTransformer` keeps its lambda, its parameters and a bare function name
    /// outside `children`, so a call written inside an `APPLY` is not reached by the walk below.
    if (const auto * apply = ast.as<ASTColumnsApplyTransformer>())
    {
        if (!apply->func_name.empty() && nameIsOrHidesArrayJoin(apply->func_name, visited_udfs))
            return true;
        if (apply->lambda && expressionContainsArrayJoinImpl(*apply->lambda, visited_udfs))
            return true;
        if (apply->parameters && expressionContainsArrayJoinImpl(*apply->parameters, visited_udfs))
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
