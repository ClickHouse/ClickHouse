#include <Interpreters/astContainsStatefulFunction.h>

#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>

#include <unordered_set>

namespace DB
{

namespace
{

bool containsArrayJoinImpl(const ASTPtr & ast, std::unordered_set<String> & visited_udfs)
{
    if (!ast)
        return false;
    if (const auto * function = ast->as<ASTFunction>())
    {
        if (getFunctionCanonicalNameIfAny(function->name) == "arrayJoin")
            return true;
        /// A SQL user-defined function is inlined into the query before execution, so its body can
        /// hide an `arrayJoin` call (`CREATE FUNCTION explode AS a -> arrayJoin(a)`). Descend into
        /// the definition; the visited set protects from recursive definitions and rescans.
        /// Mirrors `expressionContainsArrayJoin` in `RowPolicy.cpp`.
        if (auto udf = UserDefinedSQLFunctionFactory::instance().tryGet(function->name);
            udf && visited_udfs.insert(function->name).second && containsArrayJoinImpl(udf, visited_udfs))
            return true;
    }
    for (const auto & child : ast->children)
        if (!child->as<ASTSelectQuery>() && containsArrayJoinImpl(child, visited_udfs))
            return true;
    return false;
}

bool containsStatefulImpl(const ASTPtr & ast, const ContextPtr & context, std::unordered_set<String> & visited_udfs)
{
    if (!ast)
        return false;
    if (const auto * function = ast->as<ASTFunction>())
    {
        const auto function_resolver = FunctionFactory::instance().tryGet(function->name, context);
        if (function_resolver && function_resolver->isStateful())
            return true;
        /// `FunctionFactory` only sees built-in functions; a SQL user-defined function
        /// (`CREATE FUNCTION f AS x -> neighbor(x, 1)`) is inlined into the query before
        /// execution, so descend into its body too (with protection from recursive definitions).
        if (auto udf = UserDefinedSQLFunctionFactory::instance().tryGet(function->name);
            udf && visited_udfs.insert(function->name).second && containsStatefulImpl(udf, context, visited_udfs))
            return true;
    }
    for (const auto & child : ast->children)
        if (!child->as<ASTSelectQuery>() && containsStatefulImpl(child, context, visited_udfs))
            return true;
    return false;
}

}

bool astContainsArrayJoinFunction(const ASTPtr & ast)
{
    std::unordered_set<String> visited_udfs;
    return containsArrayJoinImpl(ast, visited_udfs);
}

bool astContainsStatefulFunction(const ASTPtr & ast, const ContextPtr & context)
{
    std::unordered_set<String> visited_udfs;
    return containsStatefulImpl(ast, context, visited_udfs);
}

}
