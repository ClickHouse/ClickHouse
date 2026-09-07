#include <Interpreters/ExpressionContainsArrayJoin.h>

#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>

#include <unordered_set>

namespace DB
{

namespace
{
    bool expressionContainsArrayJoinImpl(const ASTPtr & ast, std::unordered_set<String> & visited_udfs)
    {
        if (!ast)
            return false;
        if (const auto * function = ast->as<ASTFunction>())
        {
            if (getFunctionCanonicalNameIfAny(function->name) == "arrayJoin")
                return true;
            if (auto udf_body = UserDefinedSQLFunctionFactory::instance().tryGet(function->name);
                udf_body && visited_udfs.insert(function->name).second
                    && expressionContainsArrayJoinImpl(udf_body, visited_udfs))
                return true;
        }
        for (const auto & child : ast->children)
        {
            if (!child->as<ASTSelectQuery>() && expressionContainsArrayJoinImpl(child, visited_udfs))
                return true;
        }
        return false;
    }
}

bool expressionContainsArrayJoin(const ASTPtr & ast)
{
    std::unordered_set<String> visited_udfs;
    return expressionContainsArrayJoinImpl(ast, visited_udfs);
}

}
