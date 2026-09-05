#include <Functions/astContainsArrayJoin.h>

#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST.h>

#include <Common/UnorderedSetWithMemoryTracking.h>

#include <base/types.h>


namespace DB
{

namespace
{

bool astContainsArrayJoinImpl(const IAST & ast, bool descend_into_sql_udfs, UnorderedSetWithMemoryTracking<String> & visited_udfs)
{
    if (const auto * function = ast.as<ASTFunction>())
    {
        if (getFunctionCanonicalNameIfAny(function->name) == "arrayJoin")
            return true;

        if (descend_into_sql_udfs)
        {
            /// Each body is walked at most once, so that a cycle among them cannot make this recurse
            /// forever.
            auto udf = UserDefinedSQLFunctionFactory::instance().tryGet(function->name);
            if (udf && visited_udfs.insert(function->name).second
                && astContainsArrayJoinImpl(*udf, descend_into_sql_udfs, visited_udfs))
                return true;
        }
    }

    for (const auto & child : ast.children)
    {
        if (child->as<ASTSelectQuery>())
            continue;

        if (astContainsArrayJoinImpl(*child, descend_into_sql_udfs, visited_udfs))
            return true;
    }

    return false;
}

}

bool astContainsArrayJoin(const IAST & ast, bool descend_into_sql_udfs)
{
    UnorderedSetWithMemoryTracking<String> visited_udfs;
    return astContainsArrayJoinImpl(ast, descend_into_sql_udfs, visited_udfs);
}

}
