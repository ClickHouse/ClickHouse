#include <Interpreters/ASTNonDeterministicFunctions.h>

#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DEPRECATED_FUNCTION;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

struct HasNonDeterministicFunctionsMatcher
{
    struct Data
    {
        const ContextPtr context;
        bool has_non_deterministic_functions = false;
    };

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

    static void visit(const ASTPtr & node, Data & data)
    {
        if (data.has_non_deterministic_functions)
            return;

        if (const auto * function = node->as<ASTFunction>())
        {
            FunctionOverloadResolverPtr func;
            try
            {
                func = FunctionFactory::instance().tryGet(function->name, data.context);
            }
            catch (const Exception & e)
            {
                /// tryGet instantiates the implementation; deprecations and experimental gates may throw instead of returning nullptr.
                /// Conservative for cache eligibility: treat as non-deterministic (disable caching).
                if (e.code() == ErrorCodes::DEPRECATED_FUNCTION || e.code() == ErrorCodes::SUPPORT_IS_DISABLED)
                {
                    data.has_non_deterministic_functions = true;
                    return;
                }
                throw;
            }
            if (func)
            {
                if (!func->isDeterministic())
                    data.has_non_deterministic_functions = true;
                return;
            }
            if (const auto udf_sql = UserDefinedSQLFunctionFactory::instance().tryGet(function->name))
            {
                /// ClickHouse currently doesn't know if SQL-based UDFs are deterministic or not. We must assume they are non-deterministic.
                data.has_non_deterministic_functions = true;
                return;
            }
            if (const auto udf_executable = UserDefinedExecutableFunctionFactory::tryGet(function->name, data.context))
            {
                if (!udf_executable->isDeterministic())
                    data.has_non_deterministic_functions = true;
                return;
            }
        }
    }
};

using HasNonDeterministicFunctionsVisitor = InDepthNodeVisitor<HasNonDeterministicFunctionsMatcher, true>;

}

bool astContainsNonDeterministicFunctions(ASTPtr ast, ContextPtr context)
{
    if (!ast || !context)
        return true;

    /// FunctionFactory::tryGet records used functions in query_log; suppress for eligibility checks.
    Context::SuppressQueryFactoriesInfoScope suppress_query_factories_info;

    HasNonDeterministicFunctionsMatcher::Data finder_data{context};
    HasNonDeterministicFunctionsVisitor(finder_data).visit(ast);
    return finder_data.has_non_deterministic_functions;
}

}
