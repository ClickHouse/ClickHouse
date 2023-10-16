#include <Interpreters/Streaming/SubstituteStreamingFunction.h>

#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int FUNCTION_NOT_ALLOWED;
}

namespace Streaming
{

std::unordered_map<String, String> StreamingFunctionData::func_map = {
    {"neighbor", "__streaming_neighbor"},
    {"row_number", "__streaming_row_number"},
    {"now64", "__streaming_now64"},
    {"now", "__streaming_now"},
};

std::set<String> StreamingFunctionData::streaming_only_func
    = {"__streaming_neighbor",
       "__streaming_row_number",
       "__streaming_now64",
       "__streaming_now",
       /// changelog_only
       "__count_retract",
       "__sum_retract",
       "__sum_kahan_retract",
       "__sum_with_overflow_retract",
       "__avg_retract",
       "__max_retract",
       "__min_retract",
       "__arg_min_retract",
       "__arg_max_retract"};


void StreamingFunctionData::visit(DB::ASTFunction & func, DB::ASTPtr)
{
    if (func.name == "emit_version")
    {
        emit_version = true;
        return;
    }

    if (streaming)
    {
        auto iter = func_map.find(func.name);
        if (iter != func_map.end())
        {
            /// Always show original column name
            func.code_name = func.getColumnNameWithoutAlias();
            func.name = iter->second;
            return;
        }
    }
    else if (streaming_only_func.contains(func.name))
        throw Exception(
            ErrorCodes::FUNCTION_NOT_ALLOWED, "{} function is private and is not supposed to be used directly in a query", func.name);
}

bool StreamingFunctionData::ignoreSubquery(const DB::ASTPtr &, const DB::ASTPtr & child)
{
    /// Don't go to FROM, JOIN, UNION since they are already handled recursively
    if (child->as<ASTTableExpression>() || child->as<ASTSelectQuery>())
        return false;

    return true;
}

}
}
