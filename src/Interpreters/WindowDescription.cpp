#include <Interpreters/WindowDescription.h>

#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

std::string WindowFunctionDescription::dump() const
{
    WriteBufferFromOwnString ss;

    ss << "window function '" << column_name << "\n";
    ss << "function node " << function_node->dumpTree() << "\n";
    ss << "aggregate function '" << aggregate_function->getName() << "'\n";
    if (!function_parameters.empty())
    {
        ss << "parameters " << toString(function_parameters) << "\n";
    }

    return ss.str();
}

std::string WindowDescription::dump() const
{
    WriteBufferFromOwnString ss;

    ss << "window '" << window_name << "'\n";
    ss << "partition_by " << dumpSortDescription(partition_by) << "\n";
    ss << "order_by " << dumpSortDescription(order_by) << "\n";
    ss << "full_sort_description " << dumpSortDescription(full_sort_description) << "\n";

    return ss.str();
}

}
