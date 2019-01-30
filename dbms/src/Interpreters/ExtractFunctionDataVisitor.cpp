#include <Interpreters/ExtractFunctionDataVisitor.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>


namespace DB
{

void ExtractFunctionData::visit(ASTFunction & function, ASTPtr &)
{
    if (AggregateFunctionFactory::instance().isAggregateFunctionName(function.name))
        aggregate_functions.emplace_back(&function);
    else
        functions.emplace_back(&function);
}

}
