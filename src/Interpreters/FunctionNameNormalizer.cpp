#include <Interpreters/FunctionNameNormalizer.h>

namespace DB
{

const String & getFunctionCanonicalNameIfAny(const String & name);
const String & getAggregateFunctionCanonicalNameIfAny(const String & name);

void FunctionNameNormalizer::visit(ASTPtr & ast)
{
    if (auto * node_func = ast->as<ASTFunction>())
        node_func->name = getAggregateFunctionCanonicalNameIfAny(getFunctionCanonicalNameIfAny(node_func->name));

    for (auto & child : ast->children)
        visit(child);
}

}
