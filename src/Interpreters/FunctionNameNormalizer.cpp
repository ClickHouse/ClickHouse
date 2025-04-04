#include <Interpreters/FunctionNameNormalizer.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTTLElement.h>

#include <Functions/FunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

void FunctionNameNormalizer::visit(IAST * ast)
{
    if (!ast)
        return;

    // Normalize only selected children. Avoid normalizing engine clause because some engine might
    // have the same name as function, e.g. Log.
    if (auto * node_storage = ast->as<ASTStorage>())
    {
        visit(node_storage->partition_by);
        visit(node_storage->primary_key);
        visit(node_storage->order_by);
        visit(node_storage->sample_by);
        visit(node_storage->ttl_table);
        return;
    }

    // Normalize only selected children. Avoid normalizing type clause because some type might
    // have the same name as function, e.g. Date.
    if (auto * node_decl = ast->as<ASTColumnDeclaration>())
    {
        visit(node_decl->default_expression.get());
        visit(node_decl->ttl.get());
        return;
    }

    if (auto * node_func = ast->as<ASTFunction>())
        node_func->name = getAggregateFunctionCanonicalNameIfAny(getFunctionCanonicalNameIfAny(node_func->name));

    for (auto & child : ast->children)
        visit(child.get());

    if (auto * ttl_elem = ast->as<ASTTTLElement>())
    {
        for (const auto & a : ttl_elem->group_by_key)
            visit(a.get());
        for (const auto & a : ttl_elem->group_by_assignments)
            visit(a.get());
    }
}

}
