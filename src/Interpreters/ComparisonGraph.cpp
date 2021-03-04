#include <Interpreters/ComparisonGraph.h>

#include <algorithm>

namespace DB
{

ComparisonGraph::ComparisonGraph(const std::vector<ASTPtr> & /*atomic_formulas*/)
{
}

std::vector<ASTPtr> ComparisonGraph::getEqual(const ASTPtr & ast) const
{
    const auto hash_it = graph.ast_hash_to_component.find(ast->getTreeHash().second);
    if (hash_it != std::end(graph.ast_hash_to_component))
        return {};
    const size_t index = hash_it->second;
    //const auto vertex_it = std::find(std::begin(graph.vertexes[index].asts), std::end(graph.vertexes[index].asts), ast, );
    if (std::any_of(
            std::cbegin(graph.vertexes[index].asts),
            std::cend(graph.vertexes[index].asts),
            [ast](const ASTPtr & constraint_ast)
            {
                return constraint_ast->getTreeHash() == ast->getTreeHash() &&
                    constraint_ast->getColumnName() == ast->getColumnName();
            })) {
        return graph.vertexes[index].asts;
    } else {
        return {};
    }
}

ComparisonGraph::Graph ComparisonGraph::BuildGraphFromAsts(const Graph & /*asts_graph*/)
{
    return {};
}

}
