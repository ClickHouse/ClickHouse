#include <Interpreters/ComparisonGraph.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>

#include <Poco/Logger.h>
#include <Poco/LogStream.h>
#include <algorithm>
#include <deque>

namespace DB
{

/// make function a < b or a <= b
ASTPtr ComparisonGraph::normalizeAtom(const ASTPtr & atom)
{
    static const std::map<std::string, std::string> inverse_relations = {
        {"greaterOrEquals", "lessOrEquals"},
        {"greater", "less"},
    };

    ASTPtr res = atom->clone();
    {
        auto * func = res->as<ASTFunction>();
        if (func)
        {
            if (const auto it = inverse_relations.find(func->name); it != std::end(inverse_relations))
            {
                res = makeASTFunction(it->second, func->arguments->children[1]->clone(), func->arguments->children[0]->clone());
            }
        }
    }

    return res;
}

ComparisonGraph::ComparisonGraph(const std::vector<ASTPtr> & atomic_formulas)
{
    if (atomic_formulas.empty())
        return;
    static const std::unordered_map<std::string, Edge::Type> relation_to_enum =
    {
        {"equals", Edge::Type::EQUAL},
        {"less", Edge::Type::LESS},
        {"lessOrEquals", Edge::Type::LESS_OR_EQUAL},
    };

    Graph g;
    for (const auto & atom_raw : atomic_formulas)
    {
        const auto atom = ComparisonGraph::normalizeAtom(atom_raw);

        const auto bad_term = std::numeric_limits<std::size_t>::max();
        auto get_index = [](const ASTPtr & ast, Graph & asts_graph) -> std::size_t
        {
            const auto it = asts_graph.ast_hash_to_component.find(ast->getTreeHash());
            if (it != std::end(asts_graph.ast_hash_to_component))
            {
                if (!std::any_of(
                        std::cbegin(asts_graph.vertices[it->second].asts),
                        std::cend(asts_graph.vertices[it->second].asts),
                        [ast](const ASTPtr & constraint_ast)
                        {
                            return constraint_ast->getTreeHash() == ast->getTreeHash()
                                && constraint_ast->getColumnName() == ast->getColumnName();
                        }))
                {
                    return bad_term;
                }

                return it->second;
            }
            else
            {
                asts_graph.ast_hash_to_component[ast->getTreeHash()] = asts_graph.vertices.size();
                asts_graph.vertices.push_back(EqualComponent{{ast}});
                asts_graph.edges.emplace_back();
                return asts_graph.vertices.size() - 1;
            }
        };

        const auto * func = atom->as<ASTFunction>();
        if (func && func->arguments->children.size() == 2)
        {
            const size_t index_left = get_index(func->arguments->children[0], g);
            const size_t index_right = get_index(func->arguments->children[1], g);

            if (index_left != bad_term && index_right != bad_term)
            {
                if (const auto it = relation_to_enum.find(func->name);
                    it != std::end(relation_to_enum) && func->arguments->children.size() == 2)
                {
                    g.edges[index_right].push_back(Edge{it->second, index_left});
                    if (func->name == "equals")
                    {
                        g.edges[index_left].push_back(Edge{it->second, index_right});
                    }
                }
                else if (func->name == "notEquals")
                {
                    /// Do nothing.
                }
            }
        }
    }

    graph = ComparisonGraph::BuildGraphFromAstsGraph(g);
    dists = ComparisonGraph::BuildDistsFromGraph(graph);
    std::tie(ast_const_lower_bound, ast_const_upper_bound) = buildConstBounds();

    for (const auto & atom_raw : atomic_formulas)
    {
        const auto atom = ComparisonGraph::normalizeAtom(atom_raw);
        if (const auto * func = atom->as<ASTFunction>(); func)
        {
            auto index_left = graph.ast_hash_to_component.at(func->arguments->children[0]->getTreeHash());
            auto index_right = graph.ast_hash_to_component.at(func->arguments->children[1]->getTreeHash());
            not_equal.emplace(index_left, index_right);
            not_equal.emplace(index_right, index_left);
        }
    }
}
/// returns {is less, is strict}
/// {true, true} = <
/// {true, false} = =<
/// {false, ...} = ?
std::pair<bool, bool> ComparisonGraph::findPath(const size_t start, const size_t finish) const
{
    const auto it = dists.find(std::make_pair(start, finish));
    if (it == std::end(dists))
        return {false, false};
    else
    {
        return {true, it->second == Path::LESS || not_equal.contains({start, finish})};
    }
}

ComparisonGraph::CompareResult ComparisonGraph::compare(const ASTPtr & left, const ASTPtr & right) const
{
    size_t start = 0;
    size_t finish = 0;
    {
        /// TODO: check full ast
        const auto it_left = graph.ast_hash_to_component.find(left->getTreeHash());
        const auto it_right = graph.ast_hash_to_component.find(right->getTreeHash());
        if (it_left == std::end(graph.ast_hash_to_component) || it_right == std::end(graph.ast_hash_to_component))
        {
            {
                const auto left_bound = getConstLowerBound(left);
                const auto right_bound = getConstUpperBound(right);
                if (left_bound && right_bound)
                {
                    if (left_bound->first < right_bound->first)
                        return CompareResult::UNKNOWN;
                    else if (left_bound->first > right_bound->first)
                        return CompareResult::GREATER;
                    else if (left_bound->second || right_bound->second)
                        return CompareResult::GREATER;
                    else
                        return CompareResult::GREATER_OR_EQUAL;
                }
            }
            {
                const auto left_bound = getConstUpperBound(left);
                const auto right_bound = getConstLowerBound(right);
                if (left_bound && right_bound)
                {
                    if (left_bound->first > right_bound->first)
                        return CompareResult::UNKNOWN;
                    else if (left_bound->first < right_bound->first)
                        return CompareResult::LESS;
                    else if (left_bound->second || right_bound->second)
                        return CompareResult::LESS;
                    else
                        return CompareResult::LESS_OR_EQUAL;
                }
            }
            return CompareResult::UNKNOWN;
        }
        else
        {
            start = it_left->second;
            finish = it_right->second;
        }
    }

    if (start == finish)
        return CompareResult::EQUAL;

    const auto [has_path, is_strict] = findPath(start, finish);
    if (has_path)
        return is_strict ? CompareResult::GREATER : CompareResult::GREATER_OR_EQUAL;

    const auto [has_path_reverse, is_strict_reverse] = findPath(finish, start);
    if (has_path_reverse)
        return is_strict_reverse ? CompareResult::LESS : CompareResult::LESS_OR_EQUAL;

    if (not_equal.contains({start, finish}))
        return CompareResult::NOT_EQUAL;

    return CompareResult::UNKNOWN;
}

bool ComparisonGraph::isPossibleCompare(const CompareResult expected, const ASTPtr & left, const ASTPtr & right) const
{
    const auto result = compare(left, right);

    if (expected == CompareResult::UNKNOWN || result == CompareResult::UNKNOWN)
    {
        return true;
    }
    if (expected == result)
        return true;

    static const std::set<std::pair<CompareResult, CompareResult>> possible_pairs = {
        {CompareResult::EQUAL, CompareResult::LESS_OR_EQUAL},
        {CompareResult::EQUAL, CompareResult::GREATER_OR_EQUAL},
        {CompareResult::LESS_OR_EQUAL, CompareResult::LESS},
        {CompareResult::LESS_OR_EQUAL, CompareResult::EQUAL},
        {CompareResult::LESS_OR_EQUAL, CompareResult::NOT_EQUAL},
        {CompareResult::GREATER_OR_EQUAL, CompareResult::GREATER},
        {CompareResult::GREATER_OR_EQUAL, CompareResult::EQUAL},
        {CompareResult::GREATER_OR_EQUAL, CompareResult::NOT_EQUAL},
        {CompareResult::LESS, CompareResult::LESS},
        {CompareResult::LESS, CompareResult::LESS_OR_EQUAL},
        {CompareResult::LESS, CompareResult::NOT_EQUAL},
        {CompareResult::GREATER, CompareResult::GREATER},
        {CompareResult::GREATER, CompareResult::GREATER_OR_EQUAL},
        {CompareResult::GREATER, CompareResult::NOT_EQUAL},
        {CompareResult::NOT_EQUAL, CompareResult::LESS},
        {CompareResult::NOT_EQUAL, CompareResult::GREATER},
        {CompareResult::NOT_EQUAL, CompareResult::LESS_OR_EQUAL},
        {CompareResult::NOT_EQUAL, CompareResult::GREATER_OR_EQUAL},
    };

    return possible_pairs.contains({expected, result});
}

bool ComparisonGraph::isAlwaysCompare(const CompareResult expected, const ASTPtr & left, const ASTPtr & right) const
{
    const auto result = compare(left, right);

    if (expected == CompareResult::UNKNOWN || result == CompareResult::UNKNOWN)
        return false;
    if (expected == result)
        return true;

    static const std::set<std::pair<CompareResult, CompareResult>> possible_pairs = {
        {CompareResult::LESS_OR_EQUAL, CompareResult::LESS},
        {CompareResult::LESS_OR_EQUAL, CompareResult::EQUAL},
        {CompareResult::GREATER_OR_EQUAL, CompareResult::GREATER},
        {CompareResult::GREATER_OR_EQUAL, CompareResult::EQUAL},
        {CompareResult::NOT_EQUAL, CompareResult::GREATER},
        {CompareResult::NOT_EQUAL, CompareResult::LESS},
    };

    return possible_pairs.contains({expected, result});
}


std::vector<ASTPtr> ComparisonGraph::getEqual(const ASTPtr & ast) const
{
    const auto res = getComponentId(ast);
    if (!res)
        return {};
    else
        return getComponent(res.value());
}

std::optional<std::size_t> ComparisonGraph::getComponentId(const ASTPtr & ast) const
{
    const auto hash_it = graph.ast_hash_to_component.find(ast->getTreeHash());
    if (hash_it == std::end(graph.ast_hash_to_component))
        return {};
    const size_t index = hash_it->second;
    if (std::any_of(
        std::cbegin(graph.vertices[index].asts),
        std::cend(graph.vertices[index].asts),
        [ast](const ASTPtr & constraint_ast)
        {
            return constraint_ast->getTreeHash() == ast->getTreeHash() &&
                   constraint_ast->getColumnName() == ast->getColumnName();
        }))
    {
        return index;
    }
    else
    {
        return {};
    }
}

bool ComparisonGraph::hasPath(const size_t left, const size_t right) const
{
    return findPath(left, right).first || findPath(right, left).first;
}

std::vector<ASTPtr> ComparisonGraph::getComponent(const std::size_t id) const
{
    return graph.vertices[id].asts;
}

bool ComparisonGraph::EqualComponent::hasConstant() const
{
    return constant_index != -1;
}

ASTPtr ComparisonGraph::EqualComponent::getConstant() const
{
    return asts[constant_index];
}

void ComparisonGraph::EqualComponent::buildConstants()
{
    constant_index = -1;
    for (size_t i = 0; i < asts.size(); ++i)
    {
        if (asts[i]->as<ASTLiteral>())
        {
            constant_index = i;
            return;
        }
    }
}

ComparisonGraph::CompareResult ComparisonGraph::getCompareResult(const std::string & name)
{
    static const std::unordered_map<std::string, CompareResult> relation_to_compare =
    {
        {"equals", CompareResult::EQUAL},
        {"notEquals", CompareResult::NOT_EQUAL},
        {"less", CompareResult::LESS},
        {"lessOrEquals", CompareResult::LESS_OR_EQUAL},
        {"greaterOrEquals", CompareResult::GREATER_OR_EQUAL},
        {"greater", CompareResult::GREATER},
    };

    const auto it = relation_to_compare.find(name);
    return it == std::end(relation_to_compare) ? CompareResult::UNKNOWN : it->second;
}

ComparisonGraph::CompareResult ComparisonGraph::inverseCompareResult(const CompareResult result)
{
    static const std::unordered_map<CompareResult, CompareResult> inverse_relations =
    {
        {CompareResult::NOT_EQUAL, CompareResult::EQUAL},
        {CompareResult::EQUAL, CompareResult::NOT_EQUAL},
        {CompareResult::GREATER_OR_EQUAL, CompareResult::LESS},
        {CompareResult::GREATER, CompareResult::LESS_OR_EQUAL},
        {CompareResult::LESS, CompareResult::GREATER_OR_EQUAL},
        {CompareResult::LESS_OR_EQUAL, CompareResult::GREATER},
        {CompareResult::UNKNOWN, CompareResult::UNKNOWN},
    };
    return inverse_relations.at(result);
}

std::optional<ASTPtr> ComparisonGraph::getEqualConst(const ASTPtr & ast) const
{
    const auto hash_it = graph.ast_hash_to_component.find(ast->getTreeHash());
    if (hash_it == std::end(graph.ast_hash_to_component))
        return std::nullopt;
    const size_t index = hash_it->second;
    return graph.vertices[index].hasConstant()
        ? std::optional<ASTPtr>{graph.vertices[index].getConstant()}
        : std::nullopt;
}

std::optional<std::pair<Field, bool>> ComparisonGraph::getConstUpperBound(const ASTPtr & ast) const
{
    {
        const auto * literal = ast->as<ASTLiteral>();
        if (literal)
            return std::make_pair(literal->value, false);
    }
    const auto it = graph.ast_hash_to_component.find(ast->getTreeHash());
    if (it == std::end(graph.ast_hash_to_component))
        return std::nullopt;
    const size_t to = it->second;
    const ssize_t from = ast_const_upper_bound[to];
    if (from == -1)
        return  std::nullopt;
    return std::make_pair(graph.vertices[from].getConstant()->as<ASTLiteral>()->value, dists.at({from, to}) == Path::LESS);
}

std::optional<std::pair<Field, bool>> ComparisonGraph::getConstLowerBound(const ASTPtr & ast) const
{
    {
        const auto * literal = ast->as<ASTLiteral>();
        if (literal)
            return std::make_pair(literal->value, false);
    }
    const auto it = graph.ast_hash_to_component.find(ast->getTreeHash());
    if (it == std::end(graph.ast_hash_to_component))
        return std::nullopt;
    const size_t from = it->second;
    const ssize_t to = ast_const_lower_bound[from];
    if (to == -1)
        return  std::nullopt;
    return std::make_pair(graph.vertices[to].getConstant()->as<ASTLiteral>()->value, dists.at({from, to}) == Path::LESS);
}

void ComparisonGraph::dfsOrder(const Graph & asts_graph, size_t v, std::vector<bool> & visited, std::vector<size_t> & order)
{
    visited[v] = true;
    for (const auto & edge : asts_graph.edges[v])
    {
        if (!visited[edge.to])
        {
            dfsOrder(asts_graph, edge.to, visited, order);
        }
    }
    order.push_back(v);
}

ComparisonGraph::Graph ComparisonGraph::reverseGraph(const Graph & asts_graph)
{
    Graph g;
    g.ast_hash_to_component = asts_graph.ast_hash_to_component;
    g.vertices = asts_graph.vertices;
    g.edges.resize(g.vertices.size());
    for (size_t v = 0; v < asts_graph.vertices.size(); ++v)
    {
        for (const auto & edge : asts_graph.edges[v])
        {
            g.edges[edge.to].push_back(Edge{edge.type, v});
        }
    }
    return asts_graph;
}

std::vector<ASTs> ComparisonGraph::getVertices() const
{
    std::vector<ASTs> result;
    for (const auto & vertex : graph.vertices)
    {
        result.emplace_back();
        for (const auto & ast : vertex.asts)
            result.back().push_back(ast);
    }
    return result;
}

void ComparisonGraph::dfsComponents(
    const Graph & reversed_graph, size_t v, std::vector<size_t> & components, const size_t not_visited, const size_t component)
{
    components[v] = component;
    for (const auto & edge : reversed_graph.edges[v])
    {
        if (components[edge.to] == not_visited)
        {
            dfsComponents(reversed_graph, edge.to, components, not_visited, component);
        }
    }
}

ComparisonGraph::Graph ComparisonGraph::BuildGraphFromAstsGraph(const Graph & asts_graph)
{
    /// Find strongly connected component
    const auto n = asts_graph.vertices.size();

    std::vector<size_t> order;
    {
        std::vector<bool> visited(n, false);
        for (size_t v = 0; v < n; ++v)
        {
            if (!visited[v])
                ComparisonGraph::dfsOrder(asts_graph, v, visited, order);
        }
    }

    const auto not_visited = std::numeric_limits<size_t>::max();
    std::vector<size_t> components(n, not_visited);
    size_t component = 0;
    {
        const Graph reversed_graph = reverseGraph(asts_graph);
        for (const size_t v : order)
        {
            if (components[v] == not_visited)
            {
                ComparisonGraph::dfsComponents(reversed_graph, v, components, not_visited, component);
                ++component;
            }
        }
    }

    Graph result;
    result.vertices.resize(component);
    result.edges.resize(component);
    for (const auto & [hash, index] : asts_graph.ast_hash_to_component)
    {
        result.ast_hash_to_component[hash] = components[index];
        result.vertices[components[index]].asts.insert(
            std::end(result.vertices[components[index]].asts),
            std::begin(asts_graph.vertices[index].asts),
            std::end(asts_graph.vertices[index].asts)); // asts_graph has only one ast per vertex
    }

    /// Calculate constants
    for (auto & vertex : result.vertices)
    {
        vertex.buildConstants();
    }

    Poco::Logger::get("ComparisonGraph").information("components: " + std::to_string(component));

    for (size_t v = 0; v < n; ++v)
    {
        for (const auto & edge : asts_graph.edges[v])
        {
            result.edges[components[v]].push_back(Edge{edge.type, components[edge.to]});
        }
        // TODO: make edges unique (left most strict)
    }

    for (size_t v = 0; v < result.vertices.size(); ++v)
    {
        for (size_t u = 0; u < result.vertices.size(); ++u)
        {
            if (v == u)
                continue;
            if (result.vertices[v].hasConstant() && result.vertices[u].hasConstant())
            {
                const auto * left = result.vertices[v].getConstant()->as<ASTLiteral>();
                const auto * right = result.vertices[u].getConstant()->as<ASTLiteral>();

                /// Only less. Equal constant fields = equal literals so it was already considered above.
                if (left->value > right->value)
                {
                    result.edges[v].push_back(Edge{Edge::LESS, u});
                }
            }
        }
    }

    return result;
}

std::map<std::pair<size_t, size_t>, ComparisonGraph::Path> ComparisonGraph::BuildDistsFromGraph(const Graph & g)
{
    // min path : < = -1, =< = 0
    const auto inf = std::numeric_limits<int8_t>::max();
    const size_t n = g.vertices.size();
    std::vector<std::vector<int8_t>> results(n, std::vector<int8_t>(n, inf));
    for (size_t v = 0; v < n; ++v)
    {
        results[v][v] = 0;
        for (const auto & edge : g.edges[v])
            results[v][edge.to] = std::min(results[v][edge.to], static_cast<int8_t>(edge.type == Edge::LESS ? -1 : 0));
    }

    for (size_t k = 0; k < n; ++k)
        for (size_t v = 0; v < n; ++v)
            for (size_t u = 0; u < n; ++u)
                if (results[v][k] != inf && results[k][u] != inf)
                    results[v][u] = std::min(results[v][u], std::min(results[v][k], results[k][u]));

    std::map<std::pair<size_t, size_t>, Path> path;
    for (size_t v = 0; v < n; ++v)
        for (size_t u = 0; u < n; ++u)
            if (results[v][u] != inf)
                path[std::make_pair(v, u)] = (results[v][u] == -1 ? Path::LESS : Path::LESS_OR_EQUAL);

    return path;
}

std::pair<std::vector<ssize_t>, std::vector<ssize_t>> ComparisonGraph::buildConstBounds() const
{
    const size_t n = graph.vertices.size();
    std::vector<ssize_t> lower(n, -1);
    std::vector<ssize_t> upper(n, -1);

    auto get_value = [this](const size_t vertex) -> Field
    {
        return graph.vertices[vertex].getConstant()->as<ASTLiteral>()->value;
    };

    for (const auto & [edge, path] : dists)
    {
        const auto [from, to] = edge;
        if (graph.vertices[to].hasConstant())
        {
            if (lower[from] == -1
                || get_value(lower[from]) > get_value(to)
                || (get_value(lower[from]) >= get_value(to) && dists.at({from, to}) == Path::LESS))
                lower[from] = to;
        }
        if (graph.vertices[from].hasConstant())
        {
            if (upper[to] == -1
                || get_value(upper[to]) < get_value(from)
                || (get_value(upper[to]) <= get_value(from) && dists.at({from, to}) == Path::LESS))
                upper[to] = from;
        }
    }

    return {lower, upper};
}


}
