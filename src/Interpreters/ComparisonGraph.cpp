#include <Interpreters/ComparisonGraph.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/queryToString.h>
#include <Common/FieldVisitorsAccurateComparison.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int VIOLATED_CONSTRAINT;
}

namespace
{

/// Make function a > b or a >= b
static ASTPtr normalizeAtom(const ASTPtr & atom)
{
    static const std::map<std::string, std::string> inverse_relations =
    {
        {"lessOrEquals", "greaterOrEquals"},
        {"less", "greater"},
    };

    ASTPtr res = atom->clone();
    if (const auto * func = res->as<ASTFunction>())
    {
        if (const auto it = inverse_relations.find(func->name); it != std::end(inverse_relations))
        {
            res = makeASTFunction(it->second, func->arguments->children.back()->clone(), func->arguments->children.front()->clone());
        }
    }

    return res;
}

bool less(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateLess{}, lhs, rhs); }
bool greater(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateLess{}, rhs, lhs); }
bool equals(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateEquals{}, lhs, rhs); }

}

ComparisonGraph::ComparisonGraph(const ASTList & atomic_formulas)
{
    if (atomic_formulas.empty())
        return;

    static const std::unordered_map<std::string, Edge::Type> relation_to_enum =
    {
        {"equals", Edge::EQUAL},
        {"greater", Edge::GREATER},
        {"greaterOrEquals", Edge::GREATER_OR_EQUAL},
    };

    /// Firstly build an intermediate graph,
    /// in which each vertex corresponds to one expression.
    /// That means that if we have edge (A, B) with type GREATER, then always A > B.
    /// If we have EQUAL relation, then we add both edges (A, B) and (B, A).

    Graph g;
    for (const auto & atom_raw : atomic_formulas)
    {
        const auto atom = normalizeAtom(atom_raw);

        auto get_index = [](const ASTPtr & ast, Graph & asts_graph) -> std::optional<size_t>
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
                    return {};
                }

                return it->second;
            }
            else
            {
                asts_graph.ast_hash_to_component[ast->getTreeHash()] = asts_graph.vertices.size();
                asts_graph.vertices.push_back(EqualComponent{{ast}, std::nullopt});
                asts_graph.edges.emplace_back();
                return asts_graph.vertices.size() - 1;
            }
        };

        const auto * func = atom->as<ASTFunction>();
        if (func && func->arguments->children.size() == 2)
        {
            auto index_left = get_index(func->arguments->children.front(), g);
            auto index_right = get_index(func->arguments->children.back(), g);

            if (index_left && index_right)
            {
                if (const auto it = relation_to_enum.find(func->name); it != std::end(relation_to_enum))
                {
                    g.edges[*index_left].push_back(Edge{it->second, *index_right});
                    if (it->second == Edge::EQUAL)
                        g.edges[*index_right].push_back(Edge{it->second, *index_left});
                }
            }
        }
    }

    /// Now expressions A and B are equal, if and only if
    /// we have both paths from A to B and from B to A in graph.
    /// That means that equivalence classes of expressions
    /// are the same as strongly connected components in graph.
    /// So, we find such components and build graph on them.
    /// All expressions from one equivalence class will be stored
    /// in the corresponding vertex of new graph.

    graph = buildGraphFromAstsGraph(g);
    dists = buildDistsFromGraph(graph);
    std::tie(ast_const_lower_bound, ast_const_upper_bound) = buildConstBounds();

    /// Find expressions that are known to be unequal.
    static const std::unordered_set<String> not_equals_functions = {"notEquals", "greater"};

    /// Explicitly save unequal components.
    /// TODO: Build a graph for unequal components.
    for (const auto & atom_raw : atomic_formulas)
    {
        const auto atom = normalizeAtom(atom_raw);
        const auto * func = atom->as<ASTFunction>();

        if (func && not_equals_functions.contains(func->name))
        {
            auto index_left = graph.ast_hash_to_component.at(func->arguments->children.front()->getTreeHash());
            auto index_right = graph.ast_hash_to_component.at(func->arguments->children.back()->getTreeHash());

            if (index_left == index_right)
                throw Exception(ErrorCodes::VIOLATED_CONSTRAINT,
                    "Found expression '{}', but its arguments considered equal according to constraints",
                    queryToString(atom));

            not_equal.emplace(index_left, index_right);
            not_equal.emplace(index_right, index_left);
        }
    }
}

ComparisonGraph::CompareResult ComparisonGraph::pathToCompareResult(Path path, bool inverse)
{
    switch (path)
    {
        case Path::GREATER: return inverse ? CompareResult::LESS : CompareResult::GREATER;
        case Path::GREATER_OR_EQUAL: return inverse ? CompareResult::LESS_OR_EQUAL : CompareResult::GREATER_OR_EQUAL;
    }
    __builtin_unreachable();
}

std::optional<ComparisonGraph::Path> ComparisonGraph::findPath(size_t start, size_t finish) const
{
    const auto it = dists.find(std::make_pair(start, finish));
    if (it == std::end(dists))
        return {};

    /// Since path can be only GREATER or GREATER_OR_EQUALS,
    /// we can strengthen the condition.
    return not_equal.contains({start, finish}) ? Path::GREATER : it->second;
}

ComparisonGraph::CompareResult ComparisonGraph::compare(const ASTPtr & left, const ASTPtr & right) const
{
    size_t start = 0;
    size_t finish = 0;

    /// TODO: check full ast
    const auto it_left = graph.ast_hash_to_component.find(left->getTreeHash());
    const auto it_right = graph.ast_hash_to_component.find(right->getTreeHash());

    if (it_left == std::end(graph.ast_hash_to_component) || it_right == std::end(graph.ast_hash_to_component))
    {
        CompareResult result = CompareResult::UNKNOWN;
        {
            const auto left_bound = getConstLowerBound(left);
            const auto right_bound = getConstUpperBound(right);

            if (left_bound && right_bound)
            {
                if (greater(left_bound->first, right_bound->first))
                    result = CompareResult::GREATER;
                else if (equals(left_bound->first, right_bound->first))
                    result = left_bound->second || right_bound->second
                        ? CompareResult::GREATER : CompareResult::GREATER_OR_EQUAL;
            }
        }
        {
            const auto left_bound = getConstUpperBound(left);
            const auto right_bound = getConstLowerBound(right);

            if (left_bound && right_bound)
            {
                if (less(left_bound->first, right_bound->first))
                    result = CompareResult::LESS;
                else if (equals(left_bound->first, right_bound->first))
                    result = left_bound->second || right_bound->second
                        ? CompareResult::LESS : CompareResult::LESS_OR_EQUAL;
            }
        }

        return result;
    }
    else
    {
        start = it_left->second;
        finish = it_right->second;
    }

    if (start == finish)
        return CompareResult::EQUAL;

    if (auto path = findPath(start, finish))
        return pathToCompareResult(*path, /*inverse=*/ false);

    if (auto path = findPath(finish, start))
        return pathToCompareResult(*path, /*inverse=*/ true);

    if (not_equal.contains({start, finish}))
        return CompareResult::NOT_EQUAL;

    return CompareResult::UNKNOWN;
}

bool ComparisonGraph::isPossibleCompare(CompareResult expected, const ASTPtr & left, const ASTPtr & right) const
{
    const auto result = compare(left, right);

    if (expected == CompareResult::UNKNOWN || result == CompareResult::UNKNOWN)
        return true;

    if (expected == result)
        return true;

    static const std::set<std::pair<CompareResult, CompareResult>> possible_pairs =
    {
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

bool ComparisonGraph::isAlwaysCompare(CompareResult expected, const ASTPtr & left, const ASTPtr & right) const
{
    const auto result = compare(left, right);

    if (expected == CompareResult::UNKNOWN || result == CompareResult::UNKNOWN)
        return false;

    if (expected == result)
        return true;

    static const std::set<std::pair<CompareResult, CompareResult>> possible_pairs =
    {
        {CompareResult::LESS_OR_EQUAL, CompareResult::LESS},
        {CompareResult::LESS_OR_EQUAL, CompareResult::EQUAL},
        {CompareResult::GREATER_OR_EQUAL, CompareResult::GREATER},
        {CompareResult::GREATER_OR_EQUAL, CompareResult::EQUAL},
        {CompareResult::NOT_EQUAL, CompareResult::GREATER},
        {CompareResult::NOT_EQUAL, CompareResult::LESS},
    };

    return possible_pairs.contains({expected, result});
}


ASTs ComparisonGraph::getEqual(const ASTPtr & ast) const
{
    const auto res = getComponentId(ast);
    if (!res)
        return {};
    else
        return getComponent(res.value());
}

std::optional<size_t> ComparisonGraph::getComponentId(const ASTPtr & ast) const
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

bool ComparisonGraph::hasPath(size_t left, size_t right) const
{
    return findPath(left, right) || findPath(right, left);
}

ASTs ComparisonGraph::getComponent(size_t id) const
{
    return graph.vertices[id].asts;
}

bool ComparisonGraph::EqualComponent::hasConstant() const
{
    return constant_index.has_value();
}

ASTPtr ComparisonGraph::EqualComponent::getConstant() const
{
    assert(constant_index);
    return asts[*constant_index];
}

void ComparisonGraph::EqualComponent::buildConstants()
{
    constant_index.reset();
    for (size_t i = 0; i < asts.size(); ++i)
    {
        if (asts[i]->as<ASTLiteral>())
        {
            constant_index = i;
            return;
        }
    }
}

ComparisonGraph::CompareResult ComparisonGraph::atomToCompareResult(const CNFQuery::AtomicFormula & atom)
{
    if (const auto * func = atom.ast->as<ASTFunction>())
    {
        auto expected = functionNameToCompareResult(func->name);
        if (atom.negative)
            expected = inverseCompareResult(expected);
        return expected;
    }

    return ComparisonGraph::CompareResult::UNKNOWN;
}

ComparisonGraph::CompareResult ComparisonGraph::functionNameToCompareResult(const std::string & name)
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

ComparisonGraph::CompareResult ComparisonGraph::inverseCompareResult(CompareResult result)
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
    if (const auto * literal = ast->as<ASTLiteral>())
        return std::make_pair(literal->value, false);

    const auto it = graph.ast_hash_to_component.find(ast->getTreeHash());
    if (it == std::end(graph.ast_hash_to_component))
        return std::nullopt;

    const size_t to = it->second;
    const ssize_t from = ast_const_upper_bound[to];
    if (from == -1)
        return  std::nullopt;

    return std::make_pair(graph.vertices[from].getConstant()->as<ASTLiteral>()->value, dists.at({from, to}) == Path::GREATER);
}

std::optional<std::pair<Field, bool>> ComparisonGraph::getConstLowerBound(const ASTPtr & ast) const
{
    if (const auto * literal = ast->as<ASTLiteral>())
        return std::make_pair(literal->value, false);

    const auto it = graph.ast_hash_to_component.find(ast->getTreeHash());
    if (it == std::end(graph.ast_hash_to_component))
        return std::nullopt;

    const size_t from = it->second;
    const ssize_t to = ast_const_lower_bound[from];
    if (to == -1)
        return std::nullopt;

    return std::make_pair(graph.vertices[to].getConstant()->as<ASTLiteral>()->value, dists.at({from, to}) == Path::GREATER);
}

void ComparisonGraph::dfsOrder(const Graph & asts_graph, size_t v, std::vector<bool> & visited, std::vector<size_t> & order)
{
    visited[v] = true;
    for (const auto & edge : asts_graph.edges[v])
        if (!visited[edge.to])
            dfsOrder(asts_graph, edge.to, visited, order);

    order.push_back(v);
}

ComparisonGraph::Graph ComparisonGraph::reverseGraph(const Graph & asts_graph)
{
    Graph g;
    g.ast_hash_to_component = asts_graph.ast_hash_to_component;
    g.vertices = asts_graph.vertices;
    g.edges.resize(g.vertices.size());

    for (size_t v = 0; v < asts_graph.vertices.size(); ++v)
        for (const auto & edge : asts_graph.edges[v])
            g.edges[edge.to].push_back(Edge{edge.type, v});

    return g;
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
    const Graph & reversed_graph, size_t v,
    OptionalIndices & components, size_t component)
{
    components[v] = component;
    for (const auto & edge : reversed_graph.edges[v])
        if (!components[edge.to])
            dfsComponents(reversed_graph, edge.to, components, component);
}

ComparisonGraph::Graph ComparisonGraph::buildGraphFromAstsGraph(const Graph & asts_graph)
{
    /// Find strongly connected component by using 2 dfs traversals.
    /// https://en.wikipedia.org/wiki/Kosaraju%27s_algorithm
    const auto n = asts_graph.vertices.size();

    std::vector<size_t> order;
    {
        std::vector<bool> visited(n, false);
        for (size_t v = 0; v < n; ++v)
        {
            if (!visited[v])
                dfsOrder(asts_graph, v, visited, order);
        }
    }

    OptionalIndices components(n);
    size_t component = 0;
    {
        const Graph reversed_graph = reverseGraph(asts_graph);
        for (auto it = order.rbegin(); it != order.rend(); ++it)
        {
            if (!components[*it])
            {
                dfsComponents(reversed_graph, *it, components, component);
                ++component;
            }
        }
    }

    Graph result;
    result.vertices.resize(component);
    result.edges.resize(component);
    for (const auto & [hash, index] : asts_graph.ast_hash_to_component)
    {
        assert(components[index]);
        result.ast_hash_to_component[hash] = *components[index];
        result.vertices[*components[index]].asts.insert(
            std::end(result.vertices[*components[index]].asts),
            std::begin(asts_graph.vertices[index].asts),
            std::end(asts_graph.vertices[index].asts)); // asts_graph has only one ast per vertex
    }

    /// Calculate constants
    for (auto & vertex : result.vertices)
        vertex.buildConstants();

    /// For each edge in initial graph, we add an edge between components in condensation graph.
    for (size_t v = 0; v < n; ++v)
    {
        for (const auto & edge : asts_graph.edges[v])
            result.edges[*components[v]].push_back(Edge{edge.type, *components[edge.to]});

        /// TODO: make edges unique (left most strict)
    }

    /// If we have constansts in two components, we can compare them and add and extra edge.
    for (size_t v = 0; v < result.vertices.size(); ++v)
    {
        for (size_t u = 0; u < result.vertices.size(); ++u)
        {
            if (v != u && result.vertices[v].hasConstant() && result.vertices[u].hasConstant())
            {
                const auto * left = result.vertices[v].getConstant()->as<ASTLiteral>();
                const auto * right = result.vertices[u].getConstant()->as<ASTLiteral>();

                /// Only GREATER. Equal constant fields = equal literals so it was already considered above.
                if (greater(left->value, right->value))
                    result.edges[v].push_back(Edge{Edge::GREATER, u});
            }
        }
    }

    return result;
}

std::map<std::pair<size_t, size_t>, ComparisonGraph::Path> ComparisonGraph::buildDistsFromGraph(const Graph & g)
{
    /// Min path : -1 means GREATER, 0 means GREATER_OR_EQUALS.
    /// We use Floyd–Warshall algorithm to find distances between all pairs of vertices.
    /// https://en.wikipedia.org/wiki/Floyd–Warshall_algorithm

    constexpr auto inf = std::numeric_limits<Int8>::max();
    const size_t n = g.vertices.size();
    std::vector<std::vector<Int8>> results(n, std::vector<Int8>(n, inf));

    for (size_t v = 0; v < n; ++v)
    {
        results[v][v] = 0;
        for (const auto & edge : g.edges[v])
            results[v][edge.to] = std::min(results[v][edge.to], static_cast<Int8>(edge.type == Edge::GREATER ? -1 : 0));
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
                path[std::make_pair(v, u)] = (results[v][u] == -1 ? Path::GREATER : Path::GREATER_OR_EQUAL);

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
                || greater(get_value(to), get_value(lower[from]))
                || (equals(get_value(to), get_value(lower[from])) && path == Path::GREATER))
                lower[from] = to;
        }

        if (graph.vertices[from].hasConstant())
        {
            if (upper[to] == -1
                || less(get_value(from), get_value(upper[to]))
                || (equals(get_value(from), get_value(upper[to])) && path == Path::GREATER))
                upper[to] = from;
        }
    }

    return {lower, upper};
}

}
