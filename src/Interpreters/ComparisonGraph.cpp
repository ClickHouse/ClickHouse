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
ASTPtr ComparisonGraph::normalizeAtom(const ASTPtr & atom) const
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
    static const std::map<std::string, Edge::Type> relation_to_enum = {
        {"equals", Edge::Type::EQUAL},
        {"less", Edge::Type::LESS},
        {"lessOrEquals", Edge::Type::LESS_OR_EQUAL},
    };

    Graph g;
    for (const auto & atom_raw : atomic_formulas) {
        const auto atom = normalizeAtom(atom_raw);

        const auto bad_term = std::numeric_limits<std::size_t>::max();
        auto get_index = [](const ASTPtr & ast, Graph & asts_graph) -> std::size_t {
            const auto it = asts_graph.ast_hash_to_component.find(ast->getTreeHash());
            if (it != std::end(asts_graph.ast_hash_to_component))
            {
                if (!std::any_of(
                        std::cbegin(asts_graph.vertexes[it->second].asts),
                        std::cend(asts_graph.vertexes[it->second].asts),
                        [ast](const ASTPtr & constraint_ast) {
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
                asts_graph.ast_hash_to_component[ast->getTreeHash()] = asts_graph.vertexes.size();
                asts_graph.vertexes.push_back(EqualComponent{{ast}});
                asts_graph.edges.emplace_back();
                return asts_graph.vertexes.size() - 1;
            }
        };

        const auto * func = atom->as<ASTFunction>();
        if (func)
        {
            if (const auto it = relation_to_enum.find(func->name); it != std::end(relation_to_enum) && func->arguments->children.size() == 2)
            {
                const size_t index_left = get_index(func->arguments->children[0], g);
                const size_t index_right = get_index(func->arguments->children[1], g);

                if (index_left != bad_term && index_right != bad_term)
                {
                    //Poco::Logger::get("Edges").information("GOOD: " + atom->dumpTree());
                    Poco::Logger::get("Edges").information("left=" + std::to_string(index_left) + " right=" + std::to_string(index_right));
                    Poco::Logger::get("Edges").information("sz=" + std::to_string(g.edges.size()));
                    g.edges[index_right].push_back(Edge{it->second, index_left});
                    if (func->name == "equals")
                    {
                        //Poco::Logger::get("Edges").information("right=" + std::to_string(index_left) + " left=" + std::to_string(index_right));
                        g.edges[index_left].push_back(Edge{it->second, index_right});
                    }
                }
                else
                {
                    //Poco::Logger::get("Edges").information("BAD: " + atom->dumpTree());
                }
            }
        }
    }

    graph = BuildGraphFromAstsGraph(g);
    dists = BuildDistsFromGraph(graph);
    std::tie(ast_const_lower_bound, ast_const_upper_bound) = buildConstBounds();
}

/// resturns {is less, is strict}
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
        Poco::Logger::get("dists found").information(std::to_string(start) + " " + std::to_string(finish) + " : " + std::to_string(static_cast<int>(it->second)));
        //Poco::Logger::get("dists found").information(graph.vertexes[start].asts.back()->dumpTree());
        //Poco::Logger::get("dists found").information(graph.vertexes[finish].asts.back()->dumpTree());
        return {true, it->second == Path::LESS};
    }
}

ComparisonGraph::CompareResult ComparisonGraph::compare(const ASTPtr & left, const ASTPtr & right) const
{
    size_t start = 0;
    size_t finish = 0;
    {
        for (const auto & [k, k2] : dists)
        {
            Poco::Logger::get("dists").information(std::to_string(k.first) + "-" + std::to_string(k.second) + " : " + std::to_string(static_cast<int>(k2)));
        }

        /// TODO: check full ast
        const auto it_left = graph.ast_hash_to_component.find(left->getTreeHash());
        const auto it_right = graph.ast_hash_to_component.find(right->getTreeHash());
        if (it_left == std::end(graph.ast_hash_to_component) || it_right == std::end(graph.ast_hash_to_component))
        {
            Poco::Logger::get("Graph").information("not found");
            Poco::Logger::get("Graph").information(std::to_string(left->getTreeHash().second));
            Poco::Logger::get("Graph").information(std::to_string(right->getTreeHash().second));
            for (const auto & [hash, id] : graph.ast_hash_to_component)
            {
                Poco::Logger::get("Graph MAP").information(std::to_string(hash.second) + " "  + std::to_string(id));
            }
            {
                const auto left_bound = getConstLowerBound(left);
                const auto right_bound = getConstUpperBound(right);
                if (left_bound && right_bound)
                {
                    //Poco::Logger::get("&&&&&&&").information(left_bound->first.dump() + " " + std::to_string(left_bound->second) + " | " + right_bound->first.dump() + " " + std::to_string(right_bound->second));
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
                //Poco::Logger::get("!!!!!!").information(left_bound->first.dump() + " " + std::to_string(left_bound->second) + " | " + right_bound->first.dump() + " " + std::to_string(right_bound->second));
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
            Poco::Logger::get("Graph").information("found:" + std::to_string(start) + " " + std::to_string(finish));
        }
    }

    if (start == finish)
        return CompareResult::EQUAL;

    auto [has_path, is_strict] = findPath(start, finish);
    if (has_path)
        return is_strict ? CompareResult::GREATER : CompareResult::GREATER_OR_EQUAL;

    auto [has_path_reverse, is_strict_reverse] = findPath(finish, start);
    if (has_path_reverse)
        return is_strict_reverse ? CompareResult::LESS : CompareResult::LESS_OR_EQUAL;

    return CompareResult::UNKNOWN;
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
        std::cbegin(graph.vertexes[index].asts),
        std::cend(graph.vertexes[index].asts),
        [ast](const ASTPtr & constraint_ast)
        {
            return constraint_ast->getTreeHash() == ast->getTreeHash() &&
                   constraint_ast->getColumnName() == ast->getColumnName();
        })) {
        return index;
    } else {
        return {};
    }
}

std::vector<ASTPtr> ComparisonGraph::getComponent(const std::size_t id) const
{
    return graph.vertexes[id].asts;
}

bool ComparisonGraph::EqualComponent::hasConstant() const {
    return constant_index != -1;
}

ASTPtr ComparisonGraph::EqualComponent::getConstant() const {
    return asts[constant_index];
}

void ComparisonGraph::EqualComponent::buildConstants() {
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

std::optional<ASTPtr> ComparisonGraph::getEqualConst(const ASTPtr & ast) const
{
    const auto hash_it = graph.ast_hash_to_component.find(ast->getTreeHash());
    if (hash_it == std::end(graph.ast_hash_to_component))
        return std::nullopt;
    const size_t index = hash_it->second;
    return graph.vertexes[index].hasConstant()
        ? std::optional<ASTPtr>{graph.vertexes[index].getConstant()}
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
    return std::make_pair(graph.vertexes[from].getConstant()->as<ASTLiteral>()->value, dists.at({from, to}) == Path::LESS);
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
    return std::make_pair(graph.vertexes[to].getConstant()->as<ASTLiteral>()->value, dists.at({from, to}) == Path::LESS);
}

void ComparisonGraph::dfsOrder(const Graph & asts_graph, size_t v, std::vector<bool> & visited, std::vector<size_t> & order) const
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

ComparisonGraph::Graph ComparisonGraph::reverseGraph(const Graph & asts_graph) const
{
    Graph g;
    g.ast_hash_to_component = asts_graph.ast_hash_to_component;
    g.vertexes = asts_graph.vertexes;
    g.edges.resize(g.vertexes.size());
    for (size_t v = 0; v < asts_graph.vertexes.size(); ++v)
    {
        for (const auto & edge : asts_graph.edges[v])
        {
            g.edges[edge.to].push_back(Edge{edge.type, v});
        }
    }
    return asts_graph;
}

std::vector<ASTs> ComparisonGraph::getVertexes() const
{
    std::vector<ASTs> result;
    for (const auto & vertex : graph.vertexes)
    {
        result.emplace_back();
        for (const auto & ast : vertex.asts)
            result.back().push_back(ast);
    }
    return result;
}

void ComparisonGraph::dfsComponents(
    const Graph & reversed_graph, size_t v, std::vector<size_t> & components, const size_t not_visited, const size_t component) const
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

ComparisonGraph::Graph ComparisonGraph::BuildGraphFromAstsGraph(const Graph & asts_graph) const
{
    Poco::Logger::get("Graph").information("building");
    /// Find strongly connected component
    const auto n = asts_graph.vertexes.size();

    for (size_t v = 0; v < n; ++v) {
        //Poco::LogStream{"kek"}.information() << "VERTEX " << v << " " << asts_graph.vertexes[v].asts.back()->dumpTree() << std::endl;
        for (const auto & edge : asts_graph.edges[v]) {
            //Poco::LogStream{"kek"}.information() << "TO " << edge.to << " " << static_cast<int>(edge.type) << std::endl;
        }
    }

    std::vector<size_t> order;
    {
        std::vector<bool> visited(n, false);
        for (size_t v = 0; v < n; ++v)
        {
            if (!visited[v])
                dfsOrder(asts_graph, v, visited, order);
        }
    }

    Poco::Logger::get("Graph").information("dfs1");

    const auto not_visited = std::numeric_limits<size_t>::max();
    std::vector<size_t> components(n, not_visited);
    size_t component = 0;
    {
        const Graph reversed_graph = reverseGraph(asts_graph);
        for (const size_t v : order)
        {
            if (components[v] == not_visited)
            {
                dfsComponents(reversed_graph, v, components, not_visited, component);
                ++component;
            }
        }
    }

    Poco::Logger::get("Graph").information("dfs2");

    Graph result;
    result.vertexes.resize(component);
    result.edges.resize(component);
    for (const auto & [hash, index] : asts_graph.ast_hash_to_component)
    {
        result.ast_hash_to_component[hash] = components[index];
        result.vertexes[components[index]].asts.insert(
            std::end(result.vertexes[components[index]].asts),
            std::begin(asts_graph.vertexes[index].asts),
            std::end(asts_graph.vertexes[index].asts)); // asts_graph has only one ast per vertex
    }

    /// Calculate constants
    for (auto & vertex : result.vertexes)
    {
        vertex.buildConstants();
    }

    Poco::Logger::get("Graph").information("components: " + std::to_string(component));

    for (size_t v = 0; v < n; ++v)
    {
        for (const auto & edge : asts_graph.edges[v])
        {
            result.edges[components[v]].push_back(Edge{edge.type, components[edge.to]});
        }
        // TODO: make edges unique (left most strict)
    }

    for (size_t v = 0; v < result.vertexes.size(); ++v)
    {
        for (size_t u = 0; u < result.vertexes.size(); ++u)
        {
            if (v == u)
                continue;
            if (result.vertexes[v].hasConstant() && result.vertexes[u].hasConstant())
            {
                const auto * left = result.vertexes[v].getConstant()->as<ASTLiteral>();
                const auto * right = result.vertexes[u].getConstant()->as<ASTLiteral>();
                //Poco::Logger::get("Graph").information(left->value.dump() + " " + right->value.dump());

                /// Only less. Equal constant fields = equal literals so it was already considered above.
                if (left->value > right->value)
                {
                    result.edges[v].push_back(Edge{Edge::LESS, u});
                }
            }
        }
    }

    Poco::Logger::get("Graph").information("finish");


   /* for (size_t v = 0; v < result.vertexes.size(); ++v) {
        Poco::LogStream{"kekkek"}.information() << "VERTEX " << v << " " << result.vertexes[v].asts.back()->dumpTree() << std::endl;
        for (const auto & edge : result.edges[v]) {
            Poco::LogStream{"kekkek"}.information() << "TO " << edge.to << " " << static_cast<int>(edge.type) << std::endl;
        }
    }*/

    for (size_t v = 0; v < result.vertexes.size(); ++v)
    {
        std::stringstream s;
        for (const auto & atom : result.vertexes[v].asts)
        {
            s << atom->getTreeHash().second << " ";
        }
        s << "|";
        for (const auto & atom : result.ast_hash_to_component)
        {
            s << atom.first.second << " -" << atom.second << " ";
        }

        Poco::Logger::get("Graph").information(s.str());
    }

    return result;
}

std::map<std::pair<size_t, size_t>, ComparisonGraph::Path> ComparisonGraph::BuildDistsFromGraph(const Graph & g) const
{
    // min path : < = -1, =< = 0
    const auto inf = std::numeric_limits<int8_t>::max();
    const size_t n = graph.vertexes.size();
    std::vector<std::vector<int8_t>> results(n, std::vector<int8_t>(n, inf));
    for (size_t v = 0; v < n; ++v)
    {
        results[v][v] = 0;
        for (const auto & edge : g.edges[v])
            results[v][edge.to] = std::min(results[v][edge.to], static_cast<int8_t>(edge.type == Edge::LESS ? -1 : 0));
    }
    for (size_t v = 0; v < n; ++v)
        for (size_t u = 0; u < n; ++u)
            Poco::LogStream{Poco::Logger::get("Graph ---=------------")}.information() << v << " " << u << " " << static_cast<int>(results[v][u]) << std::endl;

    for (size_t k = 0; k < n; ++k)
        for (size_t v = 0; v < n; ++v)
            for (size_t u = 0; u < n; ++u)
                if (results[v][k] != inf && results[k][u] != inf)
                    results[v][u] = std::min(results[v][u], std::min(results[v][k], results[k][u]));

    std::map<std::pair<size_t, size_t>, Path> path;
    for (size_t v = 0; v < n; ++v)
        for (size_t u = 0; u < n; ++u)
        {
            Poco::LogStream{Poco::Logger::get("Graph results-------------")}.information() << v << " " << u << " " << static_cast<int>(results[v][u]) << std::endl;
            if (results[v][u] != inf)
                path[std::make_pair(v, u)] = (results[v][u] == -1 ? Path::LESS : Path::LESS_OR_EQUAL);
        }
    return path;
}

std::pair<std::vector<ssize_t>, std::vector<ssize_t>> ComparisonGraph::buildConstBounds() const
{
    const size_t n = graph.vertexes.size();
    std::vector<ssize_t> lower(n, -1);
    std::vector<ssize_t> upper(n, -1);

    auto get_value = [this](const size_t vertex) -> Field {
        return graph.vertexes[vertex].getConstant()->as<ASTLiteral>()->value;
    };

    for (const auto & [edge, path] : dists)
    {
        const auto [from, to] = edge;
        if (graph.vertexes[to].hasConstant()) {
            if (lower[from] == -1
                || get_value(lower[from]) > get_value(to)
                || (get_value(lower[from]) >= get_value(to) && dists.at({from, to}) == Path::LESS))
                lower[from] = to;
        }
        if (graph.vertexes[from].hasConstant()) {
            if (upper[to] == -1
                || get_value(upper[to]) < get_value(from)
                || (get_value(upper[to]) <= get_value(from) && dists.at({from, to}) == Path::LESS))
                upper[to] = from;
        }
    }

    return {lower, upper};
}


}
