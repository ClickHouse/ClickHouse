#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <unordered_map>
#include <vector>

namespace DB
{

/*
 * Graph of relations between terms in constraints.
 * Allows to compare terms and get equal terms.
 */
class ComparisonGraph
{
public:
    ComparisonGraph(const std::vector<ASTPtr> & atomic_formulas);

    /// Works for string and num.
    /// For other -- only eq.
    enum class CompareResult
    {
        LESS,
        LESS_OR_EQUAL,
        EQUAL,
        GREATER_OR_EQUAL,
        GREATER,
        UNKNOWN,
    };

    CompareResult compare(const ASTPtr & left, const ASTPtr & right) const;

    std::vector<ASTPtr> getEqual(const ASTPtr & ast) const;
    std::optional<ASTPtr> getEqualConst(const ASTPtr & ast) const;

    /// Find constants less and greater.
    /// For int and double linear programming can be applied here.
    // TODO: implement
    //ASTPtr getMax(const ASTPtr &) const { return nullptr; } // sup
    //ASTPtr getMin(const ASTPtr &) const { return nullptr; } // inf

private:
    /// strongly connected component
    struct EqualComponent
    {
        std::vector<ASTPtr> asts;
    };

    /// TODO: move to diff for int and double:
    /// LESS and LESS_OR_EQUAL with +const or 0 --- ok
    ///                        with -const --- not ok
    /// EQUAL is ok only for 0
    struct Edge
    {
        enum Type
        {
            LESS,
            LESS_OR_EQUAL,
            EQUAL,
        };

        Type type;
        size_t to;
    };

    struct Graph
    {
        struct ASTHash {
            size_t operator() (const IAST::Hash & hash) const {
                return hash.first;
            }
        };

        std::unordered_map<IAST::Hash, size_t, ASTHash> ast_hash_to_component;
        std::vector<EqualComponent> vertexes;
        std::vector<std::vector<Edge>> edges;
    };

    ASTPtr normalizeAtom(const ASTPtr & atom) const;
    Graph BuildGraphFromAstsGraph(const Graph & asts_graph) const;

    Graph reverseGraph(const Graph & asts_graph) const;
    void dfsOrder(const Graph & asts_graph, size_t v, std::vector<bool> & visited, std::vector<size_t> & order) const;
    void dfsComponents(
            const Graph & reversed_graph, size_t v, std::vector<size_t> & components, const size_t not_visited, const size_t component) const;

    std::pair<bool, bool> findPath(const size_t start, const size_t finish) const;

    Graph graph;
};

}
