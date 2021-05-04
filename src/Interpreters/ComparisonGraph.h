#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <unordered_map>
#include <map>
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

    enum class CompareResult
    {
        LESS,
        LESS_OR_EQUAL,
        EQUAL,
        GREATER_OR_EQUAL,
        GREATER,
        NOT_EQUAL,
        UNKNOWN,
    };

    static CompareResult getCompareResult(const std::string & name);
    static CompareResult inverseCompareResult(const CompareResult result);

    CompareResult compare(const ASTPtr & left, const ASTPtr & right) const;

    /// It's possible that left <expected> right
    bool isPossibleCompare(const CompareResult expected, const ASTPtr & left, const ASTPtr & right) const;

    /// It's always true that left <expected> right
    bool isAlwaysCompare(const CompareResult expected, const ASTPtr & left, const ASTPtr & right) const;

    std::vector<ASTPtr> getEqual(const ASTPtr & ast) const;
    std::optional<ASTPtr> getEqualConst(const ASTPtr & ast) const;

    std::optional<std::size_t> getComponentId(const ASTPtr & ast) const;
    std::vector<ASTPtr> getComponent(const std::size_t id) const;
    bool hasPath(const size_t left, const size_t right) const;

    /// Find constants lessOrEqual and greaterOrEqual.
    /// For int and double linear programming can be applied here.
    /// Returns: {constant, is strict less/greater}
    std::optional<std::pair<Field, bool>> getConstUpperBound(const ASTPtr & ast) const;
    std::optional<std::pair<Field, bool>> getConstLowerBound(const ASTPtr & ast) const;

    std::vector<ASTs> getVertices() const;

private:
    /// strongly connected component
    struct EqualComponent
    {
        std::vector<ASTPtr> asts;
        ssize_t constant_index = -1;

        bool hasConstant() const;
        ASTPtr getConstant() const;
        void buildConstants();
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
        struct ASTHash
        {
            size_t operator() (const IAST::Hash & hash) const
            {
                return hash.first;
            }
        };

        std::unordered_map<IAST::Hash, size_t, ASTHash> ast_hash_to_component;
        std::vector<EqualComponent> vertices;
        std::vector<std::vector<Edge>> edges;
    };

    ASTPtr normalizeAtom(const ASTPtr & atom) const;
    Graph BuildGraphFromAstsGraph(const Graph & asts_graph) const;

    Graph reverseGraph(const Graph & asts_graph) const;
    void dfsOrder(const Graph & asts_graph, size_t v, std::vector<bool> & visited, std::vector<size_t> & order) const;
    void dfsComponents(
            const Graph & reversed_graph, size_t v, std::vector<size_t> & components, const size_t not_visited, const size_t component) const;

    std::pair<bool, bool> findPath(const size_t start, const size_t finish) const;

    enum class Path
    {
        LESS,
        LESS_OR_EQUAL,
    };

    std::map<std::pair<size_t, size_t>, Path> BuildDistsFromGraph(const Graph & g) const;
    std::pair<std::vector<ssize_t>, std::vector<ssize_t>> buildConstBounds() const;

    Graph graph;
    std::map<std::pair<size_t, size_t>, Path> dists;
    std::set<std::pair<size_t, size_t>> not_equal;
    std::vector<ssize_t> ast_const_lower_bound;
    std::vector<ssize_t> ast_const_upper_bound;
};

}
