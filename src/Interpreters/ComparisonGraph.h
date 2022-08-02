#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/TreeCNFConverter.h>
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
    /// atomic_formulas are extracted from constraints.
    explicit ComparisonGraph(const ASTList & atomic_formulas);

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

    static CompareResult atomToCompareResult(const CNFQuery::AtomicFormula & atom);
    static CompareResult functionNameToCompareResult(const std::string & name);
    static CompareResult inverseCompareResult(CompareResult result);

    CompareResult compare(const ASTPtr & left, const ASTPtr & right) const;

    /// It's possible that left <expected> right
    bool isPossibleCompare(CompareResult expected, const ASTPtr & left, const ASTPtr & right) const;

    /// It's always true that left <expected> right
    bool isAlwaysCompare(CompareResult expected, const ASTPtr & left, const ASTPtr & right) const;

    /// Returns all expressions from component to which @ast belongs if any.
    std::vector<ASTPtr> getEqual(const ASTPtr & ast) const;

    /// Returns constant expression from component to which @ast belongs if any.
    std::optional<ASTPtr> getEqualConst(const ASTPtr & ast) const;

    /// Finds component id to which @ast belongs if any.
    std::optional<std::size_t> getComponentId(const ASTPtr & ast) const;

    /// Returns all expressions from component.
    std::vector<ASTPtr> getComponent(size_t id) const;

    size_t getNumOfComponents() const { return graph.vertices.size(); }

    bool hasPath(size_t left, size_t right) const;

    /// Find constants lessOrEqual and greaterOrEqual.
    /// For int and double linear programming can be applied here.
    /// Returns: {constant, is strict less/greater}
    std::optional<std::pair<Field, bool>> getConstUpperBound(const ASTPtr & ast) const;
    std::optional<std::pair<Field, bool>> getConstLowerBound(const ASTPtr & ast) const;

    /// Returns all expression in graph.
    std::vector<ASTs> getVertices() const;

private:
    /// Strongly connected component
    struct EqualComponent
    {
        /// All these expressions are considered as equal.
        std::vector<ASTPtr> asts;
        std::optional<size_t> constant_index;

        bool hasConstant() const;
        ASTPtr getConstant() const;
        void buildConstants();
    };

    /// Edge (from, to, type) means that it's always true that @from <op> @to,
    /// where @op is the operation of type @type.
    ///
    /// TODO: move to diff for int and double:
    /// GREATER and GREATER_OR_EQUAL with +const or 0 --- ok
    ///                        with -const --- not ok
    /// EQUAL is ok only for 0
    struct Edge
    {
        enum Type
        {
            GREATER,
            GREATER_OR_EQUAL,
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

    /// Receives graph, in which each vertex corresponds to one expression.
    /// Then finds strongly connected components and builds graph on them.
    static Graph buildGraphFromAstsGraph(const Graph & asts_graph);

    static Graph reverseGraph(const Graph & asts_graph);

    /// The first part of finding strongly connected components.
    /// Finds order of exit from vertices of dfs traversal of graph.
    static void dfsOrder(const Graph & asts_graph, size_t v, std::vector<bool> & visited, std::vector<size_t> & order);

    using OptionalIndices = std::vector<std::optional<size_t>>;

    /// The second part of finding strongly connected components.
    /// Assigns index of component for each vertex.
    static void dfsComponents(
        const Graph & reversed_graph, size_t v,
        OptionalIndices & components, size_t component);

    enum class Path
    {
        GREATER,
        GREATER_OR_EQUAL,
    };

    static CompareResult pathToCompareResult(Path path, bool inverse);
    std::optional<Path> findPath(size_t start, size_t finish) const;

    /// Calculate @dists.
    static std::map<std::pair<size_t, size_t>, Path> buildDistsFromGraph(const Graph & g);

    /// Calculate @ast_const_lower_bound and @ast_const_lower_bound.
    std::pair<std::vector<ssize_t>, std::vector<ssize_t>> buildConstBounds() const;

    /// Direct acyclic graph in which each vertex corresponds
    /// to one equivalence class of expressions.
    /// Each edge sets the relation between classes (GREATER or GREATER_OR_EQUAL).
    Graph graph;

    /// Precalculated distances between each pair of vertices.
    /// Distance can be either 0 or -1.
    /// 0 means GREATER_OR_EQUAL.
    /// -1 means GREATER.
    std::map<std::pair<size_t, size_t>, Path> dists;

    /// Explicitly collected components, for which it's known
    /// that expressions in them are unequal.
    std::set<std::pair<size_t, size_t>> not_equal;

    /// Maximal constant value for each component that
    /// is lower bound for all expressions in component.
    std::vector<ssize_t> ast_const_lower_bound;

    /// Minimal constant value for each component that
    /// is upper bound for all expressions in component.
    std::vector<ssize_t> ast_const_upper_bound;
};

}
