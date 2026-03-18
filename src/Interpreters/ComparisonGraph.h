#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/TreeCNFConverter.h>

#include <Analyzer/Passes/CNF.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/IQueryTreeNode.h>

#include <type_traits>
#include <unordered_map>
#include <map>
#include <vector>

namespace DB
{

enum class ComparisonGraphCompareResult : uint8_t
{
    LESS,
    LESS_OR_EQUAL,
    EQUAL,
    GREATER_OR_EQUAL,
    GREATER,
    NOT_EQUAL,
    UNKNOWN,
};

template <typename T>
concept ComparisonGraphNodeType = std::same_as<T, ASTPtr> || std::same_as<T, QueryTreeNodePtr>;

/*
 * Graph of relations between terms in constraints.
 * Allows to compare terms and get equal terms.
 */
template <ComparisonGraphNodeType Node>
class ComparisonGraph
{
public:
    static constexpr bool with_ast = std::same_as<Node, ASTPtr>;
    using NodeContainer = std::conditional_t<with_ast, ASTs, QueryTreeNodes>;
    using CNF = std::conditional_t<with_ast, CNFQuery, Analyzer::CNF>;

    /// atomic_formulas are extracted from constraints.
    explicit ComparisonGraph(const NodeContainer & atomic_formulas, ContextPtr context = nullptr);

    static ComparisonGraphCompareResult atomToCompareResult(const typename CNF::AtomicFormula & atom);

    ComparisonGraphCompareResult compare(const Node & left, const Node & right) const;

    /// It's possible that left <expected> right
    bool isPossibleCompare(ComparisonGraphCompareResult expected, const Node & left, const Node & right) const;

    /// It's always true that left <expected> right
    bool isAlwaysCompare(ComparisonGraphCompareResult expected, const Node & left, const Node & right) const;

    /// Returns all expressions from component to which @node belongs if any.
    NodeContainer getEqual(const Node & node) const;

    /// Returns constant expression from component to which @node belongs if any.
    std::optional<Node> getEqualConst(const Node & node) const;

    /// Finds component id to which @node belongs if any.
    std::optional<std::size_t> getComponentId(const Node & node) const;

    /// Returns all expressions from component.
    NodeContainer getComponent(size_t id) const;

    size_t getNumOfComponents() const { return graph.vertices.size(); }

    bool hasPath(size_t left, size_t right) const;

    /// Find constants lessOrEqual and greaterOrEqual.
    /// For int and double linear programming can be applied here.
    /// Returns: {constant, is strict less/greater}
    std::optional<std::pair<Field, bool>> getConstUpperBound(const Node & node) const;
    std::optional<std::pair<Field, bool>> getConstLowerBound(const Node & node) const;

    /// Returns all expression in graph.
    std::vector<NodeContainer> getVertices() const;

private:
    /// Strongly connected component
    struct EqualComponent
    {
        /// All these expressions are considered as equal.
        NodeContainer nodes;
        std::optional<size_t> constant_index;

        bool hasConstant() const;
        Node getConstant() const;
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
                return hash.low64;
            }
        };

        static auto getHash(const Node & node)
        {
            if constexpr (with_ast)
                return node->getTreeHash(/*ignore_aliases=*/ true);
            else
                return QueryTreeNodePtrWithHash{node};
        }

        using NodeHashToComponentContainer = std::conditional_t<with_ast, std::unordered_map<IAST::Hash, size_t, ASTHash>, QueryTreeNodePtrWithHashMap<size_t>>;
        NodeHashToComponentContainer node_hash_to_component;
        std::vector<EqualComponent> vertices;
        std::vector<std::vector<Edge>> edges;
    };

    /// Receives graph, in which each vertex corresponds to one expression.
    /// Then finds strongly connected components and builds graph on them.
    static Graph buildGraphFromNodesGraph(const Graph & nodes_graph);

    static Graph reverseGraph(const Graph & nodes_graph);

    /// The first part of finding strongly connected components.
    /// Finds order of exit from vertices of dfs traversal of graph.
    static void dfsOrder(const Graph & nodes_graph, size_t v, std::vector<bool> & visited, std::vector<size_t> & order);

    using OptionalIndices = std::vector<std::optional<size_t>>;

    /// The second part of finding strongly connected components.
    /// Assigns index of component for each vertex.
    static void dfsComponents(
        const Graph & reversed_graph, size_t v,
        OptionalIndices & components, size_t component);

    enum class Path : uint8_t
    {
        GREATER,
        GREATER_OR_EQUAL,
    };

    static ComparisonGraphCompareResult pathToCompareResult(Path path, bool inverse);
    std::optional<Path> findPath(size_t start, size_t finish) const;

    /// Calculate @dists.
    static std::map<std::pair<size_t, size_t>, Path> buildDistsFromGraph(const Graph & g);

    /// Calculate @nodeconst_lower_bound and @node_const_lower_bound.
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
    std::vector<ssize_t> node_const_lower_bound;

    /// Minimal constant value for each component that
    /// is upper bound for all expressions in component.
    std::vector<ssize_t> node_const_upper_bound;
};

}
