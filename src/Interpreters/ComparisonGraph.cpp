#include <Interpreters/ComparisonGraph.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>

#include <Common/FieldAccurateComparison.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/ConstantNode.h>

#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnConst.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int VIOLATED_CONSTRAINT;
    extern const int TYPE_MISMATCH;
    extern const int BAD_TYPE_OF_FIELD;
}

/// Extract column name from ActionsDAG node result_name, stripping table prefix if present
/// For example: "__table1.column_name" -> "column_name", "column_name" -> "column_name"
static std::string_view extractColumnName(std::string_view full_name)
{
    auto dot_pos = full_name.find_last_of('.');
    if (dot_pos != std::string_view::npos)
        return full_name.substr(dot_pos + 1);
    return full_name;
}

/// Semantic hash for ActionsDAG nodes that's consistent across DAG instances
UInt64 getSemanticHash(const ActionsDAG::Node * node)
{
    if (!node)
        return 0;

    SipHash hash;

    // Hash the type
    hash.update(node->type);

    // Hash the result type name
    if (node->result_type)
        hash.update(node->result_type->getName());

    switch (node->type)
    {
        case ActionsDAG::ActionType::INPUT:
            // For input columns, hash by column name only (strip table prefix)
            // This ensures "__table1.a" and "a" hash to the same value
            hash.update(extractColumnName(node->result_name));
            break;

        case ActionsDAG::ActionType::COLUMN:
            // For constants, hash the actual value
            if (node->column)
            {
                if (const auto * const_column = typeid_cast<const ColumnConst *>(node->column.get()))
                {
                    Field value = (*const_column)[0];
                    // Hash the field value
                    hash.update(value);
                }
            }
            break;

        case ActionsDAG::ActionType::FUNCTION:
            // For functions, hash function name and children
            if (node->function_base)
                hash.update(node->function_base->getName());

            // Recursively hash children
            for (const auto * child : node->children)
                hash.update(getSemanticHash(child));
            break;

        case ActionsDAG::ActionType::ALIAS:
            // For aliases, hash the child
            if (!node->children.empty())
                hash.update(getSemanticHash(node->children[0]));
            break;

        case ActionsDAG::ActionType::ARRAY_JOIN:
            // For array join, hash result name and child
            hash.update(node->result_name);
            if (!node->children.empty())
                hash.update(getSemanticHash(node->children[0]));
            break;

        default:
            break;
    }

    return hash.get64();
}

/// Semantic equality check for ActionsDAG nodes from potentially different DAG instances
bool areNodesEqual(const ActionsDAG::Node * left, const ActionsDAG::Node * right)
{
    if (!left || !right)
        return left == right;

    // Must have same type
    if (left->type != right->type)
        return false;

    // Must have same result type
    if (left->result_type && right->result_type)
    {
        if (!left->result_type->equals(*right->result_type))
            return false;
    }
    else if (left->result_type != right->result_type)
        return false;

    // Check based on node type
    switch (left->type)
    {
        case ActionsDAG::ActionType::INPUT:
        {
            // For input columns, compare by column name only (strip table prefix)
            // This ensures "__table1.a" and "a" are considered equal
            auto left_col = extractColumnName(left->result_name);
            auto right_col = extractColumnName(right->result_name);
            bool result = left_col == right_col;
            return result;
        }

        case ActionsDAG::ActionType::COLUMN:
            // For constants, compare the actual constant value
            if (left->column && right->column)
            {
                // Both should be ColumnConst for constants
                if (const auto * left_const = typeid_cast<const ColumnConst *>(left->column.get()))
                    if (const auto * right_const = typeid_cast<const ColumnConst *>(right->column.get()))
                        return (*left_const)[0] == (*right_const)[0];
            }
            return false;

        case ActionsDAG::ActionType::FUNCTION:
            // For functions, compare function name and children
            if (left->function_base && right->function_base)
            {
                if (left->function_base->getName() != right->function_base->getName())
                    return false;
            }
            else if (left->function_base != right->function_base)
                return false;

            // Compare children recursively
            if (left->children.size() != right->children.size())
                return false;

            for (size_t i = 0; i < left->children.size(); ++i)
            {
                if (!areNodesEqual(left->children[i], right->children[i]))
                    return false;
            }
            return true;

        case ActionsDAG::ActionType::ALIAS:
            // For aliases, compare the child
            if (left->children.size() != 1 || right->children.size() != 1)
                return false;
            return areNodesEqual(left->children[0], right->children[0]);

        case ActionsDAG::ActionType::ARRAY_JOIN:
            // For array join, compare child and result name
            if (left->children.size() != 1 || right->children.size() != 1)
                return false;
            return left->result_name == right->result_name && areNodesEqual(left->children[0], right->children[0]);

        default:
            return false;
    }
}

namespace
{

/// Make function a > b or a >= b
ASTPtr normalizeAtom(const ASTPtr & atom, ContextPtr)
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
            auto new_func = makeASTOperator(it->second, func->arguments->children[1]->clone(), func->arguments->children[0]->clone());
            new_func->is_operator = func->is_operator;
            res = new_func;
        }
    }

    return res;
}

QueryTreeNodePtr normalizeAtom(const QueryTreeNodePtr & atom, const ContextPtr & context)
{
    static const std::map<std::string, std::string> inverse_relations =
    {
        {"lessOrEquals", "greaterOrEquals"},
        {"less", "greater"},
    };

    if (const auto * function_node = atom->as<FunctionNode>())
    {
        if (const auto it = inverse_relations.find(function_node->getFunctionName()); it != inverse_relations.end())
        {
            auto inverted_node = function_node->clone();
            auto * inverted_function_node = inverted_node->as<FunctionNode>();
            auto function_resolver = FunctionFactory::instance().get(it->second, context);
            auto & arguments = inverted_function_node->getArguments().getNodes();
            chassert(arguments.size() == 2);
            std::swap(arguments[0], arguments[1]);
            inverted_function_node->resolveAsFunction(function_resolver);
            return inverted_node;
        }
    }

    return atom;
}

const ActionsDAG::Node * normalizeAtom(const ActionsDAG::Node * atom, const ContextPtr & /*context*/)
{
    /// For ActionsDAG, we can't create new nodes here since we don't have access to a DAG.
    /// Instead, the graph construction handles both less/greater and lessOrEquals/greaterOrEquals
    /// without normalization by including them in the relation_to_enum mapping.
    return atom;
}

const FunctionNode * tryGetFunctionNode(const QueryTreeNodePtr & node)
{
    return node->as<FunctionNode>();
}

const ASTFunction * tryGetFunctionNode(const ASTPtr & node)
{
    return node->as<ASTFunction>();
}

const ActionsDAG::Node * tryGetFunctionNode(const ActionsDAG::Node * node)
{
    if (node && node->type == ActionsDAG::ActionType::FUNCTION)
        return node;
    return nullptr;
}

std::string functionName(const QueryTreeNodePtr & node)
{
    return node->as<FunctionNode &>().getFunctionName();
}

std::string functionName(const ASTPtr & node)
{
    return node->as<ASTFunction &>().name;
}

std::string functionName(const ActionsDAG::Node * node)
{
    if (node && node->function_base)
        return node->function_base->getName();
    return "";
}

std::optional<Field> tryGetConstantValue(const QueryTreeNodePtr & node)
{
    if (const auto * constant = node->as<ConstantNode>())
        return constant->getValue();

    return {};
}

std::optional<Field> tryGetConstantValue(const ASTPtr & node)
{
    if (const auto * constant = node->as<ASTLiteral>())
        return constant->value;

    return {};
}

std::optional<Field> tryGetConstantValue(const ActionsDAG::Node * node)
{
    if (!node || node->type != ActionsDAG::ActionType::COLUMN || !node->column)
        return {};

    if (const auto * const_column = typeid_cast<const ColumnConst *>(node->column.get()))
        return (*const_column)[0];

    return {};
}

template <typename Node>
Field getConstantValue(const Node & node)
{
    const auto constant = tryGetConstantValue(node);
    chassert(constant);
    return std::move(*constant);
}

const auto & getNode(const Analyzer::CNFAtomicFormula & atom)
{
    return atom.node_with_hash.node;
}

const auto & getNode(const CNFQueryAtomicFormula & atom)
{
    return atom.ast;
}

const auto & getNode(const ActionsDAGCNF::AtomicFormula & atom)
{
    return atom.node_with_hash.node;
}

std::string nodeToString(const ASTPtr & ast)
{
    return ast->formatWithSecretsOneLine();
}

std::string nodeToString(const QueryTreeNodePtr & node)
{
    return node->toAST()->formatWithSecretsOneLine();
}

std::string nodeToString(const ActionsDAG::Node * node)
{
    if (!node)
        return "<null>";
    return node->result_name;
}

const auto & getArguments(const ASTFunction * function)
{
    return function->arguments->children;
}

const auto & getArguments(const FunctionNode * function)
{
    return function->getArguments().getNodes();
}

const auto & getArguments(const ActionsDAG::Node * function)
{
    return function->children;
}

bool less(const Field & lhs, const Field & rhs)
{
    try
    {
        return accurateLess(lhs, rhs);
    }
    catch (const DB::Exception & e)
    {
        if (e.code() == ErrorCodes::BAD_TYPE_OF_FIELD)
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "Trying to compare constants of incompatible types ({} and {}), please verify that table constraints are "
                "defined using correct types, constraints can be ignored by disabling 'optimize_using_constraints'. Constraints for table "
                "can be modified using 'ALTER TABLE ... ADD/DROP CONSTRAINT' query",
                lhs.getTypeName(),
                rhs.getTypeName());

        throw;
    }
}

bool greater(const Field & lhs, const Field & rhs)
{
    return less(rhs, lhs);
}

bool equals(const Field & lhs, const Field & rhs)
{
    try
    {
        return accurateEquals(lhs, rhs);
    }
    catch (const DB::Exception & e)
    {
        if (e.code() == ErrorCodes::BAD_TYPE_OF_FIELD)
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "Trying to compare constants of incompatible types ({} and {}), please verify that table constraints are "
                "defined using correct types, constraints can be ignored by disabling 'optimize_using_constraints'. Constraints for table "
                "can be modified using 'ALTER TABLE ... ADD/DROP CONSTRAINT' query",
                lhs.getTypeName(),
                rhs.getTypeName());

        throw;
    }
}

ComparisonGraphCompareResult functionNameToCompareResult(const std::string & name)
{
    using enum ComparisonGraphCompareResult;
    static const std::unordered_map<std::string, ComparisonGraphCompareResult> relation_to_compare =
    {
        {"equals", EQUAL},
        {"notEquals", NOT_EQUAL},
        {"less", LESS},
        {"lessOrEquals", LESS_OR_EQUAL},
        {"greaterOrEquals", GREATER_OR_EQUAL},
        {"greater", GREATER},
    };

    const auto it = relation_to_compare.find(name);
    return it == std::end(relation_to_compare) ? UNKNOWN : it->second;
}

ComparisonGraphCompareResult inverseCompareResult(ComparisonGraphCompareResult result)
{
    using enum ComparisonGraphCompareResult;
    static const std::unordered_map<ComparisonGraphCompareResult, ComparisonGraphCompareResult> inverse_relations =
    {
        {NOT_EQUAL, EQUAL},
        {EQUAL, NOT_EQUAL},
        {GREATER_OR_EQUAL, LESS},
        {GREATER, LESS_OR_EQUAL},
        {LESS, GREATER_OR_EQUAL},
        {LESS_OR_EQUAL, GREATER},
        {UNKNOWN, UNKNOWN},
    };
    return inverse_relations.at(result);
}

}

template <ComparisonGraphNodeType Node>
ComparisonGraph<Node>::ComparisonGraph(const NodeContainer & atomic_formulas, ContextPtr context)
{
    if (atomic_formulas.empty())
        return;

    static const std::unordered_map<std::string, typename Graph::Edge::Type> relation_to_enum =
    {
        {"equals", Graph::Edge::EQUAL},
        {"greater", Graph::Edge::GREATER},
        {"greaterOrEquals", Graph::Edge::GREATER_OR_EQUAL},
    };

    /// For ActionsDAG, we also need to handle the inverse relations since we don't normalize
    static const std::unordered_map<std::string, typename Graph::Edge::Type> inverse_relation_to_enum =
    {
        {"less", Graph::Edge::GREATER},          // a < b means b > a
        {"lessOrEquals", Graph::Edge::GREATER_OR_EQUAL},  // a <= b means b >= a
    };

    /// Firstly build an intermediate graph,
    /// in which each vertex corresponds to one expression.
    /// That means that if we have edge (A, B) with type GREATER, then always A > B.
    /// If we have EQUAL relation, then we add both edges (A, B) and (B, A).

    Graph g;
    for (const auto & atom_raw : atomic_formulas)
    {
        const auto atom = normalizeAtom(atom_raw, context);

        auto get_index = [](const Node & node, Graph & nodes_graph) -> std::optional<size_t>
        {
            const auto it = nodes_graph.node_hash_to_component.find(Graph::getHash(node));
            if (it != std::end(nodes_graph.node_hash_to_component))
            {
                if (!std::any_of(
                        std::cbegin(nodes_graph.vertices[it->second].nodes),
                        std::cend(nodes_graph.vertices[it->second].nodes),
                        [node](const Node & constraint_node)
                        {
                            if constexpr (with_ast)
                                return constraint_node->getTreeHash(/*ignore_aliases=*/ true) == node->getTreeHash(/*ignore_aliases=*/ true)
                                    && constraint_node->getColumnName() == node->getColumnName();
                            else if constexpr (with_actions_dag)
                                return areNodesEqual(constraint_node, node);
                            else
                                return constraint_node->isEqual(*node);
                        }))
                {
                    return {};
                }

                return it->second;
            }

            nodes_graph.node_hash_to_component[Graph::getHash(node)] = nodes_graph.vertices.size();
            nodes_graph.vertices.push_back({{node}, std::nullopt});
            nodes_graph.edges.emplace_back();
            return nodes_graph.vertices.size() - 1;
        };

        const auto * function_node = tryGetFunctionNode(atom);
        if (function_node)
        {
            const auto & arguments = getArguments(function_node);
            if (arguments.size() == 2)
            {
                auto index_left = get_index(arguments[0], g);
                auto index_right = get_index(arguments[1], g);

                if (index_left && index_right)
                {
                    const auto func_name = functionName(atom);
                    // Handle normal relations (equals, greater, greaterOrEquals)
                    if (const auto relation_it = relation_to_enum.find(func_name); relation_it != std::end(relation_to_enum))
                    {
                        g.edges[*index_left].push_back({relation_it->second, *index_right});
                        if (relation_it->second == Graph::Edge::EQUAL)
                            g.edges[*index_right].push_back({relation_it->second, *index_left});
                    }
                    // Handle inverse relations (less, lessOrEquals) for ActionsDAG
                    // For these, we add the edge in the reverse direction
                    // e.g., "a < b" becomes an edge from b to a with type GREATER
                    else if (const auto inverse_it = inverse_relation_to_enum.find(func_name); inverse_it != std::end(inverse_relation_to_enum))
                    {
                        g.edges[*index_right].push_back({inverse_it->second, *index_left});
                    }
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

    graph = buildGraphFromNodesGraph(g);
    dists = buildDistsFromGraph(graph);
    std::tie(node_const_lower_bound, node_const_upper_bound) = buildConstBounds();

    /// Find expressions that are known to be unequal.
    static const std::unordered_set<String> not_equals_functions = {"notEquals", "greater"};

    /// Explicitly save unequal components.
    /// TODO: Build a graph for unequal components.
    for (const auto & atom_raw : atomic_formulas)
    {
        const auto atom = normalizeAtom(atom_raw, context);

        const auto * function_node = tryGetFunctionNode(atom);
        if (function_node && not_equals_functions.contains(functionName(atom)))
        {
            const auto & arguments = getArguments(function_node);
            if (arguments.size() == 2)
            {
                auto index_left = graph.node_hash_to_component.at(Graph::getHash(arguments[0]));
                auto index_right = graph.node_hash_to_component.at(Graph::getHash(arguments[1]));

                if (index_left == index_right)
                {
                    throw Exception(ErrorCodes::VIOLATED_CONSTRAINT,
                        "Found expression '{}', but its arguments considered equal according to constraints",
                        nodeToString(atom));
                }

                not_equal.emplace(index_left, index_right);
                not_equal.emplace(index_right, index_left);
            }
        }
    }
}

template <ComparisonGraphNodeType Node>
ComparisonGraphCompareResult ComparisonGraph<Node>::pathToCompareResult(Path path, bool inverse)
{
    switch (path)
    {
        case Path::GREATER: return inverse ? ComparisonGraphCompareResult::LESS : ComparisonGraphCompareResult::GREATER;
        case Path::GREATER_OR_EQUAL: return inverse ? ComparisonGraphCompareResult::LESS_OR_EQUAL : ComparisonGraphCompareResult::GREATER_OR_EQUAL;
    }
}

template <ComparisonGraphNodeType Node>
std::optional<typename ComparisonGraph<Node>::Path> ComparisonGraph<Node>::findPath(size_t start, size_t finish) const
{
    const auto it = dists.find(std::make_pair(start, finish));
    if (it == std::end(dists))
        return {};

    /// Since path can be only GREATER or GREATER_OR_EQUALS,
    /// we can strengthen the condition.
    return not_equal.contains({start, finish}) ? Path::GREATER : it->second;
}

template <ComparisonGraphNodeType Node>
ComparisonGraphCompareResult ComparisonGraph<Node>::compare(const Node & left, const Node & right) const
{
    size_t start = 0;
    size_t finish = 0;

    /// TODO: check full ast
    const auto it_left = graph.node_hash_to_component.find(Graph::getHash(left));
    const auto it_right = graph.node_hash_to_component.find(Graph::getHash(right));

    if (it_left == std::end(graph.node_hash_to_component) || it_right == std::end(graph.node_hash_to_component))
    {
        auto result = ComparisonGraphCompareResult::UNKNOWN;
        {
            const auto left_bound = getConstLowerBound(left);
            const auto right_bound = getConstUpperBound(right);

            if (left_bound && right_bound)
            {
                if (greater(left_bound->first, right_bound->first))
                    result = ComparisonGraphCompareResult::GREATER;
                else if (equals(left_bound->first, right_bound->first))
                    result = left_bound->second || right_bound->second
                        ? ComparisonGraphCompareResult::GREATER : ComparisonGraphCompareResult::GREATER_OR_EQUAL;
            }
        }
        {
            const auto left_bound = getConstUpperBound(left);
            const auto right_bound = getConstLowerBound(right);

            if (left_bound && right_bound)
            {
                if (less(left_bound->first, right_bound->first))
                    result = ComparisonGraphCompareResult::LESS;
                else if (equals(left_bound->first, right_bound->first))
                    result = left_bound->second || right_bound->second
                        ? ComparisonGraphCompareResult::LESS : ComparisonGraphCompareResult::LESS_OR_EQUAL;
            }
        }

        return result;
    }

    start = it_left->second;
    finish = it_right->second;


    if (start == finish)
        return ComparisonGraphCompareResult::EQUAL;

    if (auto path = findPath(start, finish))
        return pathToCompareResult(*path, /*inverse=*/ false);

    if (auto path = findPath(finish, start))
        return pathToCompareResult(*path, /*inverse=*/ true);

    if (not_equal.contains({start, finish}))
        return ComparisonGraphCompareResult::NOT_EQUAL;

    return ComparisonGraphCompareResult::UNKNOWN;
}

template <ComparisonGraphNodeType Node>
bool ComparisonGraph<Node>::isPossibleCompare(ComparisonGraphCompareResult expected, const Node & left, const Node & right) const
{
    const auto result = compare(left, right);

    using enum ComparisonGraphCompareResult;
    if (expected == UNKNOWN || result == UNKNOWN)
        return true;

    if (expected == result)
        return true;

    static const std::set<std::pair<ComparisonGraphCompareResult, ComparisonGraphCompareResult>> possible_pairs =
    {
        {EQUAL, LESS_OR_EQUAL},
        {EQUAL, GREATER_OR_EQUAL},
        {LESS_OR_EQUAL, LESS},
        {LESS_OR_EQUAL, EQUAL},
        {LESS_OR_EQUAL, NOT_EQUAL},
        {GREATER_OR_EQUAL, GREATER},
        {GREATER_OR_EQUAL, EQUAL},
        {GREATER_OR_EQUAL, NOT_EQUAL},
        {LESS, LESS},
        {LESS, LESS_OR_EQUAL},
        {LESS, NOT_EQUAL},
        {GREATER, GREATER},
        {GREATER, GREATER_OR_EQUAL},
        {GREATER, NOT_EQUAL},
        {NOT_EQUAL, LESS},
        {NOT_EQUAL, GREATER},
        {NOT_EQUAL, LESS_OR_EQUAL},
        {NOT_EQUAL, GREATER_OR_EQUAL},
    };

    return possible_pairs.contains({expected, result});
}

template <ComparisonGraphNodeType Node>
bool ComparisonGraph<Node>::isAlwaysCompare(ComparisonGraphCompareResult expected, const Node & left, const Node & right) const
{
    const auto result = compare(left, right);

    using enum ComparisonGraphCompareResult;
    if (expected == UNKNOWN || result == UNKNOWN)
        return false;

    if (expected == result)
        return true;

    static const std::set<std::pair<ComparisonGraphCompareResult, ComparisonGraphCompareResult>> possible_pairs =
    {
        {LESS_OR_EQUAL, LESS},
        {LESS_OR_EQUAL, EQUAL},
        {GREATER_OR_EQUAL, GREATER},
        {GREATER_OR_EQUAL, EQUAL},
        {NOT_EQUAL, GREATER},
        {NOT_EQUAL, LESS},
    };

    return possible_pairs.contains({expected, result});
}


template <ComparisonGraphNodeType Node>
typename ComparisonGraph<Node>::NodeContainer ComparisonGraph<Node>::getEqual(const Node & node) const
{
    const auto res = getComponentId(node);
    if (!res)
        return {};
    return getComponent(res.value());
}

template <ComparisonGraphNodeType Node>
std::optional<size_t> ComparisonGraph<Node>::getComponentId(const Node & node) const
{
    const auto node_hash = Graph::getHash(node);
    const auto hash_it = graph.node_hash_to_component.find(node_hash);

    if (hash_it == std::end(graph.node_hash_to_component))
        return {};

    const size_t index = hash_it->second;
    if (std::any_of(
        std::cbegin(graph.vertices[index].nodes),
        std::cend(graph.vertices[index].nodes),
        [node](const Node & constraint_node)
        {
            if constexpr (with_ast)
                return constraint_node->getTreeHash(/*ignore_aliases=*/ true) == node->getTreeHash(/*ignore_aliases=*/ true)
                    && constraint_node->getColumnName() == node->getColumnName();
            else if constexpr (with_actions_dag)
                return areNodesEqual(constraint_node, node);
            else
                return constraint_node->getTreeHash() == node->getTreeHash();
        }))
    {
        return index;
    }

    return {};
}

template <ComparisonGraphNodeType Node>
bool ComparisonGraph<Node>::hasPath(size_t left, size_t right) const
{
    return findPath(left, right) || findPath(right, left);
}

template <ComparisonGraphNodeType Node>
typename ComparisonGraph<Node>::NodeContainer ComparisonGraph<Node>::getComponent(size_t id) const
{
    return graph.vertices[id].nodes;
}

template <ComparisonGraphNodeType Node>
bool GraphComponent<Node>::EqualComponent::hasConstant() const
{
    return constant_index.has_value();
}

template <ComparisonGraphNodeType Node>
Node GraphComponent<Node>::EqualComponent::getConstant() const
{
    chassert(constant_index);
    return nodes[*constant_index];
}

template <ComparisonGraphNodeType Node>
void GraphComponent<Node>::EqualComponent::buildConstants()
{
    constant_index.reset();
    for (size_t i = 0; i < nodes.size(); ++i)
    {
        if (tryGetConstantValue(nodes[i]))
        {
            constant_index = i;
            return;
        }
    }
}

template <ComparisonGraphNodeType Node>
ComparisonGraphCompareResult ComparisonGraph<Node>::atomToCompareResult(const Formula & atom)
{
    const auto & node = getNode(atom);
    if (tryGetFunctionNode(node) != nullptr)
    {
        auto expected = functionNameToCompareResult(functionName(node));
        if (atom.negative)
            expected = inverseCompareResult(expected);
        return expected;
    }

    return ComparisonGraphCompareResult::UNKNOWN;
}

template <ComparisonGraphNodeType Node>
std::optional<Node> ComparisonGraph<Node>::getEqualConst(const Node & node) const
{
    const auto hash_it = graph.node_hash_to_component.find(Graph::getHash(node));
    if (hash_it == std::end(graph.node_hash_to_component))
        return std::nullopt;

    const size_t index = hash_it->second;

    if (!graph.vertices[index].hasConstant())
        return std::nullopt;

    if constexpr (with_ast)
        return graph.vertices[index].getConstant();
    else if constexpr (with_actions_dag)
        return graph.vertices[index].getConstant();
    else
    {
        const auto & constant = getConstantValue(graph.vertices[index].getConstant());
        auto constant_node = std::make_shared<ConstantNode>(constant, node->getResultType());
        return constant_node;
    }
}

template <ComparisonGraphNodeType Node>
std::optional<std::pair<Field, bool>> ComparisonGraph<Node>::getConstUpperBound(const Node & node) const
{
    if (const auto constant = tryGetConstantValue(node))
        return std::make_pair(*constant, false);

    const auto it = graph.node_hash_to_component.find(Graph::getHash(node));
    if (it == std::end(graph.node_hash_to_component))
        return std::nullopt;

    const size_t to = it->second;
    const ssize_t from = node_const_upper_bound[to];
    if (from == -1)
        return  std::nullopt;

    return std::make_pair(getConstantValue(graph.vertices[from].getConstant()), dists.at({from, to}) == Path::GREATER);
}

template <ComparisonGraphNodeType Node>
std::optional<std::pair<Field, bool>> ComparisonGraph<Node>::getConstLowerBound(const Node & node) const
{
    if (const auto constant = tryGetConstantValue(node))
        return std::make_pair(*constant, false);

    const auto it = graph.node_hash_to_component.find(Graph::getHash(node));
    if (it == std::end(graph.node_hash_to_component))
        return std::nullopt;

    const size_t from = it->second;
    const ssize_t to = node_const_lower_bound[from];
    if (to == -1)
        return std::nullopt;

    return std::make_pair(getConstantValue(graph.vertices[to].getConstant()), dists.at({from, to}) == Path::GREATER);
}

template <ComparisonGraphNodeType Node>
void ComparisonGraph<Node>::dfsOrder(const Graph & nodes_graph, size_t v, std::vector<bool> & visited, std::vector<size_t> & order)
{
    visited[v] = true;
    for (const auto & edge : nodes_graph.edges[v])
        if (!visited[edge.to])
            dfsOrder(nodes_graph, edge.to, visited, order);

    order.push_back(v);
}

template <ComparisonGraphNodeType Node>
typename ComparisonGraph<Node>::Graph ComparisonGraph<Node>::reverseGraph(const Graph & nodes_graph)
{
    Graph g;
    g.node_hash_to_component = nodes_graph.node_hash_to_component;
    g.vertices = nodes_graph.vertices;
    g.edges.resize(g.vertices.size());

    for (size_t v = 0; v < nodes_graph.vertices.size(); ++v)
        for (const auto & edge : nodes_graph.edges[v])
            g.edges[edge.to].push_back({edge.type, v});

    return g;
}

template <ComparisonGraphNodeType Node>
std::vector<typename ComparisonGraph<Node>::NodeContainer> ComparisonGraph<Node>::getVertices() const
{
    std::vector<NodeContainer> result;
    for (const auto & vertex : graph.vertices)
    {
        result.emplace_back();
        for (const auto & node : vertex.nodes)
            result.back().push_back(node);
    }
    return result;
}

template <ComparisonGraphNodeType Node>
void ComparisonGraph<Node>::dfsComponents(
    const Graph & reversed_graph, size_t v,
    OptionalIndices & components, size_t component)
{
    components[v] = component;
    for (const auto & edge : reversed_graph.edges[v])
        if (!components[edge.to])
            dfsComponents(reversed_graph, edge.to, components, component);
}

template <ComparisonGraphNodeType Node>
typename ComparisonGraph<Node>::Graph ComparisonGraph<Node>::buildGraphFromNodesGraph(const Graph & nodes_graph)
{
    /// Find strongly connected component by using 2 dfs traversals.
    /// https://en.wikipedia.org/wiki/Kosaraju%27s_algorithm
    const auto n = nodes_graph.vertices.size();

    std::vector<size_t> order;
    {
        std::vector<bool> visited(n, false);
        for (size_t v = 0; v < n; ++v)
        {
            if (!visited[v])
                dfsOrder(nodes_graph, v, visited, order);
        }
    }

    OptionalIndices components(n);
    size_t component = 0;
    {
        const Graph reversed_graph = reverseGraph(nodes_graph);
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
    for (const auto & [hash, index] : nodes_graph.node_hash_to_component)
    {
        chassert(components[index]);
        result.node_hash_to_component[hash] = *components[index];
        result.vertices[*components[index]].nodes.insert(
            std::end(result.vertices[*components[index]].nodes),
            std::begin(nodes_graph.vertices[index].nodes),
            std::end(nodes_graph.vertices[index].nodes)); // asts_graph has only one ast per vertex
    }

    /// Calculate constants
    for (auto & vertex : result.vertices)
        vertex.buildConstants();

    /// For each edge in initial graph, we add an edge between components in condensation graph.
    for (size_t v = 0; v < n; ++v)
    {
        for (const auto & edge : nodes_graph.edges[v])
            result.edges[*components[v]].push_back({edge.type, *components[edge.to]});

        /// TODO: make edges unique (left most strict)
    }

    /// If we have constansts in two components, we can compare them and add and extra edge.
    for (size_t v = 0; v < result.vertices.size(); ++v)
    {
        for (size_t u = 0; u < result.vertices.size(); ++u)
        {
            if (v != u && result.vertices[v].hasConstant() && result.vertices[u].hasConstant())
            {
                const auto left = getConstantValue(result.vertices[v].getConstant());
                const auto right = getConstantValue(result.vertices[u].getConstant());

                /// Only GREATER. Equal constant fields = equal literals so it was already considered above.
                if (greater(left, right))
                    result.edges[v].push_back({Graph::Edge::GREATER, u});
            }
        }
    }

    return result;
}

template <ComparisonGraphNodeType Node>
std::map<std::pair<size_t, size_t>, typename ComparisonGraph<Node>::Path> ComparisonGraph<Node>::buildDistsFromGraph(const Graph & g)
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
            results[v][edge.to] = std::min(results[v][edge.to], static_cast<Int8>(edge.type == Graph::Edge::GREATER ? -1 : 0));
    }

    for (size_t k = 0; k < n; ++k)
        for (size_t v = 0; v < n; ++v)
            for (size_t u = 0; u < n; ++u)
                if (results[v][k] != inf && results[k][u] != inf)
                    results[v][u] = std::min({results[v][u], results[v][k], results[k][u]});

    std::map<std::pair<size_t, size_t>, Path> path;
    for (size_t v = 0; v < n; ++v)
        for (size_t u = 0; u < n; ++u)
            if (results[v][u] != inf)
                path[std::make_pair(v, u)] = (results[v][u] == -1 ? Path::GREATER : Path::GREATER_OR_EQUAL);

    return path;
}

template <ComparisonGraphNodeType Node>
std::pair<std::vector<ssize_t>, std::vector<ssize_t>> ComparisonGraph<Node>::buildConstBounds() const
{
    const size_t n = graph.vertices.size();
    std::vector<ssize_t> lower(n, -1);
    std::vector<ssize_t> upper(n, -1);

    auto get_value = [this](const size_t vertex) -> Field
    {
        return getConstantValue(graph.vertices[vertex].getConstant());
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

    return {std::move(lower), std::move(upper)};
}

template struct GraphComponent<ASTPtr>;
template struct GraphComponent<QueryTreeNodePtr>;
template struct GraphComponent<const ActionsDAG::Node *>;
template class ComparisonGraph<ASTPtr>;
template class ComparisonGraph<QueryTreeNodePtr>;
template class ComparisonGraph<const ActionsDAG::Node *>;

}
