#include <Storages/MergeTree/MergeTreeIndexHypothesisMergedCondition.h>

#include <Core/Settings.h>
#include <Storages/MergeTree/MergeTreeIndexHypothesis.h>
#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/ActionsDAGCNF.h>
#include <Interpreters/ComparisonGraph.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/Passes/CNF.h>
#include <Analyzer/Utils.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/PlannerCorrelatedSubqueries.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

class MergeTreeIndexhypothesisMergedCondition::IIndexImpl
{
public:
    virtual void addIndex(const MergeTreeIndexHypothesis & hypothesis_index) = 0;
    virtual bool alwaysUnknownOrTrue() const = 0;
    virtual bool mayBeTrueOnGranule(const std::vector<bool> & values) const = 0;

    virtual ~IIndexImpl() = default;
};


namespace
{

auto convertToCNF(ASTPtr expression, const ContextPtr &, const QueryTreeNodePtr &)
{
    return TreeCNFConverter::toCNF(expression);
}

auto convertToCNF(const ActionsDAG::Node * expression, const ContextPtr & context, const QueryTreeNodePtr &)
{
    return ActionsDAGCNF::toCNF(expression, context);
}

/// WARNING: This function is DANGEROUS for ActionsDAG because it creates nodes in a temporary
/// stack-allocated DAG that gets destroyed immediately, leaving dangling pointers!
/// It is removed to prevent accidental misuse. For ActionsDAG, always use nodes directly from
/// a stored CNF or create nodes in a persistent DAG that outlives their usage.
///
/// OLD DANGEROUS CODE (DO NOT USE):
/// ActionsDAGCNF::AtomicFormula cloneAtomAndPushNot(const ActionsDAGCNF::AtomicFormula & atom, const ContextPtr & context)
/// {
///     ActionsDAG temp_dag;  // DANGER: temp_dag destroyed here!
///     return ActionsDAGCNF::pushNotIntoFunction(atom, temp_dag, context);  // Returns dangling pointers!
/// }

/// This version is safe for AST because cloned ASTs are independent
CNFQueryAtomicFormula cloneAtomAndPushNot(const CNFQueryAtomicFormula & atom, const ContextPtr &)
{
    CNFQueryAtomicFormula new_atom {atom.negative, atom.ast->clone()};
    pushNotIn(new_atom);
    return new_atom;
}

/// ============================================================================
/// Helper functions to abstract differences between AST and ActionsDAG
/// ============================================================================
/// These overloaded functions provide a uniform interface for working with
/// atomic formulas from both AST-based and ActionsDAG-based CNF representations.
/// This reduces code duplication in the template IndexImpl class below.
/// ============================================================================

std::optional<std::string> functionName(const CNFQueryAtomicFormula & atom)
{
    const auto * func = atom.ast->as<ASTFunction>();
    if (func)
        return func->name;
    return std::nullopt;
}

std::optional<std::string> functionName(const ActionsDAGCNF::AtomicFormula & atom)
{
    const auto * node = atom.node_with_hash.node;
    if (node && node->type == ActionsDAG::ActionType::FUNCTION && node->function_base)
        return node->function_base->getName();
    return std::nullopt;
}

bool isFunction(const CNFQueryAtomicFormula & atom)
{
    return atom.ast->as<ASTFunction>() != nullptr;
}

bool isFunction(const ActionsDAGCNF::AtomicFormula & atom)
{
    const auto * node = atom.node_with_hash.node;
    return node && node->type == ActionsDAG::ActionType::FUNCTION;
}

const auto & functionArguments(const CNFQueryAtomicFormula & atom)
{
    return atom.ast->as<ASTFunction &>().arguments->children;
}

const auto & functionArguments(const ActionsDAGCNF::AtomicFormula & atom)
{
    return atom.node_with_hash.node->children;
}

const ActionsDAG::Node * buildFilterNode(const std::shared_ptr<const ActionsDAG> & filter_dag, ContextPtr /* context */)
{
    // For ActionsDAG, the filter is already built - just return the single output node
    if (!filter_dag)
        return nullptr;

    const auto & outputs = filter_dag->getOutputs();
    if (outputs.empty())
        return nullptr;

    // Assuming the filter DAG has a single output which is the combined filter expression
    return outputs[0];
}

ASTPtr buildFilterNode(const ASTPtr & select_query, ASTs additional_filters)
{
    auto & select_query_typed = select_query->as<ASTSelectQuery &>();

    ASTs filters;
    if (select_query_typed.where())
        filters.push_back(select_query_typed.where());

    if (select_query_typed.prewhere())
        filters.push_back(select_query_typed.prewhere());

    filters.insert(filters.end(), additional_filters.begin(), additional_filters.end());

    if (filters.empty())
        return nullptr;

    ASTPtr filter_node;

    if (filters.size() == 1)
    {
        filter_node = filters.front();
    }
    else
    {
        auto function = std::make_shared<ASTFunction>();

        function->name = "and";
        function->arguments = std::make_shared<ASTExpressionList>();
        function->children.push_back(function->arguments);
        function->arguments->children = std::move(filters);

        filter_node = std::move(function);
    }

    return filter_node;
}

/// ============================================================================
/// IndexImpl: Template implementation for both AST and ActionsDAG paths
/// ============================================================================
/// Key differences in handling:
/// 1. AST: Nodes can be freely cloned and transformed (independent copies)
/// 2. ActionsDAG: Nodes are immutable and must maintain DAG lifetime (shared ownership)
///
/// - ActionsDAG requires careful lifetime management - nodes must be stored in persistent DAGs
/// - AST allows cloning via cloneAtomAndPushNot(), but this creates dangling pointers for ActionsDA

template <typename Node>
class IndexImpl : public MergeTreeIndexhypothesisMergedCondition::IIndexImpl
{
public:
    IndexImpl(const SelectQueryInfo & query_info, const ConstraintsDescription & constraints, ContextPtr context_)
        : context(std::move(context_))
        , table_expression(query_info.table_expression)
    {
        auto filter_node = [&]
        {
            if constexpr (is_ast)
                return buildFilterNode(query_info.query, query_info.filter_asts);
            else
                return buildFilterNode(query_info.filter_actions_dag, context);
        }();

        if (filter_node)
            expression_cnf = std::make_unique<CNF>(convertToCNF(filter_node, context, table_expression));
        else
            expression_cnf = std::make_unique<CNF>(typename CNF::AndGroup{});

        /// For ActionsDAG, store the expression CNF's owned DAG to keep nodes alive
        if constexpr (is_actions_dag)
        {
            if (auto owned_dag = expression_cnf->getOwnedDag())
                expression_dag = owned_dag;
        }

        if constexpr (is_ast)
        {
            auto atomic_constraints_data = constraints.getAtomicConstraintData();
            for (const auto & atomic_formula : atomic_constraints_data)
            {
                auto atom = cloneAtomAndPushNot(atomic_formula, context);
                atomic_constraints.push_back(std::move(atom.ast));
            }
        }
        else
        {
            /// For ActionsDAG, we need to carefully manage node lifetimes
            auto constraints_data = constraints.getActionsDAGData(context, table_expression);
            auto atomic_constraints_data = constraints_data.getAtomicConstraintData();

            /// Store the constraints DAG to keep nodes alive
            if (auto owned_dag = constraints_data.getOwnedDag())
                constraints_dag = owned_dag;

            /// Use nodes directly from constraints_data without cloning
            for (const auto & atomic_formula : atomic_constraints_data)
            {
                /// If the formula is negative, we would need to transform it
                /// But for now, we skip negative formulas in constraints
                /// (this matches the behavior for hypothesis indices)
                if (atomic_formula.negative)
                    continue;

                atomic_constraints.push_back(atomic_formula.node_with_hash.node);
            }
        }
    }

    void addIndex(const MergeTreeIndexHypothesis & hypothesis_index) override
    {
        static const NameSet relations = { "equals", "notEquals", "less", "lessOrEquals", "greaterOrEquals", "greater"};

        // TODO: move to index hypothesis
        std::vector<Node> compare_hypotheses_data;
        std::vector<typename CNF::OrGroup> hypotheses_data;

        auto cnf = [&]
        {
            auto expression_ast = hypothesis_index.index.expression_list_ast->children.front();

            if constexpr (is_ast)
            {
                return convertToCNF(expression_ast, context, table_expression);
            }
            else
            {
                /// Convert AST → QueryTree → ActionsDAG → CNF
                auto query_tree_node = buildQueryTree(expression_ast, context);

                /// Run query analysis to resolve columns
                QueryAnalysisPass pass(table_expression);
                pass.run(query_tree_node, context);

                auto execution_context = Context::createCopy(context);
                auto global_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, FiltersForTableExpressionMap{});
                SelectQueryOptions select_query_options;
                auto planner_context = std::make_shared<PlannerContext>(
                    execution_context,
                    std::move(global_planner_context),
                    select_query_options);

                auto mutable_query_tree = query_tree_node;
                collectSourceColumns(mutable_query_tree, planner_context);

                ActionsDAG actions_dag;
                ColumnNodePtrWithHashSet empty_correlated_columns_set;
                PlannerActionsVisitor visitor(planner_context, empty_correlated_columns_set);
                auto [actions_dag_nodes, correlated_subtrees] = visitor.visit(actions_dag, mutable_query_tree);

                if (actions_dag_nodes.empty())
                    return ActionsDAGCNF(ActionsDAGCNF::AndGroup{});

                const auto * dag_node = actions_dag_nodes[0];

                /// Convert to CNF while actions_dag is still in scope
                return convertToCNF(dag_node, context, table_expression);
            }
        }();

        // NOTE: We do NOT call pullNotOutFunctions() for hypothesis indices because we want to keep
        // functions like notEquals, greater, etc. as-is for the ComparisonGraph to process.
        // pullNotOutFunctions would convert "a != b" to "NOT (a = b)" which makes the atomic formula
        // negative, and we would skip it.

        for (const auto & group : cnf.getStatements())
        {
            if (group.size() == 1)
            {
                hypotheses_data.push_back(group);
                auto atomic_formula = *group.begin();

                // For ActionsDAG, we need to use the node directly from the CNF, not clone it
                // because cloning creates a temporary DAG that gets destroyed
                if constexpr (is_actions_dag)
                {
                    // Use the node directly from atomic_formula, checking if it needs NOT pushed in
                    const auto * node = atomic_formula.node_with_hash.node;
                    bool negative = atomic_formula.negative;

                    // If negative, we would need to push NOT in, but that requires creating new nodes
                    // For now, assert that hypotheses are not negative (they should be normalized)
                    if (negative)
                        continue;

                    const auto function_name = functionName(atomic_formula);
                    if (function_name && relations.contains(*function_name))
                    {
                        compare_hypotheses_data.push_back(node);
                    }
                }
                else
                {
                    auto atom = cloneAtomAndPushNot(atomic_formula, context);
                    assert(!atom.negative);

                    const auto function_name = functionName(atom);
                    if (function_name && relations.contains(*function_name))
                    {
                        compare_hypotheses_data.push_back(atom.ast);
                    }
                }
            }
        }

        /// For ActionsDAG, store the DAG so the node pointers remain valid
        if constexpr (is_actions_dag)
        {
            auto owned_dag = cnf.getOwnedDag();
            hypothesis_index_dags.push_back(owned_dag);
        }

        index_to_compare_atomic_hypotheses.push_back(compare_hypotheses_data);
        index_to_atomic_hypotheses.push_back(hypotheses_data);
    }

    bool alwaysUnknownOrTrue() const override
    {
        Nodes active_atomic_formulas(atomic_constraints);
        for (const auto & hypothesis : index_to_compare_atomic_hypotheses)
        {
            active_atomic_formulas.insert(
                std::end(active_atomic_formulas),
                std::begin(hypothesis),
                std::end(hypothesis));
        }

        /// For ActionsDAG, we need a DAG to hold transformed nodes (less -> lessOrEquals, greater -> greaterOrEquals)
        /// This must outlive the weak_graph that uses these nodes
        /// NOTE: This DAG is created on each call, which involves some overhead. An optimization would be to
        /// store this as a member variable and reuse it, but that adds complexity for thread-safety
        std::unique_ptr<ActionsDAG> weak_graph_temp_dag;

        /// transform active formulas
        if constexpr (is_ast)
        {
            for (auto & formula : active_atomic_formulas)
            {
                formula = formula->clone(); /// do all operations with copy
                auto * func = formula->template as<ASTFunction>();
                if (func)
                {
                    if (func->name == "less")
                        func->name = "lessOrEquals";
                    else if (func->name == "greater")
                        func->name = "greaterOrEquals";
                }
            }
        }
        else if constexpr (is_actions_dag)
        {
            /// For ActionsDAG nodes, we need to transform less -> lessOrEquals and greater -> greaterOrEquals
            /// Since ActionsDAG nodes are immutable, we create new nodes with the transformed functions
            /// Optimization: We don't clone child nodes, we reuse them directly since they don't need transformation
            weak_graph_temp_dag = std::make_unique<ActionsDAG>();

            for (size_t i = 0; i < active_atomic_formulas.size(); ++i)
            {
                const auto * node = active_atomic_formulas[i];

                // Only transform function nodes with less or greater
                if (node && node->type == ActionsDAG::ActionType::FUNCTION && node->function_base)
                {
                    const auto & func_name = node->function_base->getName();

                    if (func_name == "less" || func_name == "greater")
                    {
                        // Get the new function name
                        std::string new_func_name = (func_name == "less") ? "lessOrEquals" : "greaterOrEquals";

                        // Reuse the original children directly - no need to clone them
                        // The ComparisonGraph only needs to compare nodes by semantic equality,
                        // so it's safe to mix nodes from different DAGs
                        auto function_resolver = FunctionFactory::instance().get(new_func_name, context);
                        const auto * new_node = &weak_graph_temp_dag->addFunction(function_resolver, node->children, node->result_name);

                        // Replace in active_atomic_formulas
                        active_atomic_formulas[i] = new_node;
                    }
                }
            }
        }

        const auto weak_graph = std::make_unique<ComparisonGraph<Node>>(active_atomic_formulas);

        bool useless = true;
        expression_cnf->iterateGroups(
            [&](const auto & or_group)
            {
                for (const auto & atomic_formula : or_group)
                {
                    if constexpr (is_ast)
                    {
                        auto atom = cloneAtomAndPushNot(atomic_formula, context);

                        if (isFunction(atom))
                        {
                            const auto & arguments = functionArguments(atom);
                            if (arguments.size() == 2)
                            {
                                const auto left = weak_graph->getComponentId(arguments[0]);
                                const auto right = weak_graph->getComponentId(arguments[1]);
                                if (left && right && weak_graph->hasPath(left.value(), right.value()))
                                {
                                    useless = false;
                                    return;
                                }
                            }
                        }
                    }
                    else
                    {
                        /// For ActionsDAG, use nodes directly from expression_cnf
                        /// Skip negative formulas as they would require transformation
                        if (atomic_formula.negative)
                            continue;

                        if (isFunction(atomic_formula))
                        {
                            const auto & arguments = functionArguments(atomic_formula);
                            if (arguments.size() == 2)
                            {
                                const auto left = weak_graph->getComponentId(arguments[0]);
                                const auto right = weak_graph->getComponentId(arguments[1]);
                                if (left && right && weak_graph->hasPath(left.value(), right.value()))
                                {
                                    useless = false;
                                    return;
                                }
                            }
                        }
                    }
                }
            });
        return useless;
    }

    bool mayBeTrueOnGranule(const std::vector<bool> & values) const override
    {
        const ComparisonGraph<Node> * graph = nullptr;

        {
            std::lock_guard lock(cache_mutex);
            if (const auto it = answer_cache.find(values); it != std::end(answer_cache))
                return it->second;

            graph = getGraph(values);
        }

        bool always_false = false;
        expression_cnf->iterateGroups(
            [&](const auto & or_group)
            {
                if (always_false)
                    return;

                for (const auto & atomic_formula : or_group)
                {
                    if constexpr (is_ast)
                    {
                        auto atom = cloneAtomAndPushNot(atomic_formula, context);

                        if (isFunction(atom))
                        {
                            const auto & arguments = functionArguments(atom);

                            if (arguments.size() == 2)
                            {
                                const auto expected = ComparisonGraph<Node>::atomToCompareResult(atom);
                                bool is_possible = graph->isPossibleCompare(expected, arguments[0], arguments[1]);
                                if (is_possible)
                                {
                                    /// If graph failed use matching.
                                    /// We don't need to check constraints.
                                    return;
                                }
                            }
                        }
                    }
                    else
                    {
                        /// For ActionsDAG, use nodes directly from expression_cnf
                        /// Skip negative formulas as they would require transformation
                        if (atomic_formula.negative)
                            continue;

                        if (isFunction(atomic_formula))
                        {
                            const auto & arguments = functionArguments(atomic_formula);

                            if (arguments.size() == 2)
                            {
                                const auto expected = ComparisonGraph<Node>::atomToCompareResult(atomic_formula);
                                bool is_possible = graph->isPossibleCompare(expected, arguments[0], arguments[1]);
                                if (is_possible)
                                {
                                    /// If graph failed use matching.
                                    /// We don't need to check constraints.
                                    return;
                                }
                            }
                        }
                    }
                }
                always_false = true;
           });

        std::lock_guard lock(cache_mutex);

        answer_cache[values] = !always_false;
        return !always_false;
    }
private:
    std::unique_ptr<ComparisonGraph<Node>> buildGraph(const std::vector<bool> & values) const
    {
        Nodes active_atomic_formulas(atomic_constraints);
        for (size_t i = 0; i < values.size(); ++i)
        {
            if (values[i])
                active_atomic_formulas.insert(
                    std::end(active_atomic_formulas),
                    std::begin(index_to_compare_atomic_hypotheses[i]),
                    std::end(index_to_compare_atomic_hypotheses[i]));
        }
        return std::make_unique<ComparisonGraph<Node>>(active_atomic_formulas, context);
    }

    const ComparisonGraph<Node> * getGraph(const std::vector<bool> & values) const
    {
        auto [it, inserted] = graph_cache.try_emplace(values);
        if (inserted)
            it->second = buildGraph(values);

        return it->second.get();
    }

    static constexpr bool is_ast = std::same_as<Node, ASTPtr>;
    static constexpr bool is_actions_dag = std::same_as<Node, const ActionsDAG::Node *>;

    using CNF = std::conditional_t<is_ast, CNFQuery, ActionsDAGCNF>;
    using Nodes = std::conditional_t<is_ast, ASTs, std::vector<const ActionsDAG::Node *>>;

    std::unique_ptr<CNF> expression_cnf;

    /// Part analysis can be done in parallel.
    /// So, we have shared answer and graph cache.
    mutable std::mutex cache_mutex;
    mutable std::unordered_map<std::vector<bool>, std::unique_ptr<ComparisonGraph<Node>>> graph_cache;
    mutable std::unordered_map<std::vector<bool>, bool> answer_cache;

    std::vector<std::vector<Node>> index_to_compare_atomic_hypotheses;
    std::vector<std::vector<typename CNF::OrGroup>> index_to_atomic_hypotheses;
    Nodes atomic_constraints;

    /// ========================================================================
    /// ActionsDAG Lifetime Management (only used when Node = const ActionsDAG::Node*)
    /// ========================================================================
    /// ActionsDAG nodes are raw pointers that must be kept alive by their owning DAG.
    /// We store shared_ptrs to the DAGs to ensure nodes remain valid throughout the
    /// lifetime of this IndexImpl instance.
    ///
    /// DAG ownership strategy:
    /// - hypothesis_index_dags: Stores DAGs from each hypothesis index's CNF
    /// - constraints_dag: Stores the DAG containing constraint nodes
    /// - expression_dag: Stores the DAG from expression_cnf (query filter)
    ///
    /// Why we need this:
    /// Unlike AST nodes which are reference-counted shared_ptrs, ActionsDAG nodes
    /// are raw pointers owned by their DAG. If the DAG is destroyed, all node
    /// pointers become dangling. By storing the DAGs, we ensure correct lifetime.
    /// ========================================================================
    std::vector<std::shared_ptr<ActionsDAG>> hypothesis_index_dags;
    std::shared_ptr<ActionsDAG> constraints_dag;
    std::shared_ptr<ActionsDAG> expression_dag;

    ContextPtr context;
    QueryTreeNodePtr table_expression;
};

}

MergeTreeIndexhypothesisMergedCondition::MergeTreeIndexhypothesisMergedCondition(
    const SelectQueryInfo & query_info, const ConstraintsDescription & constraints, size_t granularity_, ContextPtr context)
    : IMergeTreeIndexMergedCondition(granularity_)
{
    bool has_analyzer = context->getSettingsRef()[Setting::allow_experimental_analyzer];

    if (has_analyzer)
        impl = std::make_unique<IndexImpl<const ActionsDAG::Node *>>(query_info, constraints, std::move(context));
    else
        impl = std::make_unique<IndexImpl<ASTPtr>>(query_info, constraints, std::move(context));
}

void MergeTreeIndexhypothesisMergedCondition::addIndex(const MergeTreeIndexPtr & index)
{
    if (!index->isMergeable() || index->getGranularity() != granularity)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index {} can not be merged", index->index.type);

    const auto hypothesis_index = std::dynamic_pointer_cast<const MergeTreeIndexHypothesis>(index);
    if (!hypothesis_index)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Only hypothesis index is supported here");

    impl->addIndex(*hypothesis_index);
}

/// Replaces < -> <=, > -> >= and assumes that all hypotheses are true then checks if path exists
bool MergeTreeIndexhypothesisMergedCondition::alwaysUnknownOrTrue() const
{
    return impl->alwaysUnknownOrTrue();
}

bool MergeTreeIndexhypothesisMergedCondition::mayBeTrueOnGranule(const MergeTreeIndexGranules & granules) const
{
    std::vector<bool> values;
    values.reserve(granules.size());
    for (const auto & index_granule : granules)
    {
        const auto granule = std::dynamic_pointer_cast<const MergeTreeIndexGranuleHypothesis>(index_granule);
        if (!granule)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Only hypothesis index is supported here.");
        values.push_back(granule->met);
    }

    return impl->mayBeTrueOnGranule(values);
}

MergeTreeIndexhypothesisMergedCondition::~MergeTreeIndexhypothesisMergedCondition() = default;

}
