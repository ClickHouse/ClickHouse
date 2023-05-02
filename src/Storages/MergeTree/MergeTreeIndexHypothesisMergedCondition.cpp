#include <Storages/MergeTree/MergeTreeIndexHypothesisMergedCondition.h>

#include <Storages/MergeTree/MergeTreeIndexHypothesis.h>
#include <Storages/MergeTree/MergeTreeIndexUtils.h>
#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/ComparisonGraph.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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

template <bool use_ast>
auto convertToCNF(ASTPtr expression, const ContextPtr & context, const QueryTreeNodePtr & table_expression)
{
    if constexpr (use_ast)
    {
        return TreeCNFConverter::toCNF(expression);
    }
    else
    {
        auto query_tree_node = buildQueryTree(expression, context);
        assert(table_expression);
        QueryAnalysisPass pass(table_expression);
        pass.run(query_tree_node, context);
        return Analyzer::CNF::toCNF(query_tree_node, context);
    }
}

Analyzer::CNF::AtomicFormula cloneAtomAndPushNot(const Analyzer::CNF::AtomicFormula & atom, const ContextPtr & context)
{
    Analyzer::CNF::AtomicFormula new_atom{atom.negative, atom.node_with_hash.node->clone()};
    return Analyzer::CNF::pushNotIntoFunction(new_atom, context);
}

CNFQuery::AtomicFormula cloneAtomAndPushNot(const CNFQuery::AtomicFormula & atom, const ContextPtr &)
{
    CNFQuery::AtomicFormula new_atom {atom.negative, atom.ast->clone()};
    pushNotIn(new_atom);
    return new_atom;
}

const std::string * functionName(const CNFQuery::AtomicFormula & atom)
{
    const auto * func = atom.ast->as<ASTFunction>();

    if (func)
        return &func->name;

    return nullptr;
}

const std::string * functionName(const Analyzer::CNF::AtomicFormula & atom)
{
    const auto * function = atom.node_with_hash.node->as<FunctionNode>();

    if (function)
        return &function->getFunctionName();

    return nullptr;
}

bool isFunction(const CNFQuery::AtomicFormula & atom)
{
    return atom.ast->as<ASTFunction>() != nullptr;
}

bool isFunction(const Analyzer::CNF::AtomicFormula & atom)
{
    return atom.node_with_hash.node->as<FunctionNode>() != nullptr;
}

const auto & functionArguments(const CNFQuery::AtomicFormula & atom)
{
    return atom.ast->as<ASTFunction &>().arguments->children;
}

const auto & functionArguments(const Analyzer::CNF::AtomicFormula & atom)
{
    return atom.node_with_hash.node->as<FunctionNode &>().getArguments().getNodes();
}

template <typename Node>
class IndexImpl : public MergeTreeIndexhypothesisMergedCondition::IIndexImpl
{
public:
    IndexImpl(const SelectQueryInfo & query_info, const ConstraintsDescription & constraints, ContextPtr context_)
        : context(std::move(context_))
        , table_expression(query_info.table_expression)
    {
        auto ast_filter_node = buildFilterNode(query_info.query);

        if (ast_filter_node)
            expression_cnf = std::make_unique<CNF>(convertToCNF<is_ast>(ast_filter_node, context, table_expression));
        else
            expression_cnf = std::make_unique<CNF>(typename CNF::AndGroup{});

        auto atomic_constraints_data = [&]
        {
            if constexpr (is_ast)
                return constraints.getAtomicConstraintData();
            else
                return constraints.getQueryTreeData(context, table_expression).getAtomicConstraintData();
        }();

        for (const auto & atomic_formula : atomic_constraints_data)
        {
            auto atom = cloneAtomAndPushNot(atomic_formula, context);

            if constexpr (is_ast)
                atomic_constraints.push_back(std::move(atom.ast));
            else
                atomic_constraints.push_back(std::move(atom.node_with_hash.node));
        }
    }

    void addIndex(const MergeTreeIndexHypothesis & hypothesis_index) override
    {
        static const NameSet relations = { "equals", "notEquals", "less", "lessOrEquals", "greaterOrEquals", "greater"};

        // TODO: move to index hypothesis
        std::vector<Node> compare_hypotheses_data;
        std::vector<typename CNF::OrGroup> hypotheses_data;
        auto cnf = convertToCNF<is_ast>(hypothesis_index.index.expression_list_ast->children.front(), context, table_expression);

        if constexpr (is_ast)
            cnf.pullNotOutFunctions();
        else
            cnf.pullNotOutFunctions(context);

        for (const auto & group : cnf.getStatements())
        {
            if (group.size() == 1)
            {
                hypotheses_data.push_back(group);
                auto atomic_formula = *group.begin();
                auto atom = cloneAtomAndPushNot(atomic_formula, context);

                assert(!atom.negative);

                const auto * function_name = functionName(atom);
                if (function_name && relations.contains(*function_name))
                {
                    if constexpr (is_ast)
                        compare_hypotheses_data.push_back(atom.ast);
                    else
                        compare_hypotheses_data.push_back(atom.node_with_hash.node);
                }
            }
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

        auto less_or_equals_resolver = FunctionFactory::instance().get("lessOrEquals", context);
        auto greater_or_equals_resolver = FunctionFactory::instance().get("greaterOrEquals", context);
        /// transform active formulas
        for (auto & formula : active_atomic_formulas)
        {
            formula = formula->clone(); /// do all operations with copy

            if constexpr (is_ast)
            {
                auto * func = formula->template as<ASTFunction>();
                if (func)
                {
                    if (func->name == "less")
                        func->name = "lessOrEquals";
                    else if (func->name == "greater")
                        func->name = "greaterOrEquals";
                }
            }
            else
            {
                auto * func = formula->template as<FunctionNode>();
                if (func)
                {
                    if (func->getFunctionName() == "less")
                        func->resolveAsFunction(less_or_equals_resolver);
                    else if (func->getFunctionName() == "greater")
                        func->resolveAsFunction(greater_or_equals_resolver);
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
                    auto atom = cloneAtomAndPushNot(atomic_formula, context);

                    if (isFunction(atom))
                    {
                        const auto & arguments = functionArguments(atom);

                        if (arguments.size() == 2)
                        {
                            const auto expected = ComparisonGraph<Node>::atomToCompareResult(atom);
                            if (graph->isPossibleCompare(expected, arguments[0], arguments[1]))
                            {
                                /// If graph failed use matching.
                                /// We don't need to check constraints.
                                return;
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

    using CNF = std::conditional_t<is_ast, CNFQuery, Analyzer::CNF>;
    using Nodes = std::conditional_t<is_ast, ASTs, QueryTreeNodes>;

    std::unique_ptr<CNF> expression_cnf;

    /// Part analysis can be done in parallel.
    /// So, we have shared answer and graph cache.
    mutable std::mutex cache_mutex;
    mutable std::unordered_map<std::vector<bool>, std::unique_ptr<ComparisonGraph<Node>>> graph_cache;
    mutable std::unordered_map<std::vector<bool>, bool> answer_cache;

    std::vector<std::vector<Node>> index_to_compare_atomic_hypotheses;
    std::vector<std::vector<typename CNF::OrGroup>> index_to_atomic_hypotheses;
    Nodes atomic_constraints;

    ContextPtr context;
    QueryTreeNodePtr table_expression;
};

}

MergeTreeIndexhypothesisMergedCondition::MergeTreeIndexhypothesisMergedCondition(
    const SelectQueryInfo & query_info, const ConstraintsDescription & constraints, size_t granularity_, ContextPtr context)
    : IMergeTreeIndexMergedCondition(granularity_)
{
    if (context->getSettingsRef().allow_experimental_analyzer)
        impl = std::make_unique<IndexImpl<QueryTreeNodePtr>>(query_info, constraints, std::move(context));
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
