#include <Storages/MergeTree/MergeTreeIndexMergedCondition.h>

#include <Storages/MergeTree/MergeTreeIndexHypothesis.h>
#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/ComparisonGraph.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexMergedCondition::MergeTreeIndexMergedCondition(
    const SelectQueryInfo & query_,
    ContextPtr /*context_*/,
    const size_t granularity_,
    const NamesAndTypesList & source_columns_,
    const bool use_smt_)
    : granularity(granularity_)
    , source_columns(source_columns_)
    , use_smt(use_smt_)
{
    const auto & select = query_.query->as<ASTSelectQuery &>();

    if (select.where() && select.prewhere())
        expression_ast = makeASTFunction(
            "and",
            select.where()->clone(),
            select.prewhere()->clone());
    else if (select.where())
        expression_ast = select.where()->clone();
    else if (select.prewhere())
        expression_ast = select.prewhere()->clone();

    expression_cnf = std::make_unique<CNFQuery>(TreeCNFConverter::toCNF(expression_ast));
}

void MergeTreeIndexMergedCondition::addIndex(const MergeTreeIndexPtr & index)
{
    if (!index->isMergeable() || index->getGranularity() != granularity)
        throw Exception("Index can not be merged",
                        ErrorCodes::LOGICAL_ERROR);

    const auto hypothesis_index = std::dynamic_pointer_cast<const MergeTreeIndexHypothesis>(index);
    if (!hypothesis_index)
        throw Exception(
            "Only hypothesis index is supported here.", ErrorCodes::LOGICAL_ERROR);

    static const std::set<std::string> relations = {
        "equals", "notEquals", "less", "lessOrEquals", "greaterOrEquals", "greater"};

    // TODO: move to index hypothesis
    std::vector<ASTPtr> compare_hypotheses_data;
    std::vector<CNFQuery::OrGroup> hypotheses_data;
    const auto cnf = TreeCNFConverter::toCNF(hypothesis_index->index.expression_list_ast->children.front()).pullNotOutFunctions();
    for (const auto & group : cnf.getStatements())
    {
        if (group.size() == 1)
        {
            CNFQuery::AtomicFormula atomic_formula = *group.begin();
            CNFQuery::AtomicFormula atom{atomic_formula.negative, atomic_formula.ast->clone()};
            pushNotIn(atom);
            if (atom.negative)
                throw Exception("negative atom", ErrorCodes::LOGICAL_ERROR);

            auto * func = atom.ast->as<ASTFunction>();
            if (func && relations.count(func->name))
                compare_hypotheses_data.push_back(atom.ast);
        }
        hypotheses_data.push_back(group);
    }
    index_to_compare_atomic_hypotheses.push_back(compare_hypotheses_data);
    index_to_hypotheses.push_back(hypotheses_data);
}

void MergeTreeIndexMergedCondition::addConstraints(const ConstraintsDescription & constraints_description)
{
    auto atomic_constraints_data = constraints_description.getAtomicConstraintData();
    for (const auto & atomic_formula : atomic_constraints_data)
    {
        CNFQuery::AtomicFormula atom{atomic_formula.negative, atomic_formula.ast->clone()};
        pushNotIn(atom);
        atomic_constraints.push_back(atom.ast);
    }

    for (const auto & formula : constraints_description.getConstraints())
        all_constraints.push_back(formula->as<ASTConstraintDeclaration>()->expr->clone());
}

namespace
{

ComparisonGraph::CompareResult getExpectedCompare(const CNFQuery::AtomicFormula & atom)
{
    const auto * func = atom.ast->as<ASTFunction>();
    if (func)
    {
        auto expected = ComparisonGraph::getCompareResult(func->name);
        if (atom.negative)
            expected = ComparisonGraph::inverseCompareResult(expected);
        return expected;
    }
    return ComparisonGraph::CompareResult::UNKNOWN;
}

}

bool MergeTreeIndexMergedCondition::alwaysUnknownOrTrue() const
{
    if (!use_smt)
        return alwaysUnknownOrTrueGraph();
    else
        return alwaysUnknownOrTrueSMT();
}

bool MergeTreeIndexMergedCondition::alwaysUnknownOrTrueGraph() const
{
    /// Replaces < -> <=, > -> >= and assumes that all hypotheses are true then checks if path exists

    std::vector<ASTPtr> active_atomic_formulas(atomic_constraints);
    for (const auto & hypothesis : index_to_compare_atomic_hypotheses)
    {
        active_atomic_formulas.insert(
            std::end(active_atomic_formulas),
            std::begin(hypothesis),
            std::end(hypothesis));
    }

    /// transform active formulas
    for (auto & formula : active_atomic_formulas)
    {
        formula = formula->clone(); /// do all operations with copy
        auto * func = formula->as<ASTFunction>();
        if (func && func->name == "less")
            func->name = "lessOrEquals";
        if (func && func->name == "greater")
            func->name = "greaterOrEquals";
    }

    const auto weak_graph = std::make_unique<ComparisonGraph>(active_atomic_formulas);

    bool useless = true;
    expression_cnf->iterateGroups(
        [&](const CNFQuery::OrGroup & or_group)
        {
            for (const auto & atomic_formula : or_group)
            {
                CNFQuery::AtomicFormula atom{atomic_formula.negative, atomic_formula.ast->clone()};
                pushNotIn(atom);
                const auto * func = atom.ast->as<ASTFunction>();
                if (func && func->arguments->children.size() == 2)
                {
                    const auto left = weak_graph->getComponentId(func->arguments->children[0]);
                    const auto right = weak_graph->getComponentId(func->arguments->children[1]);
                    if (!(left && right && weak_graph->hasPath(left.value(), right.value())))
                    {
                        return;
                    }
                }
                else
                {
                    return;
                }
            }

            useless = false;
        });
    return useless;
}

bool MergeTreeIndexMergedCondition::alwaysUnknownOrTrueSMT() const
{
    return true;
}

bool MergeTreeIndexMergedCondition::mayBeTrueOnGranule(const MergeTreeIndexGranules & granules) const
{
    std::vector<bool> values;
    for (const auto & index_granule : granules)
    {
        const auto granule = std::dynamic_pointer_cast<const MergeTreeIndexGranuleHypothesis>(index_granule);
        if (!granule)
            throw Exception("Only hypothesis index is supported here.", ErrorCodes::LOGICAL_ERROR);
        values.push_back(granule->met);
    }

    if (!use_smt)
        return mayBeTrueOnGranuleUsingGraph(values);
    else
        return mayBeTrueOnGranuleUsingSMT(values);
}

std::unique_ptr<TreeSMTSolver> MergeTreeIndexMergedCondition::buildSolver(const std::vector<bool> & values) const
{
    Poco::Logger::get("MergeTreeIndexMergedCondition").information("New solver");
    std::vector<ASTPtr> active_formulas(all_constraints);
    for (size_t i = 0; i < values.size(); ++i)
    {
        if (values[i])
        {
            const auto hypotheses =
                TreeCNFConverter::fromCNF(
                    CNFQuery::AndGroup(
                        std::begin(index_to_hypotheses[i]),
                        std::end(index_to_hypotheses[i])));
            active_formulas.push_back(hypotheses);
        }
    }
    auto solver = std::make_unique<TreeSMTSolver>(TreeSMTSolver::STRICTNESS::FULL, source_columns);
    for (const auto & ast : active_formulas)
        solver->addConstraint(ast);
    return solver;
}

TreeSMTSolver & MergeTreeIndexMergedCondition::getSolver(const std::vector<bool> & values) const
{
    if (!solverCache.contains(values))
        solverCache[values] = buildSolver(values);
    return *solverCache.at(values);
}

bool MergeTreeIndexMergedCondition::mayBeTrueOnGranuleUsingSMT(const std::vector<bool> & values) const
{
    auto & solver = getSolver(values);
    return !solver.alwaysFalse(expression_ast);
}

bool MergeTreeIndexMergedCondition::mayBeTrueOnGranuleUsingGraph(const std::vector<bool> & values) const
{
    const auto & graph = getGraph(values);
    bool always_false = false;
    expression_cnf->iterateGroups(
        [&](const CNFQuery::OrGroup & or_group)
        {
            if (always_false)
                return;

            for (const auto & atomic_formula : or_group)
            {
                CNFQuery::AtomicFormula atom{atomic_formula.negative, atomic_formula.ast->clone()};
                pushNotIn(atom);
                const auto * func = atom.ast->as<ASTFunction>();
                if (func && func->arguments->children.size() == 2)
                {
                    const auto expected = getExpectedCompare(atom);
                    if (graph.isPossibleCompare(expected, func->arguments->children[0], func->arguments->children[1]))
                    {
                        /// If graph failed use matching.
                        /// We don't need to check constraints.
                        return;
                    }
                }
            }
            always_false = true;
        });
    return !always_false;
}

std::unique_ptr<ComparisonGraph> MergeTreeIndexMergedCondition::buildGraph(const std::vector<bool> & values) const
{
    Poco::Logger::get("MergeTreeIndexMergedCondition").information("New graph");
    std::vector<ASTPtr> active_atomic_formulas(atomic_constraints);
    for (size_t i = 0; i < values.size(); ++i)
    {
        if (values[i])
            active_atomic_formulas.insert(
                std::end(active_atomic_formulas),
                std::begin(index_to_compare_atomic_hypotheses[i]),
                std::end(index_to_compare_atomic_hypotheses[i]));
    }
    return std::make_unique<ComparisonGraph>(active_atomic_formulas);
}

const ComparisonGraph & MergeTreeIndexMergedCondition::getGraph(const std::vector<bool> & values) const
{
    if (!graphCache.contains(values))
        graphCache[values] = buildGraph(values);
    return *graphCache.at(values);
}

}
