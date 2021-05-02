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
    extern const int INCORRECT_QUERY;
}

MergeTreeIndexMergedCondition::MergeTreeIndexMergedCondition(
    const SelectQueryInfo & query_,
    ContextPtr /*context_*/,
    const size_t granularity_)
    : granularity(granularity_)
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
        "equals", "less", "lessOrEquals", "greaterOrEquals", "greater"};

    // TODO: move to index hypothesis
    std::vector<ASTPtr> compare_hypotheses_data;
    std::vector<CNFQuery::OrGroup> hypotheses_data;
    const auto cnf = TreeCNFConverter::toCNF(hypothesis_index->index.expression_list_ast->children.front()).pullNotOutFunctions();
    for (const auto & group : cnf.getStatements()) {
        hypotheses_data.push_back(group);
        if (group.size() == 1)
        {
            CNFQuery::AtomicFormula atom = *group.begin();
            pushNotIn(atom);
            if (atom.negative)
                throw Exception("negative atom", ErrorCodes::LOGICAL_ERROR);

            auto * func = atom.ast->as<ASTFunction>();
            if (func && relations.count(func->name))
                compare_hypotheses_data.push_back(atom.ast);
        }
    }
    index_to_compare_atomic_hypotheses.push_back(compare_hypotheses_data);
    index_to_atomic_hypotheses.push_back(hypotheses_data);
}

void MergeTreeIndexMergedCondition::addConstraints(const ConstraintsDescription & constraints_description)
{
    auto atomic_constraints_data = constraints_description.getAtomicConstraintData();
    for (auto & atom : atomic_constraints_data)
    {
        pushNotIn(atom);
        atomic_constraints.push_back(atom.ast);
    }
}

namespace
{

ComparisonGraph::CompareResult getExpectedCompare(const CNFQuery::AtomicFormula & atom)
{
    static const std::map<std::string, std::string> inverse_relations = {
        {"equals", "notEquals"},
        {"less", "greaterOrEquals"},
        {"lessOrEquals", "greater"},
        {"notEquals", "equals"},
        {"greaterOrEquals", "less"},
        {"greater", "lessOrEquals"},
    };

    static const std::map<std::string, ComparisonGraph::CompareResult> relation_to_compare = {
        {"equals", ComparisonGraph::CompareResult::EQUAL},
        {"less", ComparisonGraph::CompareResult::LESS},
        {"lessOrEquals", ComparisonGraph::CompareResult::LESS_OR_EQUAL},
        {"notEquals", ComparisonGraph::CompareResult::UNKNOWN},
        {"greaterOrEquals", ComparisonGraph::CompareResult::GREATER_OR_EQUAL},
        {"greater", ComparisonGraph::CompareResult::GREATER},
    };


    const auto * func = atom.ast->as<ASTFunction>();
    if (func && inverse_relations.count(func->name))
    {
        std::string function_name = func->name;
        if (atom.negative)
            function_name = inverse_relations.at(func->name);
        return relation_to_compare.at(function_name);
    }
    return ComparisonGraph::CompareResult::UNKNOWN;
}

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
    const auto & graph = getGraph(values);

    bool always_false = false;
    expression_cnf->iterateGroups(
        [&](const CNFQuery::OrGroup & or_group)
        {
            if (always_false)
                return;

            for (auto atom : or_group)
            {
               pushNotIn(atom);
               Poco::Logger::get("KEK").information(atom.ast->dumpTree());
               const auto * func = atom.ast->as<ASTFunction>();
               if (func && func->arguments->children.size() == 2)
               {
                   const auto expected = getExpectedCompare(atom);
                   if (graph.isPossibleCompare(
                           expected,
                           func->arguments->children[0],
                           func->arguments->children[1]))
                   {
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
