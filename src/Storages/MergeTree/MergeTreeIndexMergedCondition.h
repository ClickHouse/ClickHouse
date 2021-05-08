#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/ComparisonGraph.h>
#include <Interpreters/TreeSMTSolver.h>

namespace DB
{

/*
 * IndexCondition checking several indexes at the same time.
 * Works only for hypotheses. (will also support minmax soon).
 */
class MergeTreeIndexMergedCondition
{
public:
    MergeTreeIndexMergedCondition(
        const SelectQueryInfo & query,
        ContextPtr context,
        const size_t granularity,
        const NamesAndTypesList & source_columns,
        const bool use_smt);

    void addIndex(const MergeTreeIndexPtr & index);
    void addConstraints(const ConstraintsDescription & constraints_description);

    bool alwaysUnknownOrTrue() const;
    bool mayBeTrueOnGranule(const MergeTreeIndexGranules & granules) const;

    //TODO: add constraints
private:
    std::unique_ptr<ComparisonGraph> buildGraph(const std::vector<bool> & values) const;
    const ComparisonGraph & getGraph(const std::vector<bool> & values) const;

    bool alwaysUnknownOrTrueGraph() const;

    bool mayBeTrueOnGranuleUsingGraph(const std::vector<bool> & values) const;

    std::unique_ptr<TreeSMTSolver> buildSolver(const std::vector<bool> & values) const;
    TreeSMTSolver & getSolver(const std::vector<bool> & values) const;

    bool alwaysUnknownOrTrueSMT() const;

    bool mayBeTrueOnGranuleUsingSMT(const std::vector<bool> & values) const;

    const size_t granularity;
    const NamesAndTypesList source_columns;
    const bool use_smt;
    ASTPtr expression_ast;
    std::unique_ptr<CNFQuery> expression_cnf;

    mutable std::unordered_map<std::vector<bool>, std::unique_ptr<ComparisonGraph>> graphCache;
    mutable std::unordered_map<std::vector<bool>, std::unique_ptr<TreeSMTSolver>> solverCache;

    std::vector<std::vector<ASTPtr>> index_to_compare_atomic_hypotheses;
    std::vector<std::vector<CNFQuery::OrGroup>> index_to_hypotheses;
    std::vector<ASTPtr> all_constraints;
    std::vector<ASTPtr> atomic_constraints;
};

using MergeTreeIndexMergedConditionPtr = std::shared_ptr<MergeTreeIndexMergedCondition>;
using MergeTreeIndexMergedConditions = std::vector<MergeTreeIndexMergedConditionPtr>;

}
