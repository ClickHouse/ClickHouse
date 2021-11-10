#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/ComparisonGraph.h>

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
        const size_t granularity);

    void addIndex(const MergeTreeIndexPtr & index);
    void addConstraints(const ConstraintsDescription & constraints_description);

    bool alwaysUnknownOrTrue() const;
    bool mayBeTrueOnGranule(const MergeTreeIndexGranules & granules) const;

private:
    std::unique_ptr<ComparisonGraph> buildGraph(const std::vector<bool> & values) const;
    const ComparisonGraph & getGraph(const std::vector<bool> & values) const;

    const size_t granularity;
    ASTPtr expression_ast;
    std::unique_ptr<CNFQuery> expression_cnf;

    mutable std::unordered_map<std::vector<bool>, std::unique_ptr<ComparisonGraph>> graphCache;
    mutable std::unordered_map<std::vector<bool>, bool> answerCache;

    std::vector<std::vector<ASTPtr>> index_to_compare_atomic_hypotheses;
    std::vector<std::vector<CNFQuery::OrGroup>> index_to_atomic_hypotheses;
    std::vector<ASTPtr> atomic_constraints;
};

using MergeTreeIndexMergedConditionPtr = std::shared_ptr<MergeTreeIndexMergedCondition>;
using MergeTreeIndexMergedConditions = std::vector<MergeTreeIndexMergedConditionPtr>;

}
