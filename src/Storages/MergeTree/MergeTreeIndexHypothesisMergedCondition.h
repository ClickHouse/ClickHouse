#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/ComparisonGraph.h>

namespace DB
{

/// MergedCondition for Indexhypothesis.
class MergeTreeIndexhypothesisMergedCondition : public IMergeTreeIndexMergedCondition
{
public:
    MergeTreeIndexhypothesisMergedCondition(
        const SelectQueryInfo & query, const ConstraintsDescription & constraints, size_t granularity_);

    void addIndex(const MergeTreeIndexPtr & index) override;
    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(const MergeTreeIndexGranules & granules) const override;

private:
    void addConstraints(const ConstraintsDescription & constraints_description);
    std::unique_ptr<ComparisonGraph> buildGraph(const std::vector<bool> & values) const;
    const ComparisonGraph * getGraph(const std::vector<bool> & values) const;

    ASTPtr expression_ast;
    std::unique_ptr<CNFQuery> expression_cnf;

    /// Part analysis can be done in parallel.
    /// So, we have shared answer and graph cache.
    mutable std::mutex cache_mutex;
    mutable std::unordered_map<std::vector<bool>, std::unique_ptr<ComparisonGraph>> graph_cache;
    mutable std::unordered_map<std::vector<bool>, bool> answer_cache;

    std::vector<std::vector<ASTPtr>> index_to_compare_atomic_hypotheses;
    std::vector<std::vector<CNFQuery::OrGroup>> index_to_atomic_hypotheses;
    std::vector<ASTPtr> atomic_constraints;
};

}
