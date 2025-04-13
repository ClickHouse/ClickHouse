#pragma once

#include <Utils/IndexAdvisor/IndexCandidateGenerator.h>
#include <Utils/IndexAdvisor/IndexBenefitEstimator.h>
#include <memory>
#include <vector>
#include <unordered_set>

namespace DB
{

class IIndexSelector
{
public:
    virtual ~IIndexSelector() = default;
    virtual std::vector<IndexCandidate> selectIndexes(
        const ASTPtr & query_ast,
        ContextMutablePtr context,
        size_t max_index_count) = 0;
};

class HeuristicSearchIndexSelector : public IIndexSelector
{
public:
    HeuristicSearchIndexSelector(
        std::shared_ptr<IIndexCandidateGenerator> candidate_generator_,
        std::shared_ptr<IIndexBenefitEstimator> benefit_estimator_)
        : candidate_generator(std::move(candidate_generator_)),
          benefit_estimator(std::move(benefit_estimator_)) {}

    std::vector<IndexCandidate> selectIndexes(
        const ASTPtr & query_ast,
        ContextMutablePtr context,
        size_t max_index_count) override;

private:
    bool selectIndex(
        size_t iteration,
        ASTPtr & current_query_ast,
        ContextMutablePtr context,
        IndexMetrics & baseline_metrics,
        std::unordered_set<std::string> & created_indexes,
        std::vector<IndexCandidate> & selected,
        std::vector<IndexCandidate> & candidates);

    IndexBenefit calculateIndexScore(
        const IndexCandidate & candidate,
        ContextMutablePtr context,
        const IndexMetrics & baseline_metrics,
        const ASTPtr & current_query_ast);

    std::shared_ptr<IIndexCandidateGenerator> candidate_generator;
    std::shared_ptr<IIndexBenefitEstimator> benefit_estimator;
};

} 
