#pragma once

#include <Parsers/IAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <string>
#include <vector>
#include <memory>
#include <Core/Types.h>
#include <Utils/IndexAdvisor/IndexCandidateGenerator.h>
#include <Utils/IndexAdvisor/IndexBenefitEstimator.h>
#include <Utils/IndexAdvisor/IndexSelector.h>

namespace DB
{

class IndexAdvisor
{
public:
    IndexAdvisor()
        : candidate_generator(std::make_shared<RuleBasedCandidateGenerator>())
        , benefit_estimator(std::make_shared<StatisticsBasedBenefitEstimator>())
        , selector(std::make_shared<HeuristicSearchIndexSelector>(candidate_generator, benefit_estimator))
    {}

    void analyzeQuery(const ASTPtr & query_ast, ContextMutablePtr context, size_t max_index_count = 3);
    const std::vector<IndexCandidate> & getCandidates() const { return selected_candidates; }

private:
    std::shared_ptr<IIndexCandidateGenerator> candidate_generator;
    std::shared_ptr<IIndexBenefitEstimator> benefit_estimator;
    std::shared_ptr<IIndexSelector> selector;
    std::vector<IndexCandidate> selected_candidates;
};

}
