#pragma once

#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <vector>

namespace DB
{

struct IndexMetrics
{
    size_t rows = 0;
    size_t parts = 0;
    size_t index_size = 0;

    IndexMetrics() = default;
};

using IndexBenefit = double;

class IIndexBenefitEstimator
{
public:
    virtual ~IIndexBenefitEstimator() = default;
    virtual IndexMetrics computeMetrics(const ASTPtr & query_ast, ContextMutablePtr context) = 0;
    virtual IndexBenefit computeBenefit(const IndexMetrics & baseline, const IndexMetrics & with_index, size_t baseline_bytes, size_t index_bytes) = 0;
    virtual size_t chooseBest(const std::vector<IndexBenefit> & benefits) = 0;
    virtual bool isSmallDistance(const IndexBenefit & a, const IndexBenefit & b) = 0;
};

class StatisticsBasedBenefitEstimator : public IIndexBenefitEstimator
{
public:
    StatisticsBasedBenefitEstimator() = default;
    ~StatisticsBasedBenefitEstimator() override = default;
    IndexMetrics computeMetrics(const ASTPtr & query_ast, ContextMutablePtr context) override;
    IndexBenefit computeBenefit(const IndexMetrics & baseline, const IndexMetrics & with_index, size_t baseline_bytes, size_t index_bytes) override;
    size_t chooseBest(const std::vector<IndexBenefit> & benefits) override;
    bool isSmallDistance(const IndexBenefit & a, const IndexBenefit & b) override;
private:
    static constexpr double SMALL_DISTANCE_THRESHOLD = 0.1;
};

} 
