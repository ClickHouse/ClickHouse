#include <Utils/IndexAdvisor/IndexBenefitEstimator.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterExplainQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Columns/ColumnsNumber.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Core/Block.h>

namespace DB
{

IndexMetrics StatisticsBasedBenefitEstimator::computeMetrics(const ASTPtr & query_ast, ContextMutablePtr context)
{
    if (!query_ast)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query AST is null");

    LOG_DEBUG(&Poco::Logger::get("IndexBenefitEstimator"), "Computing metrics for query type: {}", query_ast->getID());

    // Create EXPLAIN ESTIMATE query
    auto explain_query = std::make_shared<ASTExplainQuery>(ASTExplainQuery::QueryEstimates);
    explain_query->setExplainedQuery(query_ast);

    // Execute EXPLAIN ESTIMATE
    InterpreterExplainQuery interpreter(explain_query, context, SelectQueryOptions());
    auto result = interpreter.execute();
    auto pipeline = std::move(result.pipeline);
    PullingPipelineExecutor executor(pipeline);

    // Process results
    IndexMetrics metrics;
    metrics.rows = 0;
    metrics.parts = 0;

    Block block;
    while (executor.pull(block))
    {
        const auto & rows_col = block.getByName("rows").column;
        const auto & parts_col = block.getByName("parts").column;

        for (size_t i = 0; i < rows_col->size(); ++i)
        {
            metrics.rows += rows_col->getUInt(i);
        }

        for (size_t i = 0; i < parts_col->size(); ++i)
        {
            metrics.parts += parts_col->getUInt(i);
        }
    }

    LOG_DEBUG(&Poco::Logger::get("IndexBenefitEstimator"), "Computed metrics: rows={}, parts={}", metrics.rows, metrics.parts);
    return metrics;
}

IndexBenefit StatisticsBasedBenefitEstimator::computeBenefit(const IndexMetrics & baseline, const IndexMetrics & with_index, size_t baseline_bytes, size_t index_bytes)
{
    if (baseline.rows > 0 && with_index.rows < baseline.rows && baseline_bytes > 0 && index_bytes > 0)
    {
        double row_reduction = static_cast<double>(baseline.rows - with_index.rows) / baseline.rows;
        double part_reduction = static_cast<double>(baseline.parts - with_index.parts) / baseline.parts;
        double raw_score = (row_reduction + part_reduction) / 2.0;
        
        double storage_ratio = static_cast<double>(index_bytes) / baseline_bytes;
        return raw_score / storage_ratio;
    }
    
    return 0.0;
}

size_t StatisticsBasedBenefitEstimator::chooseBest(const std::vector<IndexBenefit> & benefits)
{
    if (benefits.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No benefits to choose from");

    size_t best_idx = 0;
    for (size_t i = 1; i < benefits.size(); ++i)
    {
        if (benefits[i] > benefits[best_idx])
            best_idx = i;
    }
    return best_idx;
}

bool StatisticsBasedBenefitEstimator::isSmallDistance(const IndexBenefit & a, const IndexBenefit & b)
{
    if (a == 0 && b == 0)
        return true;

    double max_score = std::max(a, b);
    double distance = std::abs(a - b) / max_score;
    return distance < SMALL_DISTANCE_THRESHOLD;
}

} 
