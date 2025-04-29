#include "CheckerSink.hpp"
#include <memory>
#include <mutex>
#include <numeric>
#include <DataTypes/DataTypeString.h>
#include "Common/Logger.h"
#include "Common/logger_useful.h"
#include "Columns/IColumn.h"
#include "Interpreters/ExpressionAnalyzer.h"
#include "Interpreters/InterpreterSelectQuery.h"
#include "Interpreters/TreeRewriter.h"
#include "Parsers/IParser.h"
#include "Parsers/ParserQuery.h"
#include "Parsers/TokenIterator.h"
#include "Processors/Executors/PullingPipelineExecutor.h"
#include "Processors/Executors/PushingPipelineExecutor.h"
#include "Processors/QueryPlan/QueryPlan.h"
#include "Processors/QueryPlan/ReadFromMemoryStorageStep.h"
#include "Processors/Sources/SourceFromSingleChunk.h"
#include "QueryPipeline/QueryPipeline.h"
#include "QueryPipeline/QueryPipelineBuilder.h"


namespace DB::Hypothesis
{


CheckerSink::CheckerSink(const Block & block_, HypothesisList hypothesis_list_, ContextPtr local_context)
    : SinkToStorage(block_)
    , hypothesis_list(std::move(hypothesis_list_))
    , context(local_context)
{
    verified.assign(hypothesis_list.size(), true);
    log = getLogger("HypothesisChecker");
    LOG_DEBUG(log, "Got {} hypothesis to verify", hypothesis_list.size());
}

void CheckerSink::consume(Chunk & chunk)
{
    std::lock_guard guard{mutex};
    const auto & header = input.getHeader();
    rows_checked += chunk.getNumRows();
    size_t idx = 0;
    for (const auto & hypothesis : hypothesis_list)
    {
        if (!verified[idx])
        {
            ++idx;
            continue;
        }
        bool is_ok = true;

        {
            // Some testing
            std::string query_text = fmt::format("min({} = {}) = 1 as res", hypothesis.toString(), hypothesis.getName());
            Tokens tokens(query_text.data(), query_text.data() + query_text.size());
            IParser::Pos token_iterator(tokens, 100, 100);
            ParserQuery parser(query_text.data() + query_text.size(), false, /*implicit_select_=*/true);
            ASTPtr query;
            Expected expected;
            if (!parser.parse(token_iterator, query, expected))
            {
                LOG_ERROR(log, "Couldn't parse hypothesis expr");
                return;
            }
            Block block(header.cloneWithoutColumns());
            block.setColumns(chunk.getColumns());
            Pipe input(std::make_shared<SourceFromSingleChunk>(std::move(block)));
            InterpreterSelectQuery select(query, context, std::move(input), SelectQueryOptions());
            auto pipeline_builder = select.buildQueryPipeline();
            auto pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
            PullingPipelineExecutor executor(pipeline);
            Block res;
            if (!executor.pull(res) || !res.has("res"))
            {
                LOG_ERROR(log, "Hypothesis pipeline is broken");
                return;
            }
            is_ok = res.getByName("res").column->getBool(0);
        }
        if (!is_ok)
        {
            verified[idx] = false;
        }
        ++idx;
    }
}
HypothesisList CheckerSink::getVerifiedHypothesis() const
{
    std::lock_guard guard{mutex};
    HypothesisList result;
    size_t idx = 0;
    for (const auto & hypothesis : hypothesis_list)
    {
        if (verified[idx])
        {
            result.push_back(hypothesis);
        }
        ++idx;
    }
    LOG_DEBUG(log, "After verification {} hypothesis left", result.size());
    return result;
}

size_t CheckerSink::hypothesisVerifiedCount() const
{
    std::lock_guard guard{mutex};
    return std::accumulate(verified.begin(), verified.end(), 0);
}
}
