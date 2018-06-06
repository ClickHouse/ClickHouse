#pragma once

#include <Interpreters/Aggregator.h>
#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


/** A pre-aggregate stream of blocks in which each block is already aggregated.
  * Aggregate functions in blocks should not be finalized so that their states can be merged.
  */
class MergingAggregatedBlockInputStream : public IProfilingBlockInputStream
{
public:
    MergingAggregatedBlockInputStream(const BlockInputStreamPtr & input, const Aggregator::Params & params, bool final_, size_t max_threads_)
        : aggregator(params), final(final_), max_threads(max_threads_)
    {
        children.push_back(input);
    }

    String getName() const override { return "MergingAggregated"; }

    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    Aggregator aggregator;
    bool final;
    size_t max_threads;

    bool executed = false;
    BlocksList blocks;
    BlocksList::iterator it;
};

}
