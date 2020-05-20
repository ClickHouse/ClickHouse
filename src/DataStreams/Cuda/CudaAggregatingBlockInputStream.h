#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/Cuda/CudaAggregator.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/CompressedReadBuffer.h>
#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


/** See description of AggregatingBlockInputStream
  * 
  * 
  * 
  */
class CudaAggregatingBlockInputStream : public IProfilingBlockInputStream
{
public:
    /** keys are taken from the GROUP BY part of the query
      * Aggregate functions are searched everywhere in the expression.
      * Columns corresponding to keys and arguments of aggregate functions must already be computed.
      */
    CudaAggregatingBlockInputStream(const BlockInputStreamPtr & input, const Aggregator::Params & params_, 
      const Context & context_, bool final_)
        : params(params_), aggregator(context_, params), final(final_)
    {
        children.push_back(input);
    }

    String getName() const override { return "CudaAggregating"; }

    Block getHeader() const override;

protected:
    Block readImpl() override;

    CudaAggregator::Params params;
    CudaAggregator aggregator;
    bool final;

    bool executed = false;

    /** From here we will get the completed blocks after the aggregation. */
    BlocksList blocks;
    //std::unique_ptr<IBlockInputStream> impl;

    Logger * log = &Logger::get("CudaAggregatingBlockInputStream");
};

}
