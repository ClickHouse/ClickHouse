#pragma once

#include <Interpreters/Aggregator.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/CompressedReadBuffer.h>
#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


/** Aggregates the stream of blocks using the specified key columns and aggregate functions.
  * Columns with aggregate functions adds to the end of the block.
  * If final = false, the aggregate functions are not finalized, that is, they are not replaced by their value, but contain an intermediate state of calculations.
  * This is necessary so that aggregation can continue (for example, by combining streams of partially aggregated data).
  */
class AggregatingBlockInputStream : public IProfilingBlockInputStream
{
public:
    /** keys are taken from the GROUP BY part of the query
      * Aggregate functions are searched everywhere in the expression.
      * Columns corresponding to keys and arguments of aggregate functions must already be computed.
      */
    AggregatingBlockInputStream(BlockInputStreamPtr input_, const Aggregator::Params & params_, bool final_)
        : params(params_), aggregator(params), final(final_)
    {
        children.push_back(input_);
    }

    String getName() const override { return "Aggregating"; }

    String getID() const override
    {
        std::stringstream res;
        res << "Aggregating(" << children.back()->getID() << ", " << aggregator.getID() << ")";
        return res.str();
    }

protected:
    Block readImpl() override;

    Aggregator::Params params;
    Aggregator aggregator;
    bool final;

    bool executed = false;

    /// To read the data that was flushed into the temporary data file.
    struct TemporaryFileStream
    {
        ReadBufferFromFile file_in;
        CompressedReadBuffer compressed_in;
        BlockInputStreamPtr block_in;

        TemporaryFileStream(const std::string & path);
    };
    std::vector<std::unique_ptr<TemporaryFileStream>> temporary_inputs;

     /** From here we will get the completed blocks after the aggregation. */
    std::unique_ptr<IBlockInputStream> impl;

    Logger * log = &Logger::get("AggregatingBlockInputStream");
};

}
