#pragma once

#include <Interpreters/Aggregator.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/CompressedReadBuffer.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/ParallelInputsProcessor.h>


namespace DB
{


/** Aggregates several sources in parallel.
  * Makes aggregation of blocks from different sources independently in different threads, then combines the results.
  * If final == false, aggregate functions are not finalized, that is, they are not replaced by their value, but contain an intermediate state of calculations.
  * This is necessary so that aggregation can continue (for example, by combining streams of partially aggregated data).
  */
class ParallelAggregatingBlockInputStream : public IProfilingBlockInputStream
{
public:
    /** Columns from key_names and arguments of aggregate functions must already be computed.
      */
    ParallelAggregatingBlockInputStream(
        const BlockInputStreams & inputs, const BlockInputStreamPtr & additional_input_at_end,
        const Aggregator::Params & params_, bool final_, size_t max_threads_, size_t temporary_data_merge_threads_);

    String getName() const override { return "ParallelAggregating"; }

    void cancel(bool kill) override;

    Block getHeader() const override;

protected:
    /// Do nothing that preparation to execution of the query be done in parallel, in ParallelInputsProcessor.
    void readPrefix() override
    {
    }

    Block readImpl() override;

private:
    Aggregator::Params params;
    Aggregator aggregator;
    bool final;
    size_t max_threads;
    size_t temporary_data_merge_threads;

    size_t keys_size;
    size_t aggregates_size;

    /** Used if there is a limit on the maximum number of rows in the aggregation,
      *  and if group_by_overflow_mode == ANY.
      * In this case, new keys are not added to the set, but aggregation is performed only by
      *  keys that have already been added into the set.
      */
    bool no_more_keys = false;

    std::atomic<bool> executed {false};

    /// To read the data stored into the temporary data file.
    struct TemporaryFileStream
    {
        ReadBufferFromFile file_in;
        CompressedReadBuffer compressed_in;
        BlockInputStreamPtr block_in;

        TemporaryFileStream(const std::string & path);
    };
    std::vector<std::unique_ptr<TemporaryFileStream>> temporary_inputs;

    Logger * log = &Logger::get("ParallelAggregatingBlockInputStream");


    ManyAggregatedDataVariants many_data;
    Exceptions exceptions;

    struct ThreadData
    {
        size_t src_rows = 0;
        size_t src_bytes = 0;

        StringRefs key;
        ColumnRawPtrs key_columns;
        Aggregator::AggregateColumns aggregate_columns;

        ThreadData(size_t keys_size, size_t aggregates_size)
        {
            key.resize(keys_size);
            key_columns.resize(keys_size);
            aggregate_columns.resize(aggregates_size);
        }
    };

    std::vector<ThreadData> threads_data;


    struct Handler
    {
        Handler(ParallelAggregatingBlockInputStream & parent_)
            : parent(parent_) {}

        void onBlock(Block & block, size_t thread_num);
        void onFinishThread(size_t thread_num);
        void onFinish();
        void onException(std::exception_ptr & exception, size_t thread_num);

        ParallelAggregatingBlockInputStream & parent;
    };

    Handler handler;
    ParallelInputsProcessor<Handler> processor;


    void execute();


    /** From here we get the finished blocks after the aggregation.
      */
    std::unique_ptr<IBlockInputStream> impl;
};

}
