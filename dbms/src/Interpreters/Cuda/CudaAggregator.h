#pragma once

#include <memory>
#include <functional>

#include <common/logger_useful.h>

#include <common/StringRef.h>
#include <Common/Arena.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/SizeLimits.h>

#include <Interpreters/Context.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Cuda/CudaStringsAggregator.h>

#include <AggregateFunctions/Cuda/ICudaAggregateFunction.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
}

class IBlockOutputStream;

struct CudaAggregatedDataVariants : private boost::noncopyable
{
    bool                                    empty_ = true;
    std::unique_ptr<CudaStringsAggregator>  strings_agg;

    bool empty() const { return empty_; }

    void init(const Context & context, CudaAggregateFunctionPtr cuda_agg_function)
    {
        const Settings & settings = context.getSettingsRef();
        /// There are no variants for now
        strings_agg = std::make_unique<decltype(strings_agg)::element_type>(
            settings.cuda_device_number, settings.cuda_chunks_number,
            settings.cuda_hash_table_max_size, settings.cuda_hash_table_strings_buffer_max_size, 
            settings.cuda_buffer_max_strings_number, settings.cuda_buffer_max_size, 
            cuda_agg_function);
        empty_ = false;
    }
    void startProcessing()
    {
        assert(!empty());
        strings_agg->startProcessing();
    }
    void waitProcessed()
    {
        assert(!empty());
        strings_agg->waitProcessed();
    }
};

using CudaAggregatedDataVariantsPtr = std::shared_ptr<CudaAggregatedDataVariants>;
using CudaManyAggregatedDataVariants = std::vector<CudaAggregatedDataVariantsPtr>;

/** Aggregates the source of the blocks.
  */
class CudaAggregator
{
public:
    using Params = Aggregator::Params;

    CudaAggregator(const Context & context_, const Params & params_);

    /// Aggregate the source. Get the result in the form of one of the data structures.
    void execute(const BlockInputStreamPtr & stream, CudaAggregatedDataVariants & result);

    using AggregateColumns = std::vector<ColumnRawPtrs>;
    using AggregateColumnsData = std::vector<ColumnAggregateFunction::Container *>;
    using AggregateColumnsConstData = std::vector<const ColumnAggregateFunction::Container *>;

    /// Process one block. Return false if the processing should be aborted (with group_by_overflow_mode = 'break').
    bool executeOnBlock(const Block & block, CudaAggregatedDataVariants & result,
        ColumnRawPtrs & key_columns, AggregateColumns & aggregate_columns);    /// Passed to not create them anew for each block

    /** Convert the aggregation data structure into a block.
      * If overflow_row = true, then aggregates for rows that are not included in max_rows_to_group_by are put in the first block.
      *
      * If final = false, then ColumnAggregateFunction is created as the aggregation columns with the state of the calculations,
      *  which can then be combined with other states (for distributed query processing).
      * If final = true, then columns with ready values are created as aggregate columns.
      */
    BlocksList convertToBlocks(CudaAggregatedDataVariants & data_variants, bool final, size_t max_threads) const;

    /// Get data structure of the result.
    Block getHeader(bool final) const;

protected:
    friend struct CudaAggregatedDataVariants;
    
    /// NOTE i took it form RemoteBlockInputStream; not sure if it's ok to copy wholy 'context'
    Context context;
    Params  params;

    CudaAggregateFunctionPtr cuda_agg_function;

    Logger * log = &Logger::get("CudaAggregator");

protected:

    void convertToBlockImplFinal(
        CudaAggregatedDataVariants & data_variants,
        MutableColumns & key_columns,
        MutableColumns & final_aggregate_columns) const;

    template <typename Filler>
    Block prepareBlockAndFill(
        CudaAggregatedDataVariants & data_variants,
        bool final,
        size_t rows,
        Filler && filler) const;

    Block prepareBlockAndFillSingleLevel(CudaAggregatedDataVariants & data_variants, bool final) const;
};


}
