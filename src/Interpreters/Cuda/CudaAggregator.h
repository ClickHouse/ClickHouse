#pragma once

#include <functional>
#include <memory>
#include <mutex>

#include <base/logger_useful.h>

#include <base/StringRef.h>
#include <Common/Arena.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/HashTable/TwoLevelHashMap.h>
#include <Common/HashTable/TwoLevelStringHashMap.h>

#include <Common/ColumnsHashing.h>
#include <Common/ThreadPool.h>
#include <Common/assert_cast.h>
#include <Common/filesystemHelpers.h>

#include <QueryPipeline/SizeLimits.h>

#include <Disks/SingleDiskVolume.h>

#include <Interpreters/AggregateDescription.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/JIT/compileFunction.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>

#include <Parsers/IAST_fwd.h>

#include <Interpreters/Context_fwd.h>

#include <AggregateFunctions/Cuda/ICudaAggregateFunction.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cuda/CudaStringsAggregator.h>

namespace DB
{

struct CudaAggregatedDataVariants : private boost::noncopyable
{
    bool empty_ = true;
    std::unique_ptr<CudaStringsAggregator> strings_agg;

    bool empty() const { return empty_; }

    void init(ContextPtr context, CudaAggregateFunctionPtr cuda_agg_function)
    {
        const Settings & settings = context->getSettingsRef();
        /// There are no variants for now
        strings_agg = std::make_unique<decltype(strings_agg)::element_type>(
            settings.cuda_device_number,
            settings.cuda_chunks_number,
            settings.cuda_hash_table_max_size,
            settings.cuda_hash_table_strings_buffer_max_size,
            settings.cuda_buffer_max_strings_number,
            settings.cuda_buffer_max_size,
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

    CudaAggregator(ContextPtr context_, const Params & params_);

    using AggregateColumns = std::vector<ColumnRawPtrs>;
    using AggregateColumnsData = std::vector<ColumnAggregateFunction::Container *>;
    using AggregateColumnsConstData = std::vector<const ColumnAggregateFunction::Container *>;

    bool executeOnBlock(
        Columns columns,
        UInt64 num_rows,
        CudaAggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool & no_more_keys) const;

    /// Get data structure of the result.
    Block getHeader(bool final) const;

protected:
    friend struct CudaAggregatedDataVariants;
    friend class CudaConvertingAggregatedToChunksTransform;

    ContextPtr context;
    Params params;

    CudaAggregateFunctionPtr cuda_agg_function;

    Poco::Logger * log = &Poco::Logger::get("CudaAggregator");

protected:
    void convertToBlockImplFinal(
        CudaAggregatedDataVariants & data_variants, MutableColumns & key_columns, MutableColumns & final_aggregate_columns) const;

    template <typename Filler>
    Block prepareBlockAndFill(CudaAggregatedDataVariants & data_variants, bool final, size_t rows, Filler && filler) const;

    Block prepareBlockAndFillSingleLevel(CudaAggregatedDataVariants & data_variants, bool final) const;
};


}
